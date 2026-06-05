"""Build Gold dimension tables from staging and fact Iceberg tables."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from pyspark.sql.functions import col

from common.partition_state import active_gold_dates, mark_gold_pending_with_error
from gold import dimensions
from gold.config import (
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_BASE_PATH,
    DEFAULT_GOLD_REFRESH_MODE,
    DEFAULT_GOLD_NAMESPACE,
    DEFAULT_GOLD_WAREHOUSE,
    DEFAULT_PARTITION_STATE_PATH,
    DEFAULT_STAGING_NAMESPACE,
    DIM_PRODUCT,
    DIM_SESSION,
    DIM_TIME,
    DIM_USER,
    FACT_EVENTS,
    FACT_SALES,
    REFRESH_MODE_FULL,
    REFRESH_MODE_INCREMENTAL,
    STG_EVENTS,
    create_spark_session,
    load_runtime_config,
    normalize_refresh_mode,
    table_location,
)
from gold.ddl import create_iceberg_table_if_not_exists, create_namespace_if_not_exists
from gold.identifiers import assert_safe_table_location, table_identifier
from gold.readers import read_required_table
from gold.validators import assert_unique_key, require_non_null
from gold.writers import (
    date_in_condition,
    write_full_refresh,
    write_replace_keys,
    write_replace_where,
)


JOB_NAME = "GoldBuildDimensions"


def log(message):
    print(f"[GoldBuildDimensions] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Build Gold dimension tables from staging and facts."
    )
    parser.add_argument("--catalog-name", default=DEFAULT_CATALOG)
    parser.add_argument("--source-namespace", default=DEFAULT_GOLD_NAMESPACE)
    parser.add_argument("--target-namespace", default=DEFAULT_GOLD_NAMESPACE)
    parser.add_argument("--staging-namespace", default=DEFAULT_STAGING_NAMESPACE)
    parser.add_argument("--staging-table", default=STG_EVENTS)
    parser.add_argument("--fact-events-table", default=FACT_EVENTS)
    parser.add_argument("--fact-sales-table", default=FACT_SALES)
    parser.add_argument("--dim-time-table", default=DIM_TIME)
    parser.add_argument("--dim-product-table", default=DIM_PRODUCT)
    parser.add_argument("--dim-user-table", default=DIM_USER)
    parser.add_argument("--dim-session-table", default=DIM_SESSION)
    parser.add_argument(
        "--dim-time-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DIM_TIME),
    )
    parser.add_argument(
        "--dim-product-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DIM_PRODUCT),
    )
    parser.add_argument(
        "--dim-user-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DIM_USER),
    )
    parser.add_argument(
        "--dim-session-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DIM_SESSION),
    )
    parser.add_argument("--refresh-mode", default=DEFAULT_GOLD_REFRESH_MODE)
    parser.add_argument("--state-path", default=DEFAULT_PARTITION_STATE_PATH)
    return parser.parse_args(argv)


def validate_args(args):
    args.refresh_mode = normalize_refresh_mode(
        args.refresh_mode,
        "gold_build_dimensions",
    )
    table_identifier(args.catalog_name, args.staging_namespace, args.staging_table)
    table_identifier(args.catalog_name, args.source_namespace, args.fact_events_table)
    table_identifier(args.catalog_name, args.source_namespace, args.fact_sales_table)
    for table in [
        args.dim_time_table,
        args.dim_product_table,
        args.dim_user_table,
        args.dim_session_table,
    ]:
        table_identifier(args.catalog_name, args.target_namespace, table)
    for path in [
        args.dim_time_path,
        args.dim_product_path,
        args.dim_user_path,
        args.dim_session_path,
    ]:
        assert_safe_table_location(path, DEFAULT_ALLOWED_LOCATION_PREFIXES)


def validate_dimension(df, key_column, table_name):
    require_non_null(df, key_column, table_name)
    assert_unique_key(df, key_column, table_name)


def _filter_dates(df, column_name, partition_dates):
    return df.where(col(column_name).cast("string").isin(partition_dates))


def _distinct_non_null(df, column_name):
    return df.select(column_name).where(col(column_name).isNotNull()).distinct()


def run_task(spark, args):
    staging_full_name = table_identifier(
        args.catalog_name,
        args.staging_namespace,
        args.staging_table,
    )
    fact_events_full_name = table_identifier(
        args.catalog_name,
        args.source_namespace,
        args.fact_events_table,
    )
    fact_sales_full_name = table_identifier(
        args.catalog_name,
        args.source_namespace,
        args.fact_sales_table,
    )

    dim_specs = [
        (
            args.dim_time_table,
            args.dim_time_path,
            dimensions.DIM_TIME_SCHEMA_SQL,
            dimensions.DIM_TIME_COLUMNS,
            "time_id",
        ),
        (
            args.dim_product_table,
            args.dim_product_path,
            dimensions.DIM_PRODUCT_SCHEMA_SQL,
            dimensions.DIM_PRODUCT_COLUMNS,
            "product_id",
        ),
        (
            args.dim_user_table,
            args.dim_user_path,
            dimensions.DIM_USER_SCHEMA_SQL,
            dimensions.DIM_USER_COLUMNS,
            "user_id",
        ),
        (
            args.dim_session_table,
            args.dim_session_path,
            dimensions.DIM_SESSION_SCHEMA_SQL,
            dimensions.DIM_SESSION_COLUMNS,
            "session_id",
        ),
    ]

    log(f"Staging table       : {staging_full_name}")
    log(f"fact_events table   : {fact_events_full_name}")
    log(f"fact_sales table    : {fact_sales_full_name}")
    log(f"Refresh mode        : {args.refresh_mode}")
    log(f"State path          : {args.state_path}")

    create_namespace_if_not_exists(spark, args.catalog_name, args.target_namespace)

    for table, path, schema_sql, _columns, _key_column in dim_specs:
        full_name = table_identifier(args.catalog_name, args.target_namespace, table)
        create_iceberg_table_if_not_exists(spark, full_name, schema_sql, path)

    if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
        partition_dates = active_gold_dates(spark, args.state_path)
        log(f"Active Gold dates    : {partition_dates}")
        if not partition_dates:
            log("No active Gold dates. Dimension build is a no-op.")
            return
    else:
        partition_dates = []

    staging_df = None
    fact_events_df = None
    fact_sales_df = None
    staging_active_df = None
    fact_events_active_df = None
    fact_sales_active_df = None
    outputs = {}

    try:
        staging_df = read_required_table(spark, staging_full_name)
        fact_events_df = read_required_table(spark, fact_events_full_name)
        fact_sales_df = read_required_table(spark, fact_sales_full_name)

        if args.refresh_mode == REFRESH_MODE_FULL:
            staging_df = staging_df.cache()
            fact_events_df = fact_events_df.cache()
            fact_sales_df = fact_sales_df.cache()
            dimensions.validate_inputs(staging_df, fact_events_df, fact_sales_df)

            outputs = {
                args.dim_time_table: dimensions.build_dim_time(staging_df).cache(),
                args.dim_product_table: dimensions.build_dim_product(staging_df).cache(),
                args.dim_user_table: dimensions.build_dim_user(
                    fact_events_df,
                    fact_sales_df,
                ).cache(),
                args.dim_session_table: dimensions.build_dim_session(
                    fact_events_df,
                    fact_sales_df,
                ).cache(),
            }

            for table, _path, _schema_sql, columns, key_column in dim_specs:
                full_name = table_identifier(args.catalog_name, args.target_namespace, table)
                output_df = outputs[table]
                row_count = output_df.count()
                log(f"{full_name} rows: {row_count}")
                validate_dimension(output_df, key_column, full_name)
                write_full_refresh(output_df, full_name, columns, mode=args.refresh_mode)

            log("Completed dimension table full refresh.")
            return

        staging_active_df = _filter_dates(
            staging_df,
            "event_date",
            partition_dates,
        ).cache()
        fact_events_active_df = _filter_dates(
            fact_events_df,
            "event_date",
            partition_dates,
        ).cache()
        fact_sales_active_df = _filter_dates(
            fact_sales_df,
            "sale_date",
            partition_dates,
        ).cache()

        dimensions.validate_inputs(
            staging_active_df,
            fact_events_active_df,
            fact_sales_active_df,
        )

        product_keys = _distinct_non_null(staging_active_df, "product_id")
        user_keys = _distinct_non_null(fact_events_active_df, "user_id")
        session_keys = _distinct_non_null(fact_events_active_df, "session_id")

        outputs = {
            args.dim_time_table: dimensions.build_dim_time(staging_active_df).cache(),
            args.dim_product_table: dimensions.build_dim_product(
                staging_df.join(product_keys, on="product_id", how="inner")
            ).cache(),
            args.dim_user_table: dimensions.build_dim_user(
                fact_events_df.join(user_keys, on="user_id", how="inner"),
                fact_sales_df.join(user_keys, on="user_id", how="inner"),
            ).cache(),
            args.dim_session_table: dimensions.build_dim_session(
                fact_events_df.join(session_keys, on="session_id", how="inner"),
                fact_sales_df.join(session_keys, on="session_id", how="inner"),
            ).cache(),
        }

        for table, _path, _schema_sql, columns, key_column in dim_specs:
            full_name = table_identifier(args.catalog_name, args.target_namespace, table)
            output_df = outputs[table]
            row_count = output_df.count()
            log(f"{full_name} rows: {row_count}")
            validate_dimension(output_df, key_column, full_name)
            if table == args.dim_time_table:
                write_replace_where(
                    output_df,
                    full_name,
                    date_in_condition("event_date", partition_dates),
                    columns,
                )
            else:
                write_replace_keys(output_df, full_name, key_column, columns)

        log("Completed dimension table incremental replace.")
    except Exception as exc:
        if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
            mark_gold_pending_with_error(spark, args.state_path, partition_dates, exc)
        raise
    finally:
        for output_df in outputs.values():
            output_df.unpersist()
        for cached_df in [
            fact_sales_active_df,
            fact_events_active_df,
            staging_active_df,
        ]:
            if cached_df is not None:
                cached_df.unpersist()
        for cached_df in [fact_sales_df, fact_events_df, staging_df]:
            if cached_df is not None and args.refresh_mode == REFRESH_MODE_FULL:
                cached_df.unpersist()


def main(argv=None):
    args = parse_args(argv)
    validate_args(args)

    runtime_config = load_runtime_config(DEFAULT_GOLD_WAREHOUSE)
    log(f"Iceberg warehouse   : {runtime_config.warehouse}")
    log(f"JDBC URI            : {runtime_config.jdbc_uri}")
    log(f"JDBC schema         : {runtime_config.jdbc_schema}")

    spark = None
    try:
        spark = create_spark_session(JOB_NAME, args.catalog_name, runtime_config)
        spark.sparkContext.setLogLevel("WARN")
        run_task(spark, args)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
