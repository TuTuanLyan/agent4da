"""Build Gold fact tables from the Gold staging Iceberg table."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from pyspark.sql.functions import col

from common.partition_state import active_gold_dates, mark_gold_pending_with_error
from gold import facts
from gold.config import (
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_BASE_PATH,
    DEFAULT_GOLD_REFRESH_MODE,
    DEFAULT_GOLD_NAMESPACE,
    DEFAULT_GOLD_WAREHOUSE,
    DEFAULT_PARTITION_STATE_PATH,
    DEFAULT_STAGING_NAMESPACE,
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
from gold.writers import date_in_condition, write_full_refresh, write_replace_where


JOB_NAME = "GoldBuildFacts"


def log(message):
    print(f"[GoldBuildFacts] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Build Gold fact tables from the staging Iceberg table."
    )
    parser.add_argument("--catalog-name", default=DEFAULT_CATALOG)
    parser.add_argument("--staging-namespace", default=DEFAULT_STAGING_NAMESPACE)
    parser.add_argument("--staging-table", default=STG_EVENTS)
    parser.add_argument("--target-namespace", default=DEFAULT_GOLD_NAMESPACE)
    parser.add_argument("--fact-events-table", default=FACT_EVENTS)
    parser.add_argument("--fact-sales-table", default=FACT_SALES)
    parser.add_argument(
        "--fact-events-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, FACT_EVENTS),
    )
    parser.add_argument(
        "--fact-sales-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, FACT_SALES),
    )
    parser.add_argument("--refresh-mode", default=DEFAULT_GOLD_REFRESH_MODE)
    parser.add_argument("--state-path", default=DEFAULT_PARTITION_STATE_PATH)
    return parser.parse_args(argv)


def validate_args(args):
    args.refresh_mode = normalize_refresh_mode(args.refresh_mode, "gold_build_facts")
    table_identifier(args.catalog_name, args.staging_namespace, args.staging_table)
    table_identifier(args.catalog_name, args.target_namespace, args.fact_events_table)
    table_identifier(args.catalog_name, args.target_namespace, args.fact_sales_table)
    assert_safe_table_location(args.fact_events_path, DEFAULT_ALLOWED_LOCATION_PREFIXES)
    assert_safe_table_location(args.fact_sales_path, DEFAULT_ALLOWED_LOCATION_PREFIXES)


def _filter_dates(df, column_name, partition_dates):
    return df.where(col(column_name).cast("string").isin(partition_dates))


def run_task(spark, args):
    staging_full_name = table_identifier(
        args.catalog_name,
        args.staging_namespace,
        args.staging_table,
    )
    fact_events_full_name = table_identifier(
        args.catalog_name,
        args.target_namespace,
        args.fact_events_table,
    )
    fact_sales_full_name = table_identifier(
        args.catalog_name,
        args.target_namespace,
        args.fact_sales_table,
    )

    log(f"Staging table       : {staging_full_name}")
    log(f"fact_events table   : {fact_events_full_name}")
    log(f"fact_events path    : {args.fact_events_path}")
    log(f"fact_sales table    : {fact_sales_full_name}")
    log(f"fact_sales path     : {args.fact_sales_path}")
    log(f"Refresh mode        : {args.refresh_mode}")
    log(f"State path          : {args.state_path}")

    create_namespace_if_not_exists(spark, args.catalog_name, args.target_namespace)
    create_iceberg_table_if_not_exists(
        spark,
        fact_events_full_name,
        facts.FACT_EVENTS_SCHEMA_SQL,
        args.fact_events_path,
        partition_clause="event_date",
    )
    create_iceberg_table_if_not_exists(
        spark,
        fact_sales_full_name,
        facts.FACT_SALES_SCHEMA_SQL,
        args.fact_sales_path,
        partition_clause="sale_date",
    )

    if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
        partition_dates = active_gold_dates(spark, args.state_path)
        log(f"Active Gold dates    : {partition_dates}")
        if not partition_dates:
            log("No active Gold dates. Fact build is a no-op.")
            return
    else:
        partition_dates = []

    staging_df = None
    fact_events_df = None
    fact_sales_df = None

    try:
        staging_df = read_required_table(spark, staging_full_name)
        if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
            staging_df = _filter_dates(staging_df, "event_date", partition_dates)
        staging_df = staging_df.cache()

        staging_count = staging_df.count()
        log(f"Staging rows read   : {staging_count}")

        fact_events_df = facts.build_fact_events(staging_df).cache()
        fact_sales_df = facts.build_fact_sales(fact_events_df).cache()
        metrics = facts.validate_fact_outputs(fact_events_df, fact_sales_df)
        log(f"fact_events rows    : {metrics['fact_events']}")
        log(f"purchase rows       : {metrics['purchases']}")
        log(f"fact_sales rows     : {metrics['fact_sales']}")

        if args.refresh_mode == REFRESH_MODE_FULL:
            write_full_refresh(
                fact_events_df,
                fact_events_full_name,
                facts.FACT_EVENTS_COLUMNS,
                mode=args.refresh_mode,
            )
            write_full_refresh(
                fact_sales_df,
                fact_sales_full_name,
                facts.FACT_SALES_COLUMNS,
                mode=args.refresh_mode,
            )
            log("Completed fact table full refresh.")
            return

        write_replace_where(
            fact_events_df,
            fact_events_full_name,
            date_in_condition("event_date", partition_dates),
            facts.FACT_EVENTS_COLUMNS,
        )
        write_replace_where(
            fact_sales_df,
            fact_sales_full_name,
            date_in_condition("sale_date", partition_dates),
            facts.FACT_SALES_COLUMNS,
        )
        log("Completed fact table incremental replace.")
    except Exception as exc:
        if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
            mark_gold_pending_with_error(spark, args.state_path, partition_dates, exc)
        raise
    finally:
        if fact_sales_df is not None:
            fact_sales_df.unpersist()
        if fact_events_df is not None:
            fact_events_df.unpersist()
        if staging_df is not None:
            staging_df.unpersist()


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
