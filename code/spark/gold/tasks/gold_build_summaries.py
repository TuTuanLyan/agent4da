"""Build Gold summary tables from fact and dimension Iceberg tables."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from pyspark.sql.functions import col

from common.partition_state import active_gold_dates, mark_gold_pending_with_error
from gold import summaries
from gold.config import (
    DAILY_BRAND_SUMMARY,
    DAILY_CATEGORY_SUMMARY,
    DAILY_EVENT_SUMMARY,
    DAILY_PRODUCT_SUMMARY,
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_BASE_PATH,
    DEFAULT_GOLD_REFRESH_MODE,
    DEFAULT_GOLD_NAMESPACE,
    DEFAULT_GOLD_WAREHOUSE,
    DEFAULT_PARTITION_STATE_PATH,
    DIM_PRODUCT,
    FACT_EVENTS,
    FACT_SALES,
    REFRESH_MODE_FULL,
    REFRESH_MODE_INCREMENTAL,
    create_spark_session,
    load_runtime_config,
    normalize_refresh_mode,
    table_location,
)
from gold.ddl import create_iceberg_table_if_not_exists, create_namespace_if_not_exists
from gold.identifiers import assert_safe_table_location, table_identifier
from gold.readers import read_required_table
from gold.writers import date_in_condition, write_full_refresh, write_replace_where


JOB_NAME = "GoldBuildSummaries"
SUMMARY_CHOICES = ["all", "event", "product", "category", "brand"]


def log(message):
    print(f"[GoldBuildSummaries] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Build Gold summary tables from facts and dimensions."
    )
    parser.add_argument("--catalog-name", default=DEFAULT_CATALOG)
    parser.add_argument("--source-namespace", default=DEFAULT_GOLD_NAMESPACE)
    parser.add_argument("--target-namespace", default=DEFAULT_GOLD_NAMESPACE)
    parser.add_argument("--fact-events-table", default=FACT_EVENTS)
    parser.add_argument("--fact-sales-table", default=FACT_SALES)
    parser.add_argument("--dim-product-table", default=DIM_PRODUCT)
    parser.add_argument("--daily-event-summary-table", default=DAILY_EVENT_SUMMARY)
    parser.add_argument("--daily-product-summary-table", default=DAILY_PRODUCT_SUMMARY)
    parser.add_argument("--daily-category-summary-table", default=DAILY_CATEGORY_SUMMARY)
    parser.add_argument("--daily-brand-summary-table", default=DAILY_BRAND_SUMMARY)
    parser.add_argument(
        "--daily-event-summary-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DAILY_EVENT_SUMMARY),
    )
    parser.add_argument(
        "--daily-product-summary-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DAILY_PRODUCT_SUMMARY),
    )
    parser.add_argument(
        "--daily-category-summary-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DAILY_CATEGORY_SUMMARY),
    )
    parser.add_argument(
        "--daily-brand-summary-path",
        default=table_location(DEFAULT_GOLD_BASE_PATH, DAILY_BRAND_SUMMARY),
    )
    parser.add_argument("--summary", choices=SUMMARY_CHOICES, default="all")
    parser.add_argument("--refresh-mode", default=DEFAULT_GOLD_REFRESH_MODE)
    parser.add_argument("--state-path", default=DEFAULT_PARTITION_STATE_PATH)
    return parser.parse_args(argv)


def validate_args(args):
    args.refresh_mode = normalize_refresh_mode(args.refresh_mode, "gold_build_summaries")
    table_identifier(args.catalog_name, args.source_namespace, args.fact_events_table)
    table_identifier(args.catalog_name, args.source_namespace, args.fact_sales_table)
    table_identifier(args.catalog_name, args.source_namespace, args.dim_product_table)
    for table in [
        args.daily_event_summary_table,
        args.daily_product_summary_table,
        args.daily_category_summary_table,
        args.daily_brand_summary_table,
    ]:
        table_identifier(args.catalog_name, args.target_namespace, table)
    for path in [
        args.daily_event_summary_path,
        args.daily_product_summary_path,
        args.daily_category_summary_path,
        args.daily_brand_summary_path,
    ]:
        assert_safe_table_location(path, DEFAULT_ALLOWED_LOCATION_PREFIXES)


def selected_summary_specs(args):
    specs = {
        "event": {
            "table": args.daily_event_summary_table,
            "path": args.daily_event_summary_path,
            "schema_sql": summaries.DAILY_EVENT_SUMMARY_SCHEMA_SQL,
            "columns": summaries.DAILY_EVENT_SUMMARY_COLUMNS,
            "builder": summaries.build_daily_event_summary,
            "validator": summaries.validate_daily_event_summary,
        },
        "product": {
            "table": args.daily_product_summary_table,
            "path": args.daily_product_summary_path,
            "schema_sql": summaries.DAILY_PRODUCT_SUMMARY_SCHEMA_SQL,
            "columns": summaries.DAILY_PRODUCT_SUMMARY_COLUMNS,
            "builder": summaries.build_daily_product_summary,
            "validator": summaries.validate_summary_id,
        },
        "category": {
            "table": args.daily_category_summary_table,
            "path": args.daily_category_summary_path,
            "schema_sql": summaries.DAILY_CATEGORY_SUMMARY_SCHEMA_SQL,
            "columns": summaries.DAILY_CATEGORY_SUMMARY_COLUMNS,
            "builder": summaries.build_daily_category_summary,
            "validator": summaries.validate_summary_id,
        },
        "brand": {
            "table": args.daily_brand_summary_table,
            "path": args.daily_brand_summary_path,
            "schema_sql": summaries.DAILY_BRAND_SUMMARY_SCHEMA_SQL,
            "columns": summaries.DAILY_BRAND_SUMMARY_COLUMNS,
            "builder": summaries.build_daily_brand_summary,
            "validator": summaries.validate_summary_id,
        },
    }
    if args.summary == "all":
        return specs
    return {args.summary: specs[args.summary]}


def build_summary_df(kind, builder, fact_events_df, fact_sales_df, dim_product_df):
    if kind == "event":
        return builder(fact_events_df, fact_sales_df)
    return builder(fact_events_df, fact_sales_df, dim_product_df)


def validate_summary(kind, validator, output_df, fact_events_df, fact_sales_df, full_name):
    if kind == "event":
        validator(output_df, fact_events_df, fact_sales_df, full_name)
    else:
        validator(output_df, full_name)


def _filter_dates(df, column_name, partition_dates):
    return df.where(col(column_name).cast("string").isin(partition_dates))


def run_task(spark, args):
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
    dim_product_full_name = table_identifier(
        args.catalog_name,
        args.source_namespace,
        args.dim_product_table,
    )
    specs = selected_summary_specs(args)

    log(f"fact_events table   : {fact_events_full_name}")
    log(f"fact_sales table    : {fact_sales_full_name}")
    log(f"dim_product table   : {dim_product_full_name}")
    log(f"summary selection   : {args.summary}")
    log(f"Refresh mode        : {args.refresh_mode}")
    log(f"State path          : {args.state_path}")

    create_namespace_if_not_exists(spark, args.catalog_name, args.target_namespace)

    for _kind, spec in specs.items():
        full_name = table_identifier(
            args.catalog_name,
            args.target_namespace,
            spec["table"],
        )
        create_iceberg_table_if_not_exists(
            spark,
            full_name,
            spec["schema_sql"],
            spec["path"],
            partition_clause="event_date",
        )

    if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
        partition_dates = active_gold_dates(spark, args.state_path)
        log(f"Active Gold dates    : {partition_dates}")
        if not partition_dates:
            log("No active Gold dates. Summary build is a no-op.")
            return
    else:
        partition_dates = []

    fact_events_df = None
    fact_sales_df = None
    dim_product_df = None
    output_df = None

    try:
        fact_events_df = read_required_table(spark, fact_events_full_name)
        fact_sales_df = read_required_table(spark, fact_sales_full_name)
        dim_product_df = read_required_table(spark, dim_product_full_name).cache()

        if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
            fact_events_df = _filter_dates(
                fact_events_df,
                "event_date",
                partition_dates,
            )
            fact_sales_df = _filter_dates(
                fact_sales_df,
                "sale_date",
                partition_dates,
            )

        fact_events_df = fact_events_df.cache()
        fact_sales_df = fact_sales_df.cache()
        summaries.validate_inputs(fact_events_df, fact_sales_df, dim_product_df)
        log(f"fact_events rows    : {fact_events_df.count()}")
        log(f"fact_sales rows     : {fact_sales_df.count()}")
        log(f"dim_product rows    : {dim_product_df.count()}")

        for kind, spec in specs.items():
            full_name = table_identifier(
                args.catalog_name,
                args.target_namespace,
                spec["table"],
            )
            log(f"Building {kind} summary: {full_name}")
            log(f"Output path          : {spec['path']}")

            output_df = build_summary_df(
                kind,
                spec["builder"],
                fact_events_df,
                fact_sales_df,
                dim_product_df,
            ).cache()
            row_count = output_df.count()
            log(f"{full_name} rows: {row_count}")

            validate_summary(
                kind,
                spec["validator"],
                output_df,
                fact_events_df,
                fact_sales_df,
                full_name,
            )
            if args.refresh_mode == REFRESH_MODE_FULL:
                write_full_refresh(
                    output_df,
                    full_name,
                    spec["columns"],
                    mode=args.refresh_mode,
                )
            else:
                write_replace_where(
                    output_df,
                    full_name,
                    date_in_condition("event_date", partition_dates),
                    spec["columns"],
                )
            output_df.unpersist()
            output_df = None

        if args.refresh_mode == REFRESH_MODE_FULL:
            log("Completed summary table full refresh.")
        else:
            log("Completed summary table incremental replace.")
    except Exception as exc:
        if args.refresh_mode == REFRESH_MODE_INCREMENTAL:
            mark_gold_pending_with_error(spark, args.state_path, partition_dates, exc)
        raise
    finally:
        if output_df is not None:
            output_df.unpersist()
        for cached_df in [dim_product_df, fact_sales_df, fact_events_df]:
            if cached_df is not None:
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
