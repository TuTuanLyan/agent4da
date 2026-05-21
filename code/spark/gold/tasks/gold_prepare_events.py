"""Prepare Gold staging events from valid Silver Parquet rows."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from gold import staging
from gold.config import (
    DEFAULT_CATALOG,
    DEFAULT_REFRESH_MODE,
    DEFAULT_SILVER_PATH,
    DEFAULT_STAGING_NAMESPACE,
    DEFAULT_STAGING_WAREHOUSE,
    DEFAULT_TEST_STAGING_BASE_PATH,
    STG_EVENTS,
    create_spark_session,
    load_runtime_config,
    require_full_refresh,
    table_location,
)
from gold.ddl import create_iceberg_table_if_not_exists, create_namespace_if_not_exists
from gold.identifiers import assert_safe_table_location, table_identifier
from gold.writers import write_full_refresh


JOB_NAME = "GoldPrepareEventsStaging"


def log(message):
    print(f"[GoldPrepareEvents] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Prepare a Gold staging table from valid Silver events."
    )
    parser.add_argument("--silver-path", default=DEFAULT_SILVER_PATH)
    parser.add_argument("--catalog-name", default=DEFAULT_CATALOG)
    parser.add_argument("--namespace", default=DEFAULT_STAGING_NAMESPACE)
    parser.add_argument("--output-table", default=STG_EVENTS)
    parser.add_argument(
        "--output-path",
        default=table_location(DEFAULT_TEST_STAGING_BASE_PATH, STG_EVENTS),
    )
    parser.add_argument("--refresh-mode", default=DEFAULT_REFRESH_MODE)
    return parser.parse_args(argv)


def validate_args(args):
    args.refresh_mode = require_full_refresh(args.refresh_mode, "gold_prepare_events")
    table_identifier(args.catalog_name, args.namespace, args.output_table)
    assert_safe_table_location(args.output_path, ["s3a://test/"])


def run_task(spark, args):
    full_name = table_identifier(args.catalog_name, args.namespace, args.output_table)

    log(f"Silver path          : {args.silver_path}")
    log(f"Output table        : {full_name}")
    log(f"Output path         : {args.output_path}")
    log(f"Refresh mode        : {args.refresh_mode}")

    silver_df = spark.read.parquet(args.silver_path)
    staging.validate_silver_columns(silver_df)
    filtered_df = staging.filter_valid_events(silver_df).cache()
    output_df = None

    try:
        input_count = filtered_df.count()
        log(f"Rows after filter   : {input_count}")

        dedup_df = staging.deduplicate_by_fingerprint(filtered_df)
        output_df = staging.select_staging_columns(dedup_df).cache()
        output_count = output_df.count()
        log(f"Rows after dedup    : {output_count}")

        create_namespace_if_not_exists(spark, args.catalog_name, args.namespace)
        create_iceberg_table_if_not_exists(
            spark,
            full_name,
            staging.STAGING_SCHEMA_SQL,
            args.output_path,
            partition_clause="event_date",
        )
        write_full_refresh(
            output_df,
            full_name,
            staging.STAGING_COLUMNS,
            mode=args.refresh_mode,
        )

        log(f"Full refresh written: {full_name}")
    finally:
        if output_df is not None:
            output_df.unpersist()
        filtered_df.unpersist()


def main(argv=None):
    args = parse_args(argv)
    validate_args(args)

    runtime_config = load_runtime_config(
        DEFAULT_STAGING_WAREHOUSE,
        "GOLD_STAGING_ICEBERG_WAREHOUSE",
    )
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
