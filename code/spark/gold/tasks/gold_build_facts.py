"""Build Gold fact tables from the Gold staging Iceberg table."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from gold import facts
from gold.config import (
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_BASE_PATH,
    DEFAULT_GOLD_NAMESPACE,
    DEFAULT_GOLD_WAREHOUSE,
    DEFAULT_REFRESH_MODE,
    DEFAULT_STAGING_NAMESPACE,
    FACT_EVENTS,
    FACT_SALES,
    STG_EVENTS,
    create_spark_session,
    load_runtime_config,
    require_full_refresh,
    table_location,
)
from gold.ddl import create_iceberg_table_if_not_exists, create_namespace_if_not_exists
from gold.identifiers import assert_safe_table_location, table_identifier
from gold.readers import read_required_table
from gold.writers import write_full_refresh


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
    parser.add_argument("--refresh-mode", default=DEFAULT_REFRESH_MODE)
    return parser.parse_args(argv)


def validate_args(args):
    args.refresh_mode = require_full_refresh(args.refresh_mode, "gold_build_facts")
    table_identifier(args.catalog_name, args.staging_namespace, args.staging_table)
    table_identifier(args.catalog_name, args.target_namespace, args.fact_events_table)
    table_identifier(args.catalog_name, args.target_namespace, args.fact_sales_table)
    assert_safe_table_location(args.fact_events_path, DEFAULT_ALLOWED_LOCATION_PREFIXES)
    assert_safe_table_location(args.fact_sales_path, DEFAULT_ALLOWED_LOCATION_PREFIXES)


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

    staging_df = read_required_table(spark, staging_full_name).cache()
    fact_events_df = None
    fact_sales_df = None

    try:
        staging_count = staging_df.count()
        log(f"Staging rows read   : {staging_count}")

        fact_events_df = facts.build_fact_events(staging_df).cache()
        fact_sales_df = facts.build_fact_sales(fact_events_df).cache()
        metrics = facts.validate_fact_outputs(fact_events_df, fact_sales_df)
        log(f"fact_events rows    : {metrics['fact_events']}")
        log(f"purchase rows       : {metrics['purchases']}")
        log(f"fact_sales rows     : {metrics['fact_sales']}")

        create_namespace_if_not_exists(spark, args.catalog_name, args.target_namespace)
        create_iceberg_table_if_not_exists(
            spark,
            fact_events_full_name,
            facts.FACT_EVENTS_SCHEMA_SQL,
            args.fact_events_path,
        )
        create_iceberg_table_if_not_exists(
            spark,
            fact_sales_full_name,
            facts.FACT_SALES_SCHEMA_SQL,
            args.fact_sales_path,
        )

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
    finally:
        if fact_sales_df is not None:
            fact_sales_df.unpersist()
        if fact_events_df is not None:
            fact_events_df.unpersist()
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
