"""Prepare Gold staging events from valid Silver Parquet rows."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from common.partition_state import (
    begin_gold_run,
    mark_gold_pending_with_error,
    path_exists,
    read_state,
)
from gold import staging
from gold.config import (
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_REFRESH_MODE,
    DEFAULT_MAX_GOLD_DATES_PER_RUN,
    DEFAULT_PARTITION_STATE_PATH,
    DEFAULT_SILVER_PATH,
    DEFAULT_STAGING_BASE_PATH,
    DEFAULT_STAGING_NAMESPACE,
    DEFAULT_STAGING_WAREHOUSE,
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
from gold.writers import date_in_condition, write_full_refresh, write_replace_where


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
        default=table_location(DEFAULT_STAGING_BASE_PATH, STG_EVENTS),
    )
    parser.add_argument("--refresh-mode", default=DEFAULT_GOLD_REFRESH_MODE)
    parser.add_argument("--state-path", default=DEFAULT_PARTITION_STATE_PATH)
    parser.add_argument(
        "--max-dates-per-run",
        type=int,
        default=DEFAULT_MAX_GOLD_DATES_PER_RUN,
    )
    parser.add_argument("--run-id", default=None)
    return parser.parse_args(argv)


def validate_args(args):
    args.refresh_mode = normalize_refresh_mode(args.refresh_mode, "gold_prepare_events")
    table_identifier(args.catalog_name, args.namespace, args.output_table)
    assert_safe_table_location(args.output_path, DEFAULT_ALLOWED_LOCATION_PREFIXES)


def _silver_partition_path(base_path, partition_date):
    return f"{base_path.rstrip('/')}/event_date={partition_date}"


def _positive_int(value):
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _read_incremental_silver(spark, args, partition_dates):
    state = read_state(spark, args.state_path)
    paths = []
    missing_dates = []

    for partition_date in partition_dates:
        partition_path = _silver_partition_path(args.silver_path, partition_date)
        if path_exists(spark, partition_path):
            paths.append(partition_path)
            continue

        entry = state.get("partitions", {}).get(partition_date, {})
        if _positive_int(entry.get("silver_valid_row_count")):
            missing_dates.append(partition_date)
        else:
            log(
                "No Silver valid partition for "
                f"{partition_date}; treating it as zero valid rows."
            )

    if missing_dates:
        raise RuntimeError(
            "Missing Silver valid partitions with non-zero state counts: "
            + ", ".join(missing_dates)
        )

    if not paths:
        return None

    return spark.read.option("basePath", args.silver_path.rstrip("/")).parquet(*paths)


def _empty_staging_df(spark, full_name):
    return spark.table(full_name).where("1 = 0")


def run_task(spark, args):
    full_name = table_identifier(args.catalog_name, args.namespace, args.output_table)

    log(f"Silver path          : {args.silver_path}")
    log(f"Output table        : {full_name}")
    log(f"Output path         : {args.output_path}")
    log(f"Refresh mode        : {args.refresh_mode}")
    log(f"State path          : {args.state_path}")
    log(f"Max dates per run   : {args.max_dates_per_run}")

    create_namespace_if_not_exists(spark, args.catalog_name, args.namespace)
    create_iceberg_table_if_not_exists(
        spark,
        full_name,
        staging.STAGING_SCHEMA_SQL,
        args.output_path,
        partition_clause="event_date",
    )

    if args.refresh_mode == REFRESH_MODE_FULL:
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
        return

    if args.refresh_mode != REFRESH_MODE_INCREMENTAL:
        raise ValueError(f"Unsupported refresh mode: {args.refresh_mode}")

    partition_dates = begin_gold_run(
        spark,
        args.state_path,
        args.max_dates_per_run,
        run_id=args.run_id,
    )
    log(f"Gold dates claimed  : {partition_dates}")
    if not partition_dates:
        log("No pending Gold dates. Nothing to prepare.")
        return

    silver_df = None
    filtered_df = None
    output_df = None

    try:
        silver_df = _read_incremental_silver(spark, args, partition_dates)
        if silver_df is None:
            output_df = _empty_staging_df(spark, full_name).cache()
            log("Rows after filter   : 0")
            log("Rows after dedup    : 0")
        else:
            staging.validate_silver_columns(silver_df)
            filtered_df = staging.filter_valid_events(silver_df).cache()
            input_count = filtered_df.count()
            log(f"Rows after filter   : {input_count}")

            dedup_df = staging.deduplicate_by_fingerprint(filtered_df)
            output_df = staging.select_staging_columns(dedup_df).cache()
            output_count = output_df.count()
            log(f"Rows after dedup    : {output_count}")

        write_replace_where(
            output_df,
            full_name,
            date_in_condition("event_date", partition_dates),
            staging.STAGING_COLUMNS,
        )

        log(f"Incremental staging written: {full_name}")
    except Exception as exc:
        mark_gold_pending_with_error(spark, args.state_path, partition_dates, exc)
        raise
    finally:
        if output_df is not None:
            output_df.unpersist()
        if filtered_df is not None:
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
