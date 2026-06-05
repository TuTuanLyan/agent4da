"""Validate semantic Gold metadata tables against real Gold schemas."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from gold.config import (
    DEFAULT_CATALOG,
    DEFAULT_GOLD_NAMESPACE,
    DEFAULT_METADATA_NAMESPACE,
    DEFAULT_METADATA_WAREHOUSE,
    DEFAULT_REFRESH_MODE,
    REFRESH_MODE_INCREMENTAL,
    create_spark_session,
    load_runtime_config,
)


JOB_NAME = "ValidateGoldAgentMetadata"


def log(message):
    print(f"[ValidateGoldAgentMetadata] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Validate Gold agent metadata table and column names."
    )
    parser.add_argument("--catalog-name", default=DEFAULT_CATALOG)
    parser.add_argument("--metadata-namespace", default=DEFAULT_METADATA_NAMESPACE)
    parser.add_argument("--gold-namespace", default=DEFAULT_GOLD_NAMESPACE)
    parser.add_argument("--partition-state-path", default=None)
    parser.add_argument("--gold-refresh-mode", default=DEFAULT_REFRESH_MODE)
    return parser.parse_args(argv)


def mark_gold_error_if_needed(spark, args, error):
    if not args.partition_state_path:
        return
    if str(args.gold_refresh_mode).strip().lower() != REFRESH_MODE_INCREMENTAL:
        return

    from common.partition_state import active_gold_dates, mark_gold_pending_with_error

    partition_dates = active_gold_dates(spark, args.partition_state_path)
    if partition_dates:
        mark_gold_pending_with_error(
            spark,
            args.partition_state_path,
            partition_dates,
            error,
        )


def main(argv=None):
    args = parse_args(argv)
    from gold import metadata

    runtime_config = load_runtime_config(
        DEFAULT_METADATA_WAREHOUSE,
        "GOLD_METADATA_ICEBERG_WAREHOUSE",
    )
    log(f"Iceberg warehouse   : {runtime_config.warehouse}")
    log(f"JDBC URI            : {runtime_config.jdbc_uri}")
    log(f"JDBC schema         : {runtime_config.jdbc_schema}")

    spark = None
    try:
        spark = create_spark_session(JOB_NAME, args.catalog_name, runtime_config)
        spark.sparkContext.setLogLevel("WARN")
        try:
            row_counts = metadata.validate_metadata_catalogs(
                spark=spark,
                catalog_name=args.catalog_name,
                metadata_namespace=args.metadata_namespace,
                gold_namespace=args.gold_namespace,
            )
        except Exception as exc:
            mark_gold_error_if_needed(spark, args, exc)
            raise
        for table_name, row_count in sorted(row_counts.items()):
            log(f"{table_name} rows: {row_count}")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
