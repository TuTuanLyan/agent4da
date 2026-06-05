"""Mark the active Gold manifest date set as completed."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from common.partition_state import active_gold_dates, mark_gold_done
from gold.config import (
    DEFAULT_CATALOG,
    DEFAULT_GOLD_REFRESH_MODE,
    DEFAULT_GOLD_WAREHOUSE,
    DEFAULT_PARTITION_STATE_PATH,
    REFRESH_MODE_FULL,
    REFRESH_MODE_INCREMENTAL,
    create_spark_session,
    load_runtime_config,
    normalize_refresh_mode,
)


JOB_NAME = "GoldMarkDone"


def log(message):
    print(f"[GoldMarkDone] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Mark active Gold incremental partitions as DONE."
    )
    parser.add_argument("--catalog-name", default=DEFAULT_CATALOG)
    parser.add_argument("--state-path", default=DEFAULT_PARTITION_STATE_PATH)
    parser.add_argument("--refresh-mode", default=DEFAULT_GOLD_REFRESH_MODE)
    return parser.parse_args(argv)


def validate_args(args):
    args.refresh_mode = normalize_refresh_mode(args.refresh_mode, "gold_mark_done")


def run_task(spark, args):
    log(f"Refresh mode        : {args.refresh_mode}")
    log(f"State path          : {args.state_path}")

    if args.refresh_mode == REFRESH_MODE_FULL:
        log("Full refresh does not mutate partition manifest status.")
        return []

    if args.refresh_mode != REFRESH_MODE_INCREMENTAL:
        raise ValueError(f"Unsupported refresh mode: {args.refresh_mode}")

    partition_dates = active_gold_dates(spark, args.state_path)
    log(f"Active Gold dates    : {partition_dates}")
    if not partition_dates:
        log("No active Gold dates. Nothing to mark DONE.")
        return []

    changed_dates = mark_gold_done(spark, args.state_path, partition_dates)
    log(f"Marked Gold DONE    : {changed_dates}")
    return changed_dates


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
