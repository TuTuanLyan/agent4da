"""
Consolidated Gold Layer job for Agent4DA.

Airflow calls this single entrypoint. Detailed DDL, readers, builders,
writers, and validators live in code/spark/gold/.
"""

import sys
import traceback

from common.config import load_config
from common.iceberg import ensure_namespace
from common.logging_utils import log, log_gold_config
from common.spark_session import create_spark_session
from gold.builders_extended import build_extended_outputs
from gold.builders_metadata import build_metadata_outputs
from gold.builders_mvp import build_mvp_outputs
from gold.ddl import create_gold_tables, create_metadata_tables
from gold.readers import read_silver_events
from gold.validators import validate_outputs
from gold.writers import (
    write_all_extended_tables,
    write_all_metadata_tables,
    write_all_mvp_tables,
)


APP_NAME = "GoldJob"


def ensure_namespaces(spark, config):
    ensure_namespace(spark, config.catalog_name, config.gold_namespace)
    ensure_namespace(spark, config.catalog_name, config.metadata_namespace)


def build_data_outputs(base_df, needs_mvp, needs_extended):
    mvp_outputs = build_mvp_outputs(base_df)
    extended_outputs = {}

    if needs_extended:
        extended_outputs = build_extended_outputs(mvp_outputs)

    if not needs_mvp and not needs_extended:
        return {}, {}

    return mvp_outputs, extended_outputs


def run_gold_data_flow(spark, config):
    needs_mvp = config.run_mode in {"all", "mvp_only"}
    needs_extended = config.run_mode in {"all", "extended_only"}
    needs_metadata = config.run_mode == "all"

    base_df = None
    try:
        base_df = read_silver_events(spark, config)
        mvp_outputs, extended_outputs = build_data_outputs(
            base_df,
            needs_mvp,
            needs_extended,
        )

        if needs_mvp:
            write_all_mvp_tables(config, mvp_outputs)

        if needs_extended:
            write_all_extended_tables(config, extended_outputs)

        if needs_metadata:
            metadata_outputs = build_metadata_outputs(spark, config)
            write_all_metadata_tables(config, metadata_outputs)
    finally:
        if base_df is not None:
            base_df.unpersist()


def run_metadata_only(spark, config):
    metadata_outputs = build_metadata_outputs(spark, config)
    write_all_metadata_tables(config, metadata_outputs)


def main():
    spark = None
    try:
        config = load_config()
        log_gold_config(config)

        if config.refresh_mode == "append":
            log("GoldJob", "WARNING: append mode can duplicate rows when re-running on the same Silver input.")

        spark = create_spark_session(APP_NAME, enable_iceberg=True, pipeline_config=config)
        spark.sparkContext.setLogLevel("WARN")

        if config.run_mode == "validate_only":
            validate_outputs(spark, config)
            log("GoldJob", "SUCCESS")
            return

        ensure_namespaces(spark, config)
        create_gold_tables(spark, config)
        create_metadata_tables(spark, config)

        if config.run_mode == "schema_only":
            validate_outputs(spark, config)
            log("GoldJob", "SUCCESS")
            return

        if config.run_mode == "metadata_only":
            run_metadata_only(spark, config)
        else:
            run_gold_data_flow(spark, config)

        validate_outputs(spark, config)
        log("GoldJob", "SUCCESS")
    except Exception as exc:
        print(f"[GoldJob] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
