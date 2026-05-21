"""Validate semantic Gold metadata tables against real Gold schemas."""

import argparse
import os
import sys


SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from gold import metadata
from gold.config import (
    DEFAULT_CATALOG,
    DEFAULT_GOLD_NAMESPACE,
    DEFAULT_METADATA_NAMESPACE,
    DEFAULT_METADATA_WAREHOUSE,
    DEFAULT_STAGING_NAMESPACE,
    create_spark_session,
    load_runtime_config,
)


JOB_NAME = "GoldValidateMetadata"


def log(message):
    print(f"[GoldValidateMetadata] {message}", flush=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Validate semantic Gold metadata catalog tables."
    )
    parser.add_argument("--catalog-name", default=DEFAULT_CATALOG)
    parser.add_argument("--metadata-namespace", default=DEFAULT_METADATA_NAMESPACE)
    parser.add_argument("--gold-namespace", default=DEFAULT_GOLD_NAMESPACE)
    parser.add_argument("--staging-namespace", default=DEFAULT_STAGING_NAMESPACE)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
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
        row_counts = metadata.validate_metadata_catalogs(
            spark=spark,
            catalog_name=args.catalog_name,
            metadata_namespace=args.metadata_namespace,
            gold_namespace=args.gold_namespace,
            staging_namespace=args.staging_namespace,
        )
        for table_name, row_count in sorted(row_counts.items()):
            log(f"{table_name} rows: {row_count}")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
