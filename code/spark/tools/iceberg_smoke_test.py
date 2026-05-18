"""
Smoke test for Spark + Apache Iceberg JDBC Catalog + MinIO warehouse.

Stage 1 only verifies infrastructure readiness. It does not build Gold
business fact/dimension tables.
"""

import os
import sys
import traceback

SPARK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from common.config import load_config, validate_identifier
from common.spark_session import create_spark_session as build_spark_session


APP_NAME = "IcebergSmokeTest"
TABLE_NAME = "iceberg_smoke_test"
CONFIG = load_config()
NAMESPACE = CONFIG.gold_namespace


def log(message):
    print(f"[IcebergSmokeTest] {message}", flush=True)


def create_spark_session():
    return build_spark_session(APP_NAME, enable_iceberg=True, pipeline_config=CONFIG)


def run_smoke_test(spark):
    catalog = CONFIG.catalog_name
    validate_identifier(catalog, "ICEBERG_CATALOG_NAME")
    validate_identifier(NAMESPACE, "namespace")
    validate_identifier(TABLE_NAME, "table")

    table = f"{catalog}.{NAMESPACE}.{TABLE_NAME}"

    log(f"Catalog: {catalog}")
    log(f"Warehouse: {CONFIG.warehouse}")
    log(f"JDBC URI: {CONFIG.jdbc_uri}")
    log(f"JDBC schema: {CONFIG.jdbc_schema}")
    log(f"MinIO endpoint: {CONFIG.minio.endpoint}")

    spark.sql("SELECT current_catalog(), current_database()").show(truncate=False)

    log(f"Creating namespace {catalog}.{NAMESPACE}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{NAMESPACE}")

    log(f"Recreating table {table}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"""
        CREATE TABLE {table} (
          id BIGINT,
          name STRING,
          created_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
          'format-version'='2'
        )
        """
    )

    log(f"Inserting smoke rows into {table}")
    spark.sql(
        f"""
        INSERT INTO {table} VALUES
          (1, 'alpha', TIMESTAMP '2026-05-01 00:00:00'),
          (2, 'bravo', TIMESTAMP '2026-05-01 00:01:00'),
          (3, 'charlie', TIMESTAMP '2026-05-01 00:02:00')
        """
    )

    log(f"Reading back {table}")
    spark.sql(f"SELECT * FROM {table} ORDER BY id").show(truncate=False)

    log(f"Namespaces in {catalog}")
    spark.sql(f"SHOW NAMESPACES IN {catalog}").show(truncate=False)

    log(f"Tables in {catalog}.{NAMESPACE}")
    spark.sql(f"SHOW TABLES IN {catalog}.{NAMESPACE}").show(truncate=False)

    log("SUCCESS")


def main():
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        run_smoke_test(spark)
    except Exception as exc:
        print(
            f"[IcebergSmokeTest] FAILED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
