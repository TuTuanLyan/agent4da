"""
Smoke test for Spark + Apache Iceberg JDBC Catalog + MinIO warehouse.

Stage 1 only verifies infrastructure readiness. It does not build Gold
business fact/dimension tables.
"""

import os
import re
import sys
import traceback

from pyspark.sql import SparkSession


APP_NAME = "IcebergSmokeTest"
NAMESPACE = "gold"
TABLE_NAME = "iceberg_smoke_test"


def env(name, default):
    return os.getenv(name, default)


MINIO_ENDPOINT = env("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", "Admin123!")

ICEBERG_CATALOG_NAME = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
ICEBERG_WAREHOUSE = env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/")
ICEBERG_JDBC_URI = env(
    "ICEBERG_JDBC_URI",
    "jdbc:postgresql://postgres-db:5432/agent4da",
)
ICEBERG_JDBC_USER = env("ICEBERG_JDBC_USER", "bigdata")
ICEBERG_JDBC_PASSWORD = env("ICEBERG_JDBC_PASSWORD", "#3Bigdata")
ICEBERG_JDBC_SCHEMA = env("ICEBERG_JDBC_SCHEMA", "iceberg")


def log(message):
    print(f"[IcebergSmokeTest] {message}", flush=True)


def validate_identifier(identifier, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid {label}: {identifier!r}")


def create_spark_session():
    catalog = ICEBERG_CATALOG_NAME
    validate_identifier(catalog, "ICEBERG_CATALOG_NAME")

    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{catalog}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.uri", ICEBERG_JDBC_URI)
        .config(f"spark.sql.catalog.{catalog}.jdbc.user", ICEBERG_JDBC_USER)
        .config(f"spark.sql.catalog.{catalog}.jdbc.password", ICEBERG_JDBC_PASSWORD)
        .config(f"spark.sql.catalog.{catalog}.jdbc.currentSchema", ICEBERG_JDBC_SCHEMA)
        .config(f"spark.sql.catalog.{catalog}.warehouse", ICEBERG_WAREHOUSE)
        .config(
            f"spark.sql.catalog.{catalog}.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run_smoke_test(spark):
    catalog = ICEBERG_CATALOG_NAME
    validate_identifier(catalog, "ICEBERG_CATALOG_NAME")
    validate_identifier(NAMESPACE, "namespace")
    validate_identifier(TABLE_NAME, "table")

    table = f"{catalog}.{NAMESPACE}.{TABLE_NAME}"

    log(f"Catalog: {catalog}")
    log(f"Warehouse: {ICEBERG_WAREHOUSE}")
    log(f"JDBC URI: {ICEBERG_JDBC_URI}")
    log(f"JDBC schema: {ICEBERG_JDBC_SCHEMA}")
    log(f"MinIO endpoint: {MINIO_ENDPOINT}")

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
