"""
Initialize Gold Extended Iceberg schema.

Stage 4 adds analytics tables on top of Gold MVP. It does not drop MVP tables
and only drops extended tables when RESET_GOLD_EXTENDED_SCHEMA=true.
"""

import os
import re
import sys
import traceback

from pyspark.sql import SparkSession


APP_NAME = "GoldExtendedSchemaInitJob"


def env(name, default):
    return os.getenv(name, default)


MINIO_ENDPOINT = env("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", "Admin123!")

ICEBERG_CATALOG_NAME = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
ICEBERG_NAMESPACE = env("ICEBERG_NAMESPACE", "gold")
ICEBERG_WAREHOUSE = env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/")
ICEBERG_JDBC_URI = env("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres-db:5432/agent4da")
ICEBERG_JDBC_USER = env("ICEBERG_JDBC_USER", "bigdata")
ICEBERG_JDBC_PASSWORD = env("ICEBERG_JDBC_PASSWORD", "#3Bigdata")
ICEBERG_JDBC_SCHEMA = env("ICEBERG_JDBC_SCHEMA", "iceberg")
RESET_GOLD_EXTENDED_SCHEMA = (
    env("RESET_GOLD_EXTENDED_SCHEMA", "false").strip().lower() == "true"
)

EXTENDED_TABLES = [
    "dim_user",
    "dim_session",
    "daily_product_summary",
    "daily_category_summary",
    "daily_brand_summary",
]

DROP_ORDER = [
    "daily_brand_summary",
    "daily_category_summary",
    "daily_product_summary",
    "dim_session",
    "dim_user",
]


def log(message):
    print(f"[GoldExtendedSchemaInitJob] {message}", flush=True)


def validate_identifier(identifier, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid {label}: {identifier!r}")


def table_name(short_name):
    validate_identifier(ICEBERG_CATALOG_NAME, "ICEBERG_CATALOG_NAME")
    validate_identifier(ICEBERG_NAMESPACE, "ICEBERG_NAMESPACE")
    validate_identifier(short_name, "table_name")
    return f"{ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}.{short_name}"


def create_spark_session():
    catalog = ICEBERG_CATALOG_NAME
    validate_identifier(catalog, "ICEBERG_CATALOG_NAME")
    validate_identifier(ICEBERG_NAMESPACE, "ICEBERG_NAMESPACE")

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
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"spark.sql.catalog.{catalog}.uri", ICEBERG_JDBC_URI)
        .config(f"spark.sql.catalog.{catalog}.jdbc.user", ICEBERG_JDBC_USER)
        .config(f"spark.sql.catalog.{catalog}.jdbc.password", ICEBERG_JDBC_PASSWORD)
        .config(f"spark.sql.catalog.{catalog}.jdbc.currentSchema", ICEBERG_JDBC_SCHEMA)
        .config(f"spark.sql.catalog.{catalog}.warehouse", ICEBERG_WAREHOUSE)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run_sql(spark, sql_text, description=None):
    if description:
        log(description)
    return spark.sql(sql_text)


def create_namespace(spark):
    run_sql(
        spark,
        f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}",
        f"Creating namespace {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}",
    )


def drop_tables_if_reset_enabled(spark):
    if not RESET_GOLD_EXTENDED_SCHEMA:
        log("RESET_GOLD_EXTENDED_SCHEMA is disabled. Existing extended tables will not be dropped.")
        return

    log("RESET_GOLD_EXTENDED_SCHEMA is enabled. Dropping Gold Extended tables.")
    for short_name in DROP_ORDER:
        run_sql(spark, f"DROP TABLE IF EXISTS {table_name(short_name)}", f"Dropping {table_name(short_name)}")


def create_extended_tables(spark):
    ddl_statements = [
        (
            "dim_user",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name("dim_user")} (
              user_id BIGINT,
              first_seen_at TIMESTAMP,
              last_seen_at TIMESTAMP,
              total_sessions BIGINT,
              total_events BIGINT,
              total_views BIGINT,
              total_cart_adds BIGINT,
              total_remove_from_carts BIGINT,
              total_purchases BIGINT,
              total_revenue DECIMAL(18,2),
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='dimension',
              'gold.grain'='one row per user',
              'gold.primary_key'='user_id',
              'gold.source'='gold.fact_events and gold.fact_sales',
              'agent.visible'='true',
              'agent.recommended'='false',
              'comment'='User behavior dimension aggregated from clean ecommerce events'
            )
            """,
        ),
        (
            "dim_session",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name("dim_session")} (
              session_id STRING,
              user_id BIGINT,
              session_start_at TIMESTAMP,
              session_end_at TIMESTAMP,
              session_duration_sec BIGINT,
              event_count BIGINT,
              view_count BIGINT,
              cart_count BIGINT,
              remove_from_cart_count BIGINT,
              purchase_count BIGINT,
              session_revenue DECIMAL(18,2),
              has_purchase BOOLEAN,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(session_start_at))
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='dimension',
              'gold.grain'='one row per user session',
              'gold.primary_key'='session_id',
              'gold.source'='gold.fact_events and gold.fact_sales',
              'agent.visible'='true',
              'agent.recommended'='false',
              'comment'='Session dimension with behavior counts, duration, and revenue'
            )
            """,
        ),
        (
            "daily_product_summary",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name("daily_product_summary")} (
              summary_id STRING,
              event_date DATE,
              product_id BIGINT,
              brand STRING,
              category_l1 STRING,
              category_l2 STRING,
              category_l3 STRING,
              view_count BIGINT,
              cart_count BIGINT,
              purchase_count BIGINT,
              remove_from_cart_count BIGINT,
              unique_users BIGINT,
              unique_sessions BIGINT,
              revenue DECIMAL(18,2),
              avg_price DECIMAL(10,2),
              min_price DECIMAL(10,2),
              max_price DECIMAL(10,2),
              conversion_rate DOUBLE,
              cart_to_purchase_rate DOUBLE,
              gold_processed_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (event_date)
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='summary',
              'gold.grain'='one row per event date and product',
              'gold.primary_key'='summary_id',
              'gold.source'='gold.fact_events joined with gold.dim_product',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Daily product performance summary for analytics and AI Agent queries'
            )
            """,
        ),
        (
            "daily_category_summary",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name("daily_category_summary")} (
              summary_id STRING,
              event_date DATE,
              category_l1 STRING,
              category_l2 STRING,
              category_l3 STRING,
              total_events BIGINT,
              view_count BIGINT,
              cart_count BIGINT,
              purchase_count BIGINT,
              remove_from_cart_count BIGINT,
              unique_users BIGINT,
              unique_sessions BIGINT,
              unique_products BIGINT,
              revenue DECIMAL(18,2),
              conversion_rate DOUBLE,
              cart_to_purchase_rate DOUBLE,
              gold_processed_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (event_date)
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='summary',
              'gold.grain'='one row per event date and category hierarchy',
              'gold.primary_key'='summary_id',
              'gold.source'='gold.fact_events joined with gold.dim_product',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Daily category-level ecommerce performance summary'
            )
            """,
        ),
        (
            "daily_brand_summary",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name("daily_brand_summary")} (
              summary_id STRING,
              event_date DATE,
              brand STRING,
              view_count BIGINT,
              cart_count BIGINT,
              purchase_count BIGINT,
              remove_from_cart_count BIGINT,
              unique_users BIGINT,
              unique_sessions BIGINT,
              unique_products BIGINT,
              revenue DECIMAL(18,2),
              conversion_rate DOUBLE,
              cart_to_purchase_rate DOUBLE,
              gold_processed_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (event_date)
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='summary',
              'gold.grain'='one row per event date and brand',
              'gold.primary_key'='summary_id',
              'gold.source'='gold.fact_events joined with gold.dim_product',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Daily brand-level ecommerce performance summary'
            )
            """,
        ),
    ]

    for short_name, ddl in ddl_statements:
        run_sql(spark, ddl, f"Creating table if not exists: {table_name(short_name)}")


def validate_extended_tables(spark):
    run_sql(spark, f"SHOW TABLES IN {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}", "Gold tables").show(200, truncate=False)
    for short_name in EXTENDED_TABLES:
        run_sql(spark, f"DESCRIBE TABLE {table_name(short_name)}", f"Describing {table_name(short_name)}").show(200, truncate=False)


def main():
    spark = None
    try:
        log(f"Catalog: {ICEBERG_CATALOG_NAME}")
        log(f"Namespace: {ICEBERG_NAMESPACE}")
        log(f"Warehouse: {ICEBERG_WAREHOUSE}")
        log(f"RESET_GOLD_EXTENDED_SCHEMA={'enabled' if RESET_GOLD_EXTENDED_SCHEMA else 'disabled'}")
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        create_namespace(spark)
        drop_tables_if_reset_enabled(spark)
        create_extended_tables(spark)
        validate_extended_tables(spark)
        log("SUCCESS")
    except Exception as exc:
        print(f"[GoldExtendedSchemaInitJob] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
