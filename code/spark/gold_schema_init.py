"""
Initialize Gold MVP Iceberg schema.

Stage 2 creates the Gold tables only. It does not load production data from
Silver and does not drop existing tables unless RESET_GOLD_SCHEMA=true.
"""

import os
import re
import sys
import traceback

from pyspark.sql import SparkSession


APP_NAME = "GoldSchemaInitJob"


def env(name, default):
    return os.getenv(name, default)


MINIO_ENDPOINT = env("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", "Admin123!")

ICEBERG_CATALOG_NAME = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
ICEBERG_NAMESPACE = env("ICEBERG_NAMESPACE", "gold")
ICEBERG_WAREHOUSE = env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/")
ICEBERG_JDBC_URI = env(
    "ICEBERG_JDBC_URI",
    "jdbc:postgresql://postgres-db:5432/agent4da",
)
ICEBERG_JDBC_USER = env("ICEBERG_JDBC_USER", "bigdata")
ICEBERG_JDBC_PASSWORD = env("ICEBERG_JDBC_PASSWORD", "#3Bigdata")
ICEBERG_JDBC_SCHEMA = env("ICEBERG_JDBC_SCHEMA", "iceberg")
RESET_GOLD_SCHEMA = env("RESET_GOLD_SCHEMA", "false").strip().lower() == "true"
ENABLE_GOLD_SCHEMA_TEST_DATA = (
    env("ENABLE_GOLD_SCHEMA_TEST_DATA", "false").strip().lower() == "true"
)

GOLD_TABLES = [
    "dim_time",
    "dim_product",
    "fact_events",
    "fact_sales",
    "daily_event_summary",
]

DROP_ORDER = [
    "daily_event_summary",
    "fact_sales",
    "fact_events",
    "dim_product",
    "dim_time",
]


def log(message):
    print(f"[GoldSchemaInitJob] {message}", flush=True)


def validate_identifier(identifier, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid {label}: {identifier!r}")


def full_table_name(table_name):
    validate_identifier(table_name, "table_name")
    return f"{ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}.{table_name}"


def create_spark_session():
    validate_identifier(ICEBERG_CATALOG_NAME, "ICEBERG_CATALOG_NAME")
    validate_identifier(ICEBERG_NAMESPACE, "ICEBERG_NAMESPACE")

    catalog = ICEBERG_CATALOG_NAME

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
    if not RESET_GOLD_SCHEMA:
        log("RESET_GOLD_SCHEMA is disabled. Existing tables will not be dropped.")
        return

    log("RESET_GOLD_SCHEMA is enabled. Dropping Gold MVP tables.")
    for table_name in DROP_ORDER:
        run_sql(
            spark,
            f"DROP TABLE IF EXISTS {full_table_name(table_name)}",
            f"Dropping table if exists: {full_table_name(table_name)}",
        )


def create_gold_tables(spark):
    ddl_statements = [
        (
            "dim_time",
            f"""
            CREATE TABLE IF NOT EXISTS {full_table_name("dim_time")} (
              time_id STRING,
              event_date DATE,
              event_year INT,
              event_month INT,
              event_day INT,
              event_hour INT,
              day_of_week INT,
              day_name STRING,
              month_name STRING,
              quarter INT,
              is_weekend BOOLEAN,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (event_year, event_month)
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='dimension',
              'gold.grain'='one row per event hour',
              'gold.primary_key'='time_id',
              'gold.source'='silver.ecommerce_events',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Time dimension for hourly and daily ecommerce analytics'
            )
            """,
        ),
        (
            "dim_product",
            f"""
            CREATE TABLE IF NOT EXISTS {full_table_name("dim_product")} (
              product_id BIGINT,
              category_id BIGINT,
              category_code STRING,
              category_l1 STRING,
              category_l2 STRING,
              category_l3 STRING,
              brand STRING,
              first_seen_at TIMESTAMP,
              last_seen_at TIMESTAMP,
              avg_observed_price DECIMAL(10,2),
              min_observed_price DECIMAL(10,2),
              max_observed_price DECIMAL(10,2),
              record_count BIGINT,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='dimension',
              'gold.grain'='one row per product',
              'gold.primary_key'='product_id',
              'gold.source'='silver.ecommerce_events',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Product dimension with category hierarchy, brand, and observed price statistics'
            )
            """,
        ),
        (
            "fact_events",
            f"""
            CREATE TABLE IF NOT EXISTS {full_table_name("fact_events")} (
              event_id STRING,
              source_event_id STRING,
              time_id STRING,
              event_ts TIMESTAMP,
              event_date DATE,
              event_type STRING,
              product_id BIGINT,
              user_id BIGINT,
              session_id STRING,
              price DECIMAL(10,2),
              is_view BOOLEAN,
              is_cart BOOLEAN,
              is_remove_from_cart BOOLEAN,
              is_purchase BOOLEAN,
              kafka_partition INT,
              kafka_offset BIGINT,
              silver_processed_at TIMESTAMP,
              gold_processed_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(event_ts))
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='fact',
              'gold.grain'='one row per clean ecommerce event',
              'gold.primary_key'='event_id',
              'gold.source'='silver.ecommerce_events',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Central event fact table derived from clean Silver ecommerce events'
            )
            """,
        ),
        (
            "fact_sales",
            f"""
            CREATE TABLE IF NOT EXISTS {full_table_name("fact_sales")} (
              sale_id STRING,
              source_event_id STRING,
              time_id STRING,
              sale_ts TIMESTAMP,
              sale_date DATE,
              product_id BIGINT,
              user_id BIGINT,
              session_id STRING,
              unit_price DECIMAL(10,2),
              quantity INT,
              gross_amount DECIMAL(18,2),
              gold_processed_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(sale_ts))
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'gold.layer'='gold',
              'gold.table_type'='fact',
              'gold.grain'='one row per purchase event',
              'gold.primary_key'='sale_id',
              'gold.source'='silver.ecommerce_events where event_type = purchase',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Sales fact table containing purchase events. quantity defaults to 1 because source dataset has no quantity column'
            )
            """,
        ),
        (
            "daily_event_summary",
            f"""
            CREATE TABLE IF NOT EXISTS {full_table_name("daily_event_summary")} (
              event_date DATE,
              total_events BIGINT,
              total_views BIGINT,
              total_carts BIGINT,
              total_remove_from_carts BIGINT,
              total_purchases BIGINT,
              unique_users BIGINT,
              unique_sessions BIGINT,
              unique_products BIGINT,
              total_revenue DECIMAL(18,2),
              avg_event_price DECIMAL(10,2),
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
              'gold.grain'='one row per event date',
              'gold.primary_key'='event_date',
              'gold.source'='silver.ecommerce_events aggregated by event_date',
              'agent.visible'='true',
              'agent.recommended'='true',
              'comment'='Daily ecommerce event summary for fast analytics and AI Agent queries'
            )
            """,
        ),
    ]

    for table_name, ddl in ddl_statements:
        run_sql(spark, ddl, f"Creating table if not exists: {full_table_name(table_name)}")


def insert_if_absent(spark, table_name, where_clause, insert_sql):
    target = full_table_name(table_name)
    row = spark.sql(
        f"SELECT COUNT(*) AS row_count FROM {target} WHERE {where_clause}"
    ).collect()[0]
    if row["row_count"] > 0:
        log(f"Skipping test data for {target}; test row already exists.")
        return

    run_sql(spark, insert_sql, f"Inserting test data into {target}")


def insert_test_data_if_enabled(spark):
    if not ENABLE_GOLD_SCHEMA_TEST_DATA:
        log("ENABLE_GOLD_SCHEMA_TEST_DATA is disabled. No test rows will be inserted.")
        return

    log("ENABLE_GOLD_SCHEMA_TEST_DATA is enabled. Inserting small 2020-01-01 test rows.")

    insert_if_absent(
        spark,
        "dim_time",
        "time_id = '2020010100'",
        f"""
        INSERT INTO {full_table_name("dim_time")} VALUES (
          '2020010100',
          DATE '2020-01-01',
          2020,
          1,
          1,
          0,
          4,
          'Wednesday',
          'January',
          1,
          false,
          TIMESTAMP '2020-01-01 00:00:00',
          TIMESTAMP '2020-01-01 00:00:00'
        )
        """,
    )

    insert_if_absent(
        spark,
        "dim_product",
        "product_id = 1001",
        f"""
        INSERT INTO {full_table_name("dim_product")} VALUES (
          1001,
          2001,
          'electronics.audio.headphone',
          'electronics',
          'audio',
          'headphone',
          'test_brand',
          TIMESTAMP '2020-01-01 00:00:00',
          TIMESTAMP '2020-01-01 00:00:00',
          CAST(19.99 AS DECIMAL(10,2)),
          CAST(19.99 AS DECIMAL(10,2)),
          CAST(19.99 AS DECIMAL(10,2)),
          1,
          TIMESTAMP '2020-01-01 00:00:00',
          TIMESTAMP '2020-01-01 00:00:00'
        )
        """,
    )

    insert_if_absent(
        spark,
        "fact_events",
        "event_id = 'test_event_2020010100_1'",
        f"""
        INSERT INTO {full_table_name("fact_events")} VALUES (
          'test_event_2020010100_1',
          'test_source_event_1',
          '2020010100',
          TIMESTAMP '2020-01-01 00:00:00',
          DATE '2020-01-01',
          'purchase',
          1001,
          3001,
          'test_session_1',
          CAST(19.99 AS DECIMAL(10,2)),
          false,
          false,
          false,
          true,
          0,
          1,
          TIMESTAMP '2020-01-01 00:01:00',
          TIMESTAMP '2020-01-01 00:02:00'
        )
        """,
    )

    insert_if_absent(
        spark,
        "fact_sales",
        "sale_id = 'test_sale_2020010100_1'",
        f"""
        INSERT INTO {full_table_name("fact_sales")} VALUES (
          'test_sale_2020010100_1',
          'test_source_event_1',
          '2020010100',
          TIMESTAMP '2020-01-01 00:00:00',
          DATE '2020-01-01',
          1001,
          3001,
          'test_session_1',
          CAST(19.99 AS DECIMAL(10,2)),
          1,
          CAST(19.99 AS DECIMAL(18,2)),
          TIMESTAMP '2020-01-01 00:02:00'
        )
        """,
    )

    insert_if_absent(
        spark,
        "daily_event_summary",
        "event_date = DATE '2020-01-01'",
        f"""
        INSERT INTO {full_table_name("daily_event_summary")} VALUES (
          DATE '2020-01-01',
          1,
          0,
          0,
          0,
          1,
          1,
          1,
          1,
          CAST(19.99 AS DECIMAL(18,2)),
          CAST(19.99 AS DECIMAL(10,2)),
          CAST(1.0 AS DOUBLE),
          CAST(1.0 AS DOUBLE),
          TIMESTAMP '2020-01-01 00:02:00'
        )
        """,
    )


def validate_gold_tables(spark):
    run_sql(
        spark,
        f"SHOW NAMESPACES IN {ICEBERG_CATALOG_NAME}",
        f"Namespaces in {ICEBERG_CATALOG_NAME}",
    ).show(200, truncate=False)

    run_sql(
        spark,
        f"SHOW TABLES IN {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}",
        f"Tables in {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}",
    ).show(200, truncate=False)

    for table_name in GOLD_TABLES:
        run_sql(
            spark,
            f"DESCRIBE TABLE {full_table_name(table_name)}",
            f"Describing table: {full_table_name(table_name)}",
        ).show(200, truncate=False)


def main():
    spark = None
    try:
        log(f"Catalog: {ICEBERG_CATALOG_NAME}")
        log(f"Namespace: {ICEBERG_NAMESPACE}")
        log(f"Warehouse: {ICEBERG_WAREHOUSE}")
        log(f"JDBC URI: {ICEBERG_JDBC_URI}")
        log(f"JDBC schema: {ICEBERG_JDBC_SCHEMA}")
        log(f"MinIO endpoint: {MINIO_ENDPOINT}")
        log(f"RESET_GOLD_SCHEMA={'enabled' if RESET_GOLD_SCHEMA else 'disabled'}")
        log(
            "ENABLE_GOLD_SCHEMA_TEST_DATA="
            f"{'enabled' if ENABLE_GOLD_SCHEMA_TEST_DATA else 'disabled'}"
        )

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        create_namespace(spark)
        drop_tables_if_reset_enabled(spark)
        create_gold_tables(spark)
        insert_test_data_if_enabled(spark)
        validate_gold_tables(spark)
        log("SUCCESS")
    except Exception as exc:
        print(
            f"[GoldSchemaInitJob] FAILED: {type(exc).__name__}: {exc}",
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
