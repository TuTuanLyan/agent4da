"""
Consolidated Gold Layer job for Agent4DA.

This job owns the full Gold workflow:
- Ensure Iceberg namespaces.
- Create Gold and semantic metadata tables.
- Read clean Silver events.
- Build MVP and extended Gold analytics tables.
- Build semantic metadata tables for the Agent.
- Write everything through the Iceberg catalog.
"""

import os
import re
import sys
import traceback
from dataclasses import dataclass

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    date_format,
    dayofweek,
    first,
    lit,
    max as spark_max,
    min as spark_min,
    quarter,
    sha2,
    sum as spark_sum,
    unix_timestamp,
    when,
)


APP_NAME = "GoldJob"
ALLOWED_RUN_MODES = {
    "all",
    "schema_only",
    "mvp_only",
    "extended_only",
    "metadata_only",
    "validate_only",
}
ALLOWED_REFRESH_MODES = {"full_refresh", "append"}

GOLD_TABLES = [
    "dim_time",
    "dim_product",
    "fact_events",
    "fact_sales",
    "daily_event_summary",
    "dim_user",
    "dim_session",
    "daily_product_summary",
    "daily_category_summary",
    "daily_brand_summary",
]

MVP_TABLES = [
    "dim_time",
    "dim_product",
    "fact_events",
    "fact_sales",
    "daily_event_summary",
]

EXTENDED_TABLES = [
    "dim_user",
    "dim_session",
    "daily_product_summary",
    "daily_category_summary",
    "daily_brand_summary",
]

METADATA_TABLES = [
    "table_catalog",
    "column_catalog",
    "metric_catalog",
    "join_catalog",
]

_ACTIVE_CATALOG_NAME = None


@dataclass(frozen=True)
class Config:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    catalog_name: str
    gold_namespace: str
    metadata_namespace: str
    warehouse: str
    jdbc_uri: str
    jdbc_user: str
    jdbc_password: str
    jdbc_schema: str
    silver_events_path: str
    run_mode: str
    refresh_mode: str
    dry_run: bool
    validate_tables: bool


def env(name, default):
    return os.getenv(name, default)


def bool_env(name, default):
    return env(name, default).strip().lower() == "true"


def log(message):
    print(f"[GoldJob] {message}", flush=True)


def validate_identifier(identifier, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid {label}: {identifier!r}")


def get_config():
    global _ACTIVE_CATALOG_NAME

    catalog_name = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    gold_namespace = env("GOLD_NAMESPACE", "gold")
    metadata_namespace = env("METADATA_NAMESPACE", "metadata")
    run_mode = env("GOLD_RUN_MODE", "all").strip().lower()
    refresh_mode = env("GOLD_REFRESH_MODE", "full_refresh").strip().lower()

    validate_identifier(catalog_name, "ICEBERG_CATALOG_NAME")
    validate_identifier(gold_namespace, "GOLD_NAMESPACE")
    validate_identifier(metadata_namespace, "METADATA_NAMESPACE")

    if run_mode not in ALLOWED_RUN_MODES:
        raise ValueError(
            f"Invalid GOLD_RUN_MODE={run_mode!r}. "
            f"Allowed: {', '.join(sorted(ALLOWED_RUN_MODES))}"
        )
    if refresh_mode not in ALLOWED_REFRESH_MODES:
        raise ValueError(
            f"Invalid GOLD_REFRESH_MODE={refresh_mode!r}. "
            f"Allowed: {', '.join(sorted(ALLOWED_REFRESH_MODES))}"
        )

    _ACTIVE_CATALOG_NAME = catalog_name

    return Config(
        minio_endpoint=env("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=env("MINIO_ACCESS_KEY", "admin"),
        minio_secret_key=env("MINIO_SECRET_KEY", "Admin123!"),
        catalog_name=catalog_name,
        gold_namespace=gold_namespace,
        metadata_namespace=metadata_namespace,
        warehouse=env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/"),
        jdbc_uri=env("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres-db:5432/agent4da"),
        jdbc_user=env("ICEBERG_JDBC_USER", "bigdata"),
        jdbc_password=env("ICEBERG_JDBC_PASSWORD", "#3Bigdata"),
        jdbc_schema=env("ICEBERG_JDBC_SCHEMA", "iceberg"),
        silver_events_path=env("SILVER_EVENTS_PATH", "s3a://silver/ecommerce_events/"),
        run_mode=run_mode,
        refresh_mode=refresh_mode,
        dry_run=bool_env("GOLD_DRY_RUN", "false"),
        validate_tables=bool_env("GOLD_VALIDATE_TABLES", "true"),
    )


def table_name(namespace, short_name):
    catalog_name = _ACTIVE_CATALOG_NAME or env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    validate_identifier(catalog_name, "ICEBERG_CATALOG_NAME")
    validate_identifier(namespace, "namespace")
    validate_identifier(short_name, "table_name")
    return f"{catalog_name}.{namespace}.{short_name}"


def run_sql(spark, sql_text, description=None):
    if description:
        log(description)
    return spark.sql(sql_text)


def create_spark_session():
    config = get_config()
    catalog = config.catalog_name

    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
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
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"spark.sql.catalog.{catalog}.uri", config.jdbc_uri)
        .config(f"spark.sql.catalog.{catalog}.jdbc.user", config.jdbc_user)
        .config(f"spark.sql.catalog.{catalog}.jdbc.password", config.jdbc_password)
        .config(f"spark.sql.catalog.{catalog}.jdbc.currentSchema", config.jdbc_schema)
        .config(f"spark.sql.catalog.{catalog}.warehouse", config.warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def ensure_namespaces(spark, config):
    run_sql(
        spark,
        f"CREATE NAMESPACE IF NOT EXISTS {config.catalog_name}.{config.gold_namespace}",
        f"Ensuring namespace {config.catalog_name}.{config.gold_namespace}",
    )
    run_sql(
        spark,
        f"CREATE NAMESPACE IF NOT EXISTS {config.catalog_name}.{config.metadata_namespace}",
        f"Ensuring namespace {config.catalog_name}.{config.metadata_namespace}",
    )


def create_gold_tables(spark, config):
    ddl_statements = [
        (
            "dim_time",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "dim_time")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "dim_product")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "fact_events")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "fact_sales")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "daily_event_summary")} (
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
        (
            "dim_user",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "dim_user")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "dim_session")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "daily_product_summary")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "daily_category_summary")} (
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
            CREATE TABLE IF NOT EXISTS {table_name(config.gold_namespace, "daily_brand_summary")} (
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
        run_sql(
            spark,
            ddl,
            f"Creating Gold table if not exists: {table_name(config.gold_namespace, short_name)}",
        )


def create_metadata_tables(spark, config):
    ddl_statements = [
        (
            "table_catalog",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name(config.metadata_namespace, "table_catalog")} (
              table_name STRING,
              layer STRING,
              table_type STRING,
              business_name STRING,
              description STRING,
              grain STRING,
              primary_key STRING,
              storage_format STRING,
              query_engine STRING,
              is_agent_visible BOOLEAN,
              recommended_for_agent BOOLEAN,
              refresh_frequency STRING,
              owner STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'metadata.layer'='metadata',
              'agent.visible'='true',
              'comment'='Semantic table catalog for Agent4DA'
            )
            """,
        ),
        (
            "column_catalog",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name(config.metadata_namespace, "column_catalog")} (
              column_id STRING,
              table_name STRING,
              column_name STRING,
              data_type STRING,
              business_name STRING,
              description STRING,
              source_table STRING,
              source_column STRING,
              transformation_logic STRING,
              is_nullable BOOLEAN,
              is_dimension BOOLEAN,
              is_metric BOOLEAN,
              is_time_column BOOLEAN,
              is_join_key BOOLEAN,
              example_values STRING,
              allowed_values STRING,
              agent_synonyms STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'metadata.layer'='metadata',
              'agent.visible'='true',
              'comment'='Semantic column catalog for Agent4DA'
            )
            """,
        ),
        (
            "metric_catalog",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name(config.metadata_namespace, "metric_catalog")} (
              metric_name STRING,
              business_name STRING,
              description STRING,
              formula_sql STRING,
              base_table STRING,
              default_time_column STRING,
              aggregation_type STRING,
              unit STRING,
              example_question STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'metadata.layer'='metadata',
              'agent.visible'='true',
              'comment'='Semantic metric catalog for Agent4DA'
            )
            """,
        ),
        (
            "join_catalog",
            f"""
            CREATE TABLE IF NOT EXISTS {table_name(config.metadata_namespace, "join_catalog")} (
              join_id STRING,
              left_table STRING,
              left_key STRING,
              right_table STRING,
              right_key STRING,
              relationship_type STRING,
              description STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
              'format-version'='2',
              'write.parquet.compression-codec'='snappy',
              'metadata.layer'='metadata',
              'agent.visible'='true',
              'comment'='Semantic join catalog for Agent4DA'
            )
            """,
        ),
    ]

    for short_name, ddl in ddl_statements:
        run_sql(
            spark,
            ddl,
            f"Creating metadata table if not exists: {table_name(config.metadata_namespace, short_name)}",
        )


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def read_silver_events(spark, config):
    log(f"Input Silver path: {config.silver_events_path}")
    if not path_exists(spark, config.silver_events_path):
        raise FileNotFoundError(f"Silver events path does not exist: {config.silver_events_path}")

    try:
        silver_df = spark.read.parquet(config.silver_events_path)
    except AnalysisException as exc:
        raise RuntimeError(f"Cannot read Silver parquet at {config.silver_events_path}: {exc}") from exc

    if not silver_df.columns:
        raise RuntimeError(f"Silver parquet has no columns: {config.silver_events_path}")

    return silver_df


def build_base_events_df(silver_df):
    total_rows = silver_df.count()
    valid_df = silver_df.where(col("is_valid") == lit(True))
    valid_rows = valid_df.count()

    casted_df = valid_df.select(
        col("source_event_id").cast("string").alias("source_event_id"),
        col("event_ts").cast("timestamp").alias("event_ts"),
        col("event_date").cast("date").alias("event_date"),
        col("event_year").cast("int").alias("event_year"),
        col("event_month").cast("int").alias("event_month"),
        col("event_day").cast("int").alias("event_day"),
        col("event_hour").cast("int").alias("event_hour"),
        col("event_type").cast("string").alias("event_type"),
        col("product_id").cast("long").alias("product_id"),
        col("category_id").cast("long").alias("category_id"),
        col("category_code").cast("string").alias("category_code"),
        col("category_l1").cast("string").alias("category_l1"),
        col("category_l2").cast("string").alias("category_l2"),
        col("category_l3").cast("string").alias("category_l3"),
        col("brand").cast("string").alias("brand"),
        col("price").cast("decimal(10,2)").alias("price"),
        col("user_id").cast("long").alias("user_id"),
        col("user_session").cast("string").alias("user_session"),
        col("kafka_partition").cast("int").alias("kafka_partition"),
        col("kafka_offset").cast("long").alias("kafka_offset"),
        col("silver_processed_at").cast("timestamp").alias("silver_processed_at"),
    )

    dedup_df = casted_df.dropDuplicates(["source_event_id"])
    dedup_rows = dedup_df.count()

    required_df = (
        dedup_df
        .where(col("source_event_id").isNotNull())
        .where(col("event_ts").isNotNull())
        .where(col("event_date").isNotNull())
        .where(col("event_type").isNotNull())
        .where(col("product_id").isNotNull())
        .where(col("user_id").isNotNull())
        .where(col("user_session").isNotNull())
        .withColumn("time_id", date_format(col("event_ts"), "yyyyMMddHH"))
        .withColumn("gold_processed_at", current_timestamp())
    )

    base_df = required_df.cache()
    base_rows = base_df.count()

    log(f"Total Silver rows: {total_rows}")
    log(f"Valid Silver rows: {valid_rows}")
    log(f"Rows after source_event_id dedup: {dedup_rows}")
    log(f"Rows after base required filters: {base_rows}")

    if total_rows == 0:
        raise RuntimeError("Silver path is readable but empty.")
    if valid_rows == 0:
        raise RuntimeError("Silver data has no is_valid=true records.")
    if base_rows == 0:
        raise RuntimeError("No Gold-eligible records remain after required filters.")

    return base_df


def build_fact_events_df(base_df):
    return base_df.select(
        col("source_event_id").alias("event_id"),
        col("source_event_id"),
        col("time_id"),
        col("event_ts"),
        col("event_date"),
        col("event_type"),
        col("product_id"),
        col("user_id"),
        col("user_session").alias("session_id"),
        col("price"),
        (col("event_type") == lit("view")).alias("is_view"),
        (col("event_type") == lit("cart")).alias("is_cart"),
        (col("event_type") == lit("remove_from_cart")).alias("is_remove_from_cart"),
        (col("event_type") == lit("purchase")).alias("is_purchase"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("silver_processed_at"),
        col("gold_processed_at"),
    )


def build_fact_sales_df(base_df):
    return (
        base_df
        .where(col("event_type") == lit("purchase"))
        .select(
            col("source_event_id").alias("sale_id"),
            col("source_event_id"),
            col("time_id"),
            col("event_ts").alias("sale_ts"),
            col("event_date").alias("sale_date"),
            col("product_id"),
            col("user_id"),
            col("user_session").alias("session_id"),
            col("price").alias("unit_price"),
            lit(1).cast("int").alias("quantity"),
            col("price").cast("decimal(18,2)").alias("gross_amount"),
            col("gold_processed_at"),
        )
    )


def build_dim_time_df(base_df):
    return (
        base_df
        .select("time_id", "event_date", "event_year", "event_month", "event_day", "event_hour", "event_ts")
        .dropDuplicates(["time_id"])
        .select(
            col("time_id"),
            col("event_date"),
            col("event_year"),
            col("event_month"),
            col("event_day"),
            col("event_hour"),
            dayofweek(col("event_ts")).alias("day_of_week"),
            date_format(col("event_ts"), "EEEE").alias("day_name"),
            date_format(col("event_ts"), "MMMM").alias("month_name"),
            quarter(col("event_ts")).alias("quarter"),
            dayofweek(col("event_ts")).isin(1, 7).alias("is_weekend"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def build_dim_product_df(base_df):
    return (
        base_df
        .groupBy("product_id")
        .agg(
            first("category_id", ignorenulls=True).alias("category_id"),
            first("category_code", ignorenulls=True).alias("category_code"),
            first("category_l1", ignorenulls=True).alias("category_l1"),
            first("category_l2", ignorenulls=True).alias("category_l2"),
            first("category_l3", ignorenulls=True).alias("category_l3"),
            first("brand", ignorenulls=True).alias("brand"),
            spark_min("event_ts").alias("first_seen_at"),
            spark_max("event_ts").alias("last_seen_at"),
            avg("price").cast("decimal(10,2)").alias("avg_observed_price"),
            spark_min("price").cast("decimal(10,2)").alias("min_observed_price"),
            spark_max("price").cast("decimal(10,2)").alias("max_observed_price"),
            count(lit(1)).cast("long").alias("record_count"),
        )
        .select(
            "product_id",
            "category_id",
            "category_code",
            "category_l1",
            "category_l2",
            "category_l3",
            "brand",
            "first_seen_at",
            "last_seen_at",
            "avg_observed_price",
            "min_observed_price",
            "max_observed_price",
            "record_count",
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def safe_divide(numerator_col, denominator_col):
    return when(denominator_col == lit(0), lit(0.0)).otherwise(
        numerator_col.cast("double") / denominator_col.cast("double")
    )


def bool_count(column_name):
    return spark_sum(when(col(column_name), 1).otherwise(0)).cast("long")


def build_daily_event_summary_df(base_df):
    purchase_amount = when(
        col("event_type") == lit("purchase"),
        col("price").cast("decimal(18,2)"),
    ).otherwise(lit(0).cast("decimal(18,2)"))

    summary_df = (
        base_df
        .groupBy("event_date")
        .agg(
            count(lit(1)).cast("long").alias("total_events"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).cast("long").alias("total_views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).cast("long").alias("total_carts"),
            spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).cast("long").alias("total_remove_from_carts"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).cast("long").alias("total_purchases"),
            countDistinct("user_id").cast("long").alias("unique_users"),
            countDistinct("user_session").cast("long").alias("unique_sessions"),
            countDistinct("product_id").cast("long").alias("unique_products"),
            spark_sum(purchase_amount).cast("decimal(18,2)").alias("total_revenue"),
            avg("price").cast("decimal(10,2)").alias("avg_event_price"),
        )
    )

    return (
        summary_df
        .withColumn("conversion_rate", safe_divide(col("total_purchases"), col("total_views")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("total_purchases"), col("total_carts")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "event_date",
            "total_events",
            "total_views",
            "total_carts",
            "total_remove_from_carts",
            "total_purchases",
            "unique_users",
            "unique_sessions",
            "unique_products",
            "total_revenue",
            "avg_event_price",
            "conversion_rate",
            "cart_to_purchase_rate",
            "gold_processed_at",
        )
    )


def sales_by_user(fact_sales_df):
    return fact_sales_df.groupBy("user_id").agg(
        spark_sum("gross_amount").cast("decimal(18,2)").alias("total_revenue")
    )


def sales_by_session(fact_sales_df):
    return fact_sales_df.groupBy("session_id").agg(
        spark_sum("gross_amount").cast("decimal(18,2)").alias("session_revenue")
    )


def build_dim_user_df(fact_events_df, fact_sales_df):
    event_agg = fact_events_df.groupBy("user_id").agg(
        spark_min("event_ts").alias("first_seen_at"),
        spark_max("event_ts").alias("last_seen_at"),
        countDistinct("session_id").cast("long").alias("total_sessions"),
        count(lit(1)).cast("long").alias("total_events"),
        bool_count("is_view").alias("total_views"),
        bool_count("is_cart").alias("total_cart_adds"),
        bool_count("is_remove_from_cart").alias("total_remove_from_carts"),
        bool_count("is_purchase").alias("total_purchases"),
    )

    return (
        event_agg
        .join(sales_by_user(fact_sales_df), on="user_id", how="left")
        .select(
            "user_id",
            "first_seen_at",
            "last_seen_at",
            "total_sessions",
            "total_events",
            "total_views",
            "total_cart_adds",
            "total_remove_from_carts",
            "total_purchases",
            coalesce(col("total_revenue"), lit(0).cast("decimal(18,2)")).alias("total_revenue"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def build_dim_session_df(fact_events_df, fact_sales_df):
    event_agg = fact_events_df.groupBy("session_id").agg(
        first("user_id", ignorenulls=True).alias("user_id"),
        spark_min("event_ts").alias("session_start_at"),
        spark_max("event_ts").alias("session_end_at"),
        count(lit(1)).cast("long").alias("event_count"),
        bool_count("is_view").alias("view_count"),
        bool_count("is_cart").alias("cart_count"),
        bool_count("is_remove_from_cart").alias("remove_from_cart_count"),
        bool_count("is_purchase").alias("purchase_count"),
    )

    return (
        event_agg
        .join(sales_by_session(fact_sales_df), on="session_id", how="left")
        .withColumn(
            "session_duration_sec",
            (unix_timestamp("session_end_at") - unix_timestamp("session_start_at")).cast("long"),
        )
        .select(
            "session_id",
            "user_id",
            "session_start_at",
            "session_end_at",
            "session_duration_sec",
            "event_count",
            "view_count",
            "cart_count",
            "remove_from_cart_count",
            "purchase_count",
            coalesce(col("session_revenue"), lit(0).cast("decimal(18,2)")).alias("session_revenue"),
            (col("purchase_count") > lit(0)).alias("has_purchase"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def product_enriched_events(fact_events_df, dim_product_df):
    product_lookup = (
        dim_product_df
        .groupBy("product_id")
        .agg(
            first("brand", ignorenulls=True).alias("brand"),
            first("category_l1", ignorenulls=True).alias("category_l1"),
            first("category_l2", ignorenulls=True).alias("category_l2"),
            first("category_l3", ignorenulls=True).alias("category_l3"),
        )
    )
    return fact_events_df.join(product_lookup, on="product_id", how="left")


def revenue_expr():
    return when(col("is_purchase"), col("price").cast("decimal(18,2)")).otherwise(
        lit(0).cast("decimal(18,2)")
    )


def build_daily_product_summary_df(fact_events_df, dim_product_df):
    grouped = product_enriched_events(fact_events_df, dim_product_df).groupBy(
        "event_date",
        "product_id",
        "brand",
        "category_l1",
        "category_l2",
        "category_l3",
    ).agg(
        bool_count("is_view").alias("view_count"),
        bool_count("is_cart").alias("cart_count"),
        bool_count("is_purchase").alias("purchase_count"),
        bool_count("is_remove_from_cart").alias("remove_from_cart_count"),
        countDistinct("user_id").cast("long").alias("unique_users"),
        countDistinct("session_id").cast("long").alias("unique_sessions"),
        spark_sum(revenue_expr()).cast("decimal(18,2)").alias("revenue"),
        avg("price").cast("decimal(10,2)").alias("avg_price"),
        spark_min("price").cast("decimal(10,2)").alias("min_price"),
        spark_max("price").cast("decimal(10,2)").alias("max_price"),
    )

    return (
        grouped
        .withColumn("summary_id", concat_ws("_", date_format("event_date", "yyyyMMdd"), col("product_id").cast("string")))
        .withColumn("conversion_rate", safe_divide(col("purchase_count"), col("view_count")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("purchase_count"), col("cart_count")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "summary_id",
            "event_date",
            "product_id",
            "brand",
            "category_l1",
            "category_l2",
            "category_l3",
            "view_count",
            "cart_count",
            "purchase_count",
            "remove_from_cart_count",
            "unique_users",
            "unique_sessions",
            "revenue",
            "avg_price",
            "min_price",
            "max_price",
            "conversion_rate",
            "cart_to_purchase_rate",
            "gold_processed_at",
        )
    )


def build_daily_category_summary_df(fact_events_df, dim_product_df):
    joined = (
        product_enriched_events(fact_events_df, dim_product_df)
        .withColumn("category_l1", coalesce(col("category_l1"), lit("unknown")))
        .withColumn("category_l2", coalesce(col("category_l2"), lit("unknown")))
        .withColumn("category_l3", coalesce(col("category_l3"), lit("unknown")))
    )

    grouped = joined.groupBy("event_date", "category_l1", "category_l2", "category_l3").agg(
        count(lit(1)).cast("long").alias("total_events"),
        bool_count("is_view").alias("view_count"),
        bool_count("is_cart").alias("cart_count"),
        bool_count("is_purchase").alias("purchase_count"),
        bool_count("is_remove_from_cart").alias("remove_from_cart_count"),
        countDistinct("user_id").cast("long").alias("unique_users"),
        countDistinct("session_id").cast("long").alias("unique_sessions"),
        countDistinct("product_id").cast("long").alias("unique_products"),
        spark_sum(revenue_expr()).cast("decimal(18,2)").alias("revenue"),
    )

    return (
        grouped
        .withColumn(
            "summary_id",
            sha2(concat_ws("||", col("event_date").cast("string"), "category_l1", "category_l2", "category_l3"), 256),
        )
        .withColumn("conversion_rate", safe_divide(col("purchase_count"), col("view_count")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("purchase_count"), col("cart_count")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "summary_id",
            "event_date",
            "category_l1",
            "category_l2",
            "category_l3",
            "total_events",
            "view_count",
            "cart_count",
            "purchase_count",
            "remove_from_cart_count",
            "unique_users",
            "unique_sessions",
            "unique_products",
            "revenue",
            "conversion_rate",
            "cart_to_purchase_rate",
            "gold_processed_at",
        )
    )


def build_daily_brand_summary_df(fact_events_df, dim_product_df):
    joined = product_enriched_events(fact_events_df, dim_product_df).withColumn(
        "brand", coalesce(col("brand"), lit("unknown"))
    )

    grouped = joined.groupBy("event_date", "brand").agg(
        bool_count("is_view").alias("view_count"),
        bool_count("is_cart").alias("cart_count"),
        bool_count("is_purchase").alias("purchase_count"),
        bool_count("is_remove_from_cart").alias("remove_from_cart_count"),
        countDistinct("user_id").cast("long").alias("unique_users"),
        countDistinct("session_id").cast("long").alias("unique_sessions"),
        countDistinct("product_id").cast("long").alias("unique_products"),
        spark_sum(revenue_expr()).cast("decimal(18,2)").alias("revenue"),
    )

    return (
        grouped
        .withColumn("summary_id", sha2(concat_ws("||", col("event_date").cast("string"), "brand"), 256))
        .withColumn("conversion_rate", safe_divide(col("purchase_count"), col("view_count")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("purchase_count"), col("cart_count")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "summary_id",
            "event_date",
            "brand",
            "view_count",
            "cart_count",
            "purchase_count",
            "remove_from_cart_count",
            "unique_users",
            "unique_sessions",
            "unique_products",
            "revenue",
            "conversion_rate",
            "cart_to_purchase_rate",
            "gold_processed_at",
        )
    )


def with_audit_timestamps(df):
    return df.withColumn("created_at", current_timestamp()).withColumn("updated_at", current_timestamp())


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    return "'" + str(value).replace("'", "''") + "'"


def metadata_df_from_rows(spark, columns, rows):
    values_sql = ",\n".join(
        "(" + ", ".join(sql_literal(value) for value in row) + ")"
        for row in rows
    )
    select_columns = ", ".join(f"`{column}`" for column in columns)
    alias_columns = ", ".join(f"`{column}`" for column in columns)
    return spark.sql(
        f"""
        SELECT
          {select_columns},
          current_timestamp() AS created_at,
          current_timestamp() AS updated_at
        FROM VALUES
          {values_sql}
        AS metadata_rows({alias_columns})
        """
    )


def build_metadata_table_catalog_df(spark, config):
    rows = [
        ("gold.dim_time", "gold", "dimension", "Time", "Hourly time dimension for ecommerce events.", "one row per event hour", "time_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.dim_product", "gold", "dimension", "Product", "Product category, brand, and observed price statistics.", "one row per product", "product_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.fact_events", "gold", "fact", "Events", "Clean ecommerce event fact table.", "one row per clean ecommerce event", "event_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.fact_sales", "gold", "fact", "Sales", "Purchase event fact table.", "one row per purchase event", "sale_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.daily_event_summary", "gold", "summary", "Daily Event Summary", "Daily funnel and revenue summary.", "one row per event date", "event_date", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.dim_user", "gold", "dimension", "User", "User behavior dimension.", "one row per user", "user_id", "Iceberg", "Spark", True, False, "per Gold run", "agent4da"),
        ("gold.dim_session", "gold", "dimension", "Session", "Session behavior dimension.", "one row per user session", "session_id", "Iceberg", "Spark", True, False, "per Gold run", "agent4da"),
        ("gold.daily_product_summary", "gold", "summary", "Daily Product Summary", "Daily product performance summary.", "one row per event date and product", "summary_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.daily_category_summary", "gold", "summary", "Daily Category Summary", "Daily category hierarchy performance summary.", "one row per event date and category hierarchy", "summary_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.daily_brand_summary", "gold", "summary", "Daily Brand Summary", "Daily brand performance summary.", "one row per event date and brand", "summary_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.table_catalog", "metadata", "semantic_catalog", "Table Catalog", "Agent-facing catalog of tables.", "one row per table", "table_name", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.column_catalog", "metadata", "semantic_catalog", "Column Catalog", "Agent-facing catalog of important columns.", "one row per important column", "column_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.metric_catalog", "metadata", "semantic_catalog", "Metric Catalog", "Agent-facing catalog of metrics and formulas.", "one row per metric", "metric_name", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.join_catalog", "metadata", "semantic_catalog", "Join Catalog", "Agent-facing catalog of supported joins.", "one row per join relationship", "join_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
    ]
    columns = [
        "table_name",
        "layer",
        "table_type",
        "business_name",
        "description",
        "grain",
        "primary_key",
        "storage_format",
        "query_engine",
        "is_agent_visible",
        "recommended_for_agent",
        "refresh_frequency",
        "owner",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def column_row(table, column_name, data_type, business_name, description, source_table, source_column, logic, nullable, is_dimension, is_metric, is_time_column, is_join_key, examples, allowed, synonyms):
    return (
        f"{table}.{column_name}",
        table,
        column_name,
        data_type,
        business_name,
        description,
        source_table,
        source_column,
        logic,
        nullable,
        is_dimension,
        is_metric,
        is_time_column,
        is_join_key,
        examples,
        allowed,
        synonyms,
    )


def build_metadata_column_catalog_df(spark, config):
    rows = [
        column_row("gold.fact_events", "event_date", "DATE", "Event Date", "Calendar date of the event.", "silver.ecommerce_events", "event_date", "cast to date", False, True, False, True, False, "2019-10-01", "", "date,event day,ngay su kien"),
        column_row("gold.fact_events", "event_type", "STRING", "Event Type", "Type of ecommerce interaction.", "silver.ecommerce_events", "event_type", "clean valid event type", False, True, False, False, False, "view,cart,purchase", "view,cart,remove_from_cart,purchase", "event,action,hanh vi"),
        column_row("gold.fact_events", "product_id", "BIGINT", "Product ID", "Product identifier.", "silver.ecommerce_events", "product_id", "cast to bigint", False, True, False, False, True, "1005003", "", "product,san pham"),
        column_row("gold.fact_events", "user_id", "BIGINT", "User ID", "User identifier.", "silver.ecommerce_events", "user_id", "cast to bigint", False, True, False, False, True, "5128042", "", "user,customer,khach hang"),
        column_row("gold.fact_events", "session_id", "STRING", "Session ID", "User session identifier.", "silver.ecommerce_events", "user_session", "rename user_session to session_id", False, True, False, False, True, "abc-session", "", "session,phien"),
        column_row("gold.fact_events", "time_id", "STRING", "Time ID", "Hourly time key.", "silver.ecommerce_events", "event_ts", "date_format(event_ts, yyyyMMddHH)", False, True, False, True, True, "2019100100", "", "hour,time,gio"),
        column_row("gold.fact_sales", "gross_amount", "DECIMAL(18,2)", "Gross Amount", "Purchase revenue amount.", "silver.ecommerce_events", "price", "price for purchase events", True, False, True, False, False, "129.99", "", "revenue,sales,doanh thu"),
        column_row("gold.fact_sales", "sale_date", "DATE", "Sale Date", "Calendar date of purchase.", "silver.ecommerce_events", "event_date", "event_date where event_type purchase", False, True, False, True, False, "2019-10-01", "", "sale date,ngay ban"),
        column_row("gold.dim_product", "brand", "STRING", "Brand", "Observed product brand.", "silver.ecommerce_events", "brand", "first non-null brand by product", True, True, False, False, False, "samsung", "", "brand,thuong hieu"),
        column_row("gold.dim_product", "category_l1", "STRING", "Category Level 1", "Top-level product category.", "silver.ecommerce_events", "category_l1", "first non-null category_l1 by product", True, True, False, False, False, "electronics", "", "category,danh muc"),
        column_row("gold.dim_product", "category_l2", "STRING", "Category Level 2", "Second-level product category.", "silver.ecommerce_events", "category_l2", "first non-null category_l2 by product", True, True, False, False, False, "smartphone", "", "subcategory,danh muc cap 2"),
        column_row("gold.dim_product", "category_l3", "STRING", "Category Level 3", "Third-level product category.", "silver.ecommerce_events", "category_l3", "first non-null category_l3 by product", True, True, False, False, False, "android", "", "category leaf,danh muc cap 3"),
        column_row("gold.daily_event_summary", "total_revenue", "DECIMAL(18,2)", "Total Revenue", "Daily revenue from purchase events.", "gold.fact_events", "price", "sum purchase price by event_date", True, False, True, False, False, "1000.00", "", "doanh thu,revenue,sales"),
        column_row("gold.daily_event_summary", "total_views", "BIGINT", "Total Views", "Daily count of view events.", "gold.fact_events", "is_view", "sum view flag", False, False, True, False, False, "5000", "", "views,luot xem"),
        column_row("gold.daily_event_summary", "total_carts", "BIGINT", "Total Carts", "Daily count of cart events.", "gold.fact_events", "is_cart", "sum cart flag", False, False, True, False, False, "250", "", "cart,add to cart,gio hang"),
        column_row("gold.daily_event_summary", "total_purchases", "BIGINT", "Total Purchases", "Daily count of purchase events.", "gold.fact_events", "is_purchase", "sum purchase flag", False, False, True, False, False, "80", "", "purchases,orders,don hang"),
        column_row("gold.daily_event_summary", "conversion_rate", "DOUBLE", "Conversion Rate", "Purchases divided by views.", "gold.daily_event_summary", "total_purchases,total_views", "total_purchases / nullif(total_views, 0)", True, False, True, False, False, "0.04", "", "conversion,ty le chuyen doi"),
        column_row("gold.daily_event_summary", "cart_to_purchase_rate", "DOUBLE", "Cart To Purchase Rate", "Purchases divided by cart events.", "gold.daily_event_summary", "total_purchases,total_carts", "total_purchases / nullif(total_carts, 0)", True, False, True, False, False, "0.25", "", "cart conversion,checkout rate"),
        column_row("gold.daily_product_summary", "revenue", "DECIMAL(18,2)", "Product Revenue", "Daily revenue by product.", "gold.fact_events", "price", "sum purchase price by product and date", True, False, True, False, False, "120.00", "", "product revenue,doanh thu san pham"),
        column_row("gold.daily_product_summary", "view_count", "BIGINT", "Product Views", "Daily views by product.", "gold.fact_events", "is_view", "sum view flag by product", True, False, True, False, False, "42", "", "product views"),
        column_row("gold.daily_product_summary", "cart_count", "BIGINT", "Product Carts", "Daily carts by product.", "gold.fact_events", "is_cart", "sum cart flag by product", True, False, True, False, False, "5", "", "product carts"),
        column_row("gold.daily_product_summary", "purchase_count", "BIGINT", "Product Purchases", "Daily purchases by product.", "gold.fact_events", "is_purchase", "sum purchase flag by product", True, False, True, False, False, "2", "", "product purchases"),
        column_row("gold.daily_product_summary", "conversion_rate", "DOUBLE", "Product Conversion Rate", "Product purchases divided by product views.", "gold.daily_product_summary", "purchase_count,view_count", "purchase_count / nullif(view_count, 0)", True, False, True, False, False, "0.08", "", "product conversion"),
        column_row("gold.daily_product_summary", "cart_to_purchase_rate", "DOUBLE", "Product Cart To Purchase Rate", "Product purchases divided by product carts.", "gold.daily_product_summary", "purchase_count,cart_count", "purchase_count / nullif(cart_count, 0)", True, False, True, False, False, "0.40", "", "product cart conversion"),
        column_row("gold.daily_category_summary", "revenue", "DECIMAL(18,2)", "Category Revenue", "Daily revenue by category hierarchy.", "gold.fact_events", "price", "sum purchase price by category and date", True, False, True, False, False, "800.00", "", "category revenue,doanh thu danh muc"),
        column_row("gold.daily_brand_summary", "revenue", "DECIMAL(18,2)", "Brand Revenue", "Daily revenue by brand.", "gold.fact_events", "price", "sum purchase price by brand and date", True, False, True, False, False, "500.00", "", "brand revenue,doanh thu thuong hieu"),
        column_row("gold.dim_user", "total_revenue", "DECIMAL(18,2)", "User Revenue", "Total user revenue.", "gold.fact_sales", "gross_amount", "sum gross_amount by user", True, False, True, False, False, "250.00", "", "customer revenue,doanh thu nguoi dung"),
        column_row("gold.dim_session", "session_id", "STRING", "Session ID", "Session identifier.", "gold.fact_events", "session_id", "group by session_id", False, True, False, False, True, "abc-session", "", "session,phien"),
    ]
    columns = [
        "column_id",
        "table_name",
        "column_name",
        "data_type",
        "business_name",
        "description",
        "source_table",
        "source_column",
        "transformation_logic",
        "is_nullable",
        "is_dimension",
        "is_metric",
        "is_time_column",
        "is_join_key",
        "example_values",
        "allowed_values",
        "agent_synonyms",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def build_metadata_metric_catalog_df(spark, config):
    rows = [
        ("total_revenue", "Total Revenue", "Total sales revenue from purchase events.", "SUM(gross_amount)", "gold.fact_sales", "sale_date", "sum", "currency", "Doanh thu theo ngay la bao nhieu?"),
        ("purchase_count", "Purchase Count", "Number of purchase events.", "COUNT(*)", "gold.fact_sales", "sale_date", "count", "events", "Co bao nhieu purchase trong ngay?"),
        ("view_count", "View Count", "Number of product view events.", "SUM(total_views)", "gold.daily_event_summary", "event_date", "sum", "events", "Luot xem theo ngay la bao nhieu?"),
        ("conversion_rate", "Conversion Rate", "Purchases divided by views.", "SUM(total_purchases) / NULLIF(SUM(total_views), 0)", "gold.daily_event_summary", "event_date", "ratio", "ratio", "Ty le chuyen doi la bao nhieu?"),
        ("cart_to_purchase_rate", "Cart To Purchase Rate", "Purchases divided by cart events.", "SUM(total_purchases) / NULLIF(SUM(total_carts), 0)", "gold.daily_event_summary", "event_date", "ratio", "ratio", "Ty le gio hang sang mua hang la bao nhieu?"),
        ("active_users", "Active Users", "Distinct users that generated events.", "COUNT(DISTINCT user_id)", "gold.fact_events", "event_date", "count_distinct", "users", "Co bao nhieu user active?"),
        ("unique_sessions", "Unique Sessions", "Distinct sessions that generated events.", "COUNT(DISTINCT session_id)", "gold.fact_events", "event_date", "count_distinct", "sessions", "Co bao nhieu session?"),
        ("product_revenue", "Product Revenue", "Revenue grouped by product.", "SUM(revenue)", "gold.daily_product_summary", "event_date", "sum", "currency", "San pham nao co doanh thu cao nhat?"),
    ]
    columns = [
        "metric_name",
        "business_name",
        "description",
        "formula_sql",
        "base_table",
        "default_time_column",
        "aggregation_type",
        "unit",
        "example_question",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def build_metadata_join_catalog_df(spark, config):
    rows = [
        ("fact_events__dim_time", "gold.fact_events", "time_id", "gold.dim_time", "time_id", "many_to_one", "Join events to hourly time dimension."),
        ("fact_events__dim_product", "gold.fact_events", "product_id", "gold.dim_product", "product_id", "many_to_one", "Join events to product dimension."),
        ("fact_sales__dim_time", "gold.fact_sales", "time_id", "gold.dim_time", "time_id", "many_to_one", "Join sales to hourly time dimension."),
        ("fact_sales__dim_product", "gold.fact_sales", "product_id", "gold.dim_product", "product_id", "many_to_one", "Join sales to product dimension."),
        ("daily_product_summary__dim_product", "gold.daily_product_summary", "product_id", "gold.dim_product", "product_id", "many_to_one", "Join daily product metrics to product dimension."),
        ("fact_events__dim_user", "gold.fact_events", "user_id", "gold.dim_user", "user_id", "many_to_one", "Join events to user behavior dimension."),
        ("fact_events__dim_session", "gold.fact_events", "session_id", "gold.dim_session", "session_id", "many_to_one", "Join events to session behavior dimension."),
    ]
    columns = [
        "join_id",
        "left_table",
        "left_key",
        "right_table",
        "right_key",
        "relationship_type",
        "description",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def log_df_info(df, name):
    row_count = df.count()
    log(f"{name} rows: {row_count}")
    df.printSchema()
    return row_count


def write_table(spark, df, full_name, refresh_mode, dry_run):
    row_count = log_df_info(df, full_name)

    if dry_run:
        log(f"GOLD_DRY_RUN=true. Skipping write for {full_name}.")
        return row_count

    if refresh_mode == "append":
        log(f"Appending to {full_name}. Re-runs can duplicate data.")
        if row_count > 0:
            df.writeTo(full_name).append()
        else:
            log(f"No rows to append for {full_name}.")
        return row_count

    if refresh_mode != "full_refresh":
        raise ValueError(f"Unsupported GOLD_REFRESH_MODE={refresh_mode!r}")

    temp_view = re.sub(r"[^A-Za-z0-9_]", "_", f"tmp_{full_name}")
    columns = ", ".join(f"`{column}`" for column in df.columns)
    df.createOrReplaceTempView(temp_view)

    try:
        log(f"Full refresh via INSERT OVERWRITE for {full_name}")
        run_sql(spark, f"INSERT OVERWRITE {full_name} SELECT {columns} FROM {temp_view}")
    except Exception as exc:
        log(f"INSERT OVERWRITE failed for {full_name}: {type(exc).__name__}: {exc}")
        log(f"Fallback full refresh for {full_name}: DELETE all rows, then append if non-empty.")
        run_sql(spark, f"DELETE FROM {full_name} WHERE 1 = 1")
        if row_count > 0:
            df.writeTo(full_name).append()

    return row_count


def write_all_mvp_tables(spark, config, outputs):
    for short_name in MVP_TABLES:
        cached_df = outputs[short_name].cache()
        try:
            write_table(
                spark,
                cached_df,
                table_name(config.gold_namespace, short_name),
                config.refresh_mode,
                config.dry_run,
            )
        finally:
            cached_df.unpersist()


def write_all_extended_tables(spark, config, outputs):
    for short_name in EXTENDED_TABLES:
        cached_df = outputs[short_name].cache()
        try:
            write_table(
                spark,
                cached_df,
                table_name(config.gold_namespace, short_name),
                config.refresh_mode,
                config.dry_run,
            )
        finally:
            cached_df.unpersist()


def write_all_metadata_tables(spark, config, outputs):
    for short_name in METADATA_TABLES:
        cached_df = outputs[short_name].cache()
        try:
            write_table(
                spark,
                cached_df,
                table_name(config.metadata_namespace, short_name),
                config.refresh_mode,
                config.dry_run,
            )
        finally:
            cached_df.unpersist()


def validate_outputs(spark, config):
    if not config.validate_tables:
        log("GOLD_VALIDATE_TABLES=false. Skipping validation.")
        return

    run_sql(spark, f"SHOW NAMESPACES IN {config.catalog_name}", "Iceberg namespaces").show(200, truncate=False)
    run_sql(spark, f"SHOW TABLES IN {config.catalog_name}.{config.gold_namespace}", "Gold tables").show(200, truncate=False)
    run_sql(spark, f"SHOW TABLES IN {config.catalog_name}.{config.metadata_namespace}", "Metadata tables").show(200, truncate=False)

    for short_name in GOLD_TABLES:
        full_name = table_name(config.gold_namespace, short_name)
        run_sql(spark, f"DESCRIBE TABLE {full_name}", f"Schema for {full_name}").show(200, truncate=False)
        row = run_sql(spark, f"SELECT COUNT(*) AS row_count FROM {full_name}").collect()[0]
        log(f"Count {full_name}: {row['row_count']}")

    for short_name in METADATA_TABLES:
        full_name = table_name(config.metadata_namespace, short_name)
        run_sql(spark, f"DESCRIBE TABLE {full_name}", f"Schema for {full_name}").show(200, truncate=False)
        row = run_sql(spark, f"SELECT COUNT(*) AS row_count FROM {full_name}").collect()[0]
        log(f"Count {full_name}: {row['row_count']}")

    run_sql(
        spark,
        f"SELECT * FROM {table_name(config.gold_namespace, 'daily_event_summary')} ORDER BY event_date LIMIT 10",
        "Sample daily_event_summary",
    ).show(10, truncate=False)
    run_sql(
        spark,
        f"SELECT * FROM {table_name(config.gold_namespace, 'daily_product_summary')} LIMIT 10",
        "Sample daily_product_summary",
    ).show(10, truncate=False)
    run_sql(
        spark,
        f"SELECT * FROM {table_name(config.metadata_namespace, 'metric_catalog')}",
        "Metric catalog",
    ).show(200, truncate=False)


def build_mvp_outputs(base_df):
    return {
        "dim_time": build_dim_time_df(base_df),
        "dim_product": build_dim_product_df(base_df),
        "fact_events": build_fact_events_df(base_df),
        "fact_sales": build_fact_sales_df(base_df),
        "daily_event_summary": build_daily_event_summary_df(base_df),
    }


def build_extended_outputs(mvp_outputs):
    fact_events_df = mvp_outputs["fact_events"].cache()
    fact_sales_df = mvp_outputs["fact_sales"].cache()
    dim_product_df = mvp_outputs["dim_product"].cache()

    try:
        return {
            "dim_user": build_dim_user_df(fact_events_df, fact_sales_df),
            "dim_session": build_dim_session_df(fact_events_df, fact_sales_df),
            "daily_product_summary": build_daily_product_summary_df(fact_events_df, dim_product_df),
            "daily_category_summary": build_daily_category_summary_df(fact_events_df, dim_product_df),
            "daily_brand_summary": build_daily_brand_summary_df(fact_events_df, dim_product_df),
        }
    finally:
        fact_events_df.unpersist()
        fact_sales_df.unpersist()
        dim_product_df.unpersist()


def build_metadata_outputs(spark, config):
    return {
        "table_catalog": build_metadata_table_catalog_df(spark, config),
        "column_catalog": build_metadata_column_catalog_df(spark, config),
        "metric_catalog": build_metadata_metric_catalog_df(spark, config),
        "join_catalog": build_metadata_join_catalog_df(spark, config),
    }


def run_gold_data_flow(spark, config, write_mvp, write_extended, write_metadata):
    base_df = None
    try:
        silver_df = read_silver_events(spark, config)
        base_df = build_base_events_df(silver_df)
        mvp_outputs = build_mvp_outputs(base_df)

        if write_mvp:
            write_all_mvp_tables(spark, config, mvp_outputs)

        if write_extended:
            extended_outputs = build_extended_outputs(mvp_outputs)
            write_all_extended_tables(spark, config, extended_outputs)

        if write_metadata:
            metadata_outputs = build_metadata_outputs(spark, config)
            write_all_metadata_tables(spark, config, metadata_outputs)
    finally:
        if base_df is not None:
            base_df.unpersist()


def main():
    spark = None
    try:
        config = get_config()
        log(f"Catalog: {config.catalog_name}")
        log(f"Gold namespace: {config.gold_namespace}")
        log(f"Metadata namespace: {config.metadata_namespace}")
        log(f"Warehouse: {config.warehouse}")
        log(f"JDBC URI: {config.jdbc_uri}")
        log(f"JDBC schema: {config.jdbc_schema}")
        log(f"Silver path: {config.silver_events_path}")
        log(f"Run mode: {config.run_mode}")
        log(f"Refresh mode: {config.refresh_mode}")
        log(f"Dry run: {config.dry_run}")
        log(f"Validate tables: {config.validate_tables}")

        if config.refresh_mode == "append":
            log("WARNING: append mode can duplicate rows when re-running on the same Silver input.")

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        if config.run_mode == "validate_only":
            validate_outputs(spark, config)
            log("SUCCESS")
            return

        ensure_namespaces(spark, config)
        create_gold_tables(spark, config)
        create_metadata_tables(spark, config)

        if config.run_mode == "schema_only":
            validate_outputs(spark, config)
            log("SUCCESS")
            return

        if config.run_mode == "metadata_only":
            metadata_outputs = build_metadata_outputs(spark, config)
            write_all_metadata_tables(spark, config, metadata_outputs)
            validate_outputs(spark, config)
            log("SUCCESS")
            return

        run_gold_data_flow(
            spark,
            config,
            write_mvp=config.run_mode in {"all", "mvp_only"},
            write_extended=config.run_mode in {"all", "extended_only"},
            write_metadata=config.run_mode == "all",
        )

        validate_outputs(spark, config)
        log("SUCCESS")
    except Exception as exc:
        print(f"[GoldJob] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
