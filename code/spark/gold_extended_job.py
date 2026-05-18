"""
Build Gold Extended analytics tables from Gold MVP Iceberg tables.
"""

import os
import re
import sys
import traceback

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
    first,
    lit,
    max as spark_max,
    min as spark_min,
    sha2,
    sum as spark_sum,
    unix_timestamp,
    when,
)


APP_NAME = "GoldExtendedJob"


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

GOLD_EXTENDED_WRITE_MODE = env("GOLD_EXTENDED_WRITE_MODE", "overwrite_partitions").strip().lower()
GOLD_EXTENDED_DRY_RUN = env("GOLD_EXTENDED_DRY_RUN", "false").strip().lower() == "true"
GOLD_EXTENDED_VALIDATE_TABLES = (
    env("GOLD_EXTENDED_VALIDATE_TABLES", "true").strip().lower() == "true"
)
RESET_EXTENDED_DIMENSIONS = (
    env("RESET_EXTENDED_DIMENSIONS", "false").strip().lower() == "true"
)

ALLOWED_WRITE_MODES = {"append", "overwrite_partitions"}
INPUT_TABLES = ["fact_events", "fact_sales", "dim_product"]
OUTPUT_TABLES = [
    "dim_user",
    "dim_session",
    "daily_product_summary",
    "daily_category_summary",
    "daily_brand_summary",
]


def log(message):
    print(f"[GoldExtendedJob] {message}", flush=True)


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


def validate_required_tables(spark):
    if not GOLD_EXTENDED_VALIDATE_TABLES:
        log("GOLD_EXTENDED_VALIDATE_TABLES=false. Skipping table validation.")
        return

    rows = spark.sql(f"SHOW TABLES IN {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}").collect()
    existing = {row["tableName"] for row in rows}
    missing_inputs = [name for name in INPUT_TABLES if name not in existing]
    missing_outputs = [name for name in OUTPUT_TABLES if name not in existing]

    if missing_inputs:
        raise RuntimeError(f"Missing MVP input tables: {', '.join(missing_inputs)}. Run Stage 3 first.")
    if missing_outputs:
        raise RuntimeError(
            f"Missing extended output tables: {', '.join(missing_outputs)}. "
            "Run gold_extended_schema_init_pipeline first."
        )

    log(f"Validated required tables in {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}")


def read_fact_events(spark):
    df = spark.table(table_name("fact_events")).cache()
    log(f"Input fact_events count: {df.count()}")
    return df


def read_fact_sales(spark):
    df = spark.table(table_name("fact_sales")).cache()
    log(f"Input fact_sales count: {df.count()}")
    return df


def read_dim_product(spark):
    raw_df = spark.table(table_name("dim_product"))
    raw_count = raw_df.count()
    df = (
        raw_df
        .groupBy("product_id")
        .agg(
            first("brand", ignorenulls=True).alias("brand"),
            first("category_l1", ignorenulls=True).alias("category_l1"),
            first("category_l2", ignorenulls=True).alias("category_l2"),
            first("category_l3", ignorenulls=True).alias("category_l3"),
        )
        .cache()
    )
    log(f"Input dim_product raw count: {raw_count}")
    log(f"Input dim_product dedup count: {df.count()}")
    return df


def safe_divide(numerator_col, denominator_col):
    return when(denominator_col == lit(0), lit(0.0)).otherwise(
        numerator_col.cast("double") / denominator_col.cast("double")
    )


def bool_count(column_name):
    return spark_sum(when(col(column_name), 1).otherwise(0)).cast("long")


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
    return fact_events_df.join(dim_product_df, on="product_id", how="left")


def revenue_expr():
    return when(col("is_purchase"), col("price").cast("decimal(18,2)")).otherwise(
        lit(0).cast("decimal(18,2)")
    )


def build_daily_product_summary_df(fact_events_df, dim_product_df):
    joined = product_enriched_events(fact_events_df, dim_product_df)
    grouped = joined.groupBy(
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


def write_iceberg_table(spark, df, full_name, mode, table_type):
    row_count = df.count()
    log(f"Output rows for {full_name}: {row_count}")
    df.printSchema()

    if row_count == 0:
        log(f"Skipping write for {full_name}; DataFrame is empty.")
        return row_count
    if GOLD_EXTENDED_DRY_RUN:
        log(f"GOLD_EXTENDED_DRY_RUN=true. Skipping write for {full_name}.")
        return row_count

    if mode == "append":
        log(f"Appending to {full_name}")
        df.writeTo(full_name).append()
    elif mode == "overwrite_partitions":
        if table_type == "dim_user":
            if RESET_EXTENDED_DIMENSIONS:
                log(f"RESET_EXTENDED_DIMENSIONS=true. Deleting and appending {full_name}")
                spark.sql(f"DELETE FROM {full_name}")
                df.writeTo(full_name).append()
            else:
                log(f"WARNING: {full_name} is unpartitioned; appending can duplicate rows across runs.")
                df.writeTo(full_name).append()
        else:
            log(f"Overwriting touched partitions in {full_name}")
            df.writeTo(full_name).overwritePartitions()
    else:
        raise ValueError(f"Unsupported write mode: {mode}")

    return row_count


def log_iceberg_counts(spark):
    if GOLD_EXTENDED_DRY_RUN:
        log("GOLD_EXTENDED_DRY_RUN=true. Skipping post-write Iceberg counts.")
        return

    for short_name in OUTPUT_TABLES:
        row = spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name(short_name)}").collect()[0]
        log(f"Iceberg table count {table_name(short_name)}: {row['row_count']}")


def main():
    spark = None
    cached = []
    try:
        if GOLD_EXTENDED_WRITE_MODE not in ALLOWED_WRITE_MODES:
            raise ValueError(f"Invalid GOLD_EXTENDED_WRITE_MODE={GOLD_EXTENDED_WRITE_MODE!r}")

        log(f"Catalog: {ICEBERG_CATALOG_NAME}")
        log(f"Namespace: {ICEBERG_NAMESPACE}")
        log(f"Warehouse: {ICEBERG_WAREHOUSE}")
        log(f"Write mode: {GOLD_EXTENDED_WRITE_MODE}")
        log(f"Dry run: {GOLD_EXTENDED_DRY_RUN}")
        log(f"Validate tables: {GOLD_EXTENDED_VALIDATE_TABLES}")
        log(f"Reset extended dimensions: {RESET_EXTENDED_DIMENSIONS}")

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        validate_required_tables(spark)

        fact_events_df = read_fact_events(spark)
        fact_sales_df = read_fact_sales(spark)
        dim_product_df = read_dim_product(spark)
        cached.extend([fact_events_df, fact_sales_df, dim_product_df])

        outputs = {
            "dim_user": build_dim_user_df(fact_events_df, fact_sales_df),
            "dim_session": build_dim_session_df(fact_events_df, fact_sales_df),
            "daily_product_summary": build_daily_product_summary_df(fact_events_df, dim_product_df),
            "daily_category_summary": build_daily_category_summary_df(fact_events_df, dim_product_df),
            "daily_brand_summary": build_daily_brand_summary_df(fact_events_df, dim_product_df),
        }

        for short_name, df in outputs.items():
            cached_df = df.cache()
            cached.append(cached_df)
            write_iceberg_table(
                spark,
                cached_df,
                table_name(short_name),
                GOLD_EXTENDED_WRITE_MODE,
                short_name,
            )

        log_iceberg_counts(spark)
        log("SUCCESS")
    except Exception as exc:
        print(f"[GoldExtendedJob] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise
    finally:
        for df in cached:
            df.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
