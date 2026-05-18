"""
Silver Parquet to Gold MVP Iceberg ETL.

Stage 3 reads clean Silver events and writes the existing Gold MVP Iceberg
tables. It does not create/drop schemas, add extended Gold tables, or integrate
Trino/Agent metadata.
"""

import os
import re
import sys
import traceback

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
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
    sum as spark_sum,
    when,
)


APP_NAME = "GoldMvpJob"


def env(name, default):
    return os.getenv(name, default)


MINIO_ENDPOINT = env("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", "change_me")
MINIO_BUCKET_SILVER = env("MINIO_BUCKET_SILVER", "silver")

ICEBERG_CATALOG_NAME = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
ICEBERG_NAMESPACE = env("ICEBERG_NAMESPACE", "gold")
ICEBERG_WAREHOUSE = env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/")
ICEBERG_JDBC_URI = env(
    "ICEBERG_JDBC_URI",
    "jdbc:postgresql://postgres-db:5432/agent4da",
)
ICEBERG_JDBC_USER = env("ICEBERG_JDBC_USER", "bigdata")
ICEBERG_JDBC_PASSWORD = env("ICEBERG_JDBC_PASSWORD", "change_me")
ICEBERG_JDBC_SCHEMA = env("ICEBERG_JDBC_SCHEMA", "iceberg")

SILVER_EVENTS_PATH = env(
    "SILVER_EVENTS_PATH",
    f"s3a://{MINIO_BUCKET_SILVER}/ecommerce_events/",
)
GOLD_WRITE_MODE = env("GOLD_WRITE_MODE", "overwrite_partitions").strip().lower()
GOLD_VALIDATE_TABLES = env("GOLD_VALIDATE_TABLES", "true").strip().lower() == "true"
GOLD_DRY_RUN = env("GOLD_DRY_RUN", "false").strip().lower() == "true"
RESET_DIMENSIONS = env("RESET_DIMENSIONS", "false").strip().lower() == "true"

ALLOWED_WRITE_MODES = {"append", "overwrite_partitions"}

GOLD_TABLES = [
    "dim_time",
    "dim_product",
    "fact_events",
    "fact_sales",
    "daily_event_summary",
]


def log(message):
    print(f"[GoldMvpJob] {message}", flush=True)


def validate_identifier(identifier, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid {label}: {identifier!r}")


def table_name(short_name):
    validate_identifier(ICEBERG_CATALOG_NAME, "ICEBERG_CATALOG_NAME")
    validate_identifier(ICEBERG_NAMESPACE, "ICEBERG_NAMESPACE")
    validate_identifier(short_name, "table_name")
    return f"{ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}.{short_name}"


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


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def read_silver_events(spark):
    log(f"Input Silver path: {SILVER_EVENTS_PATH}")
    if not path_exists(spark, SILVER_EVENTS_PATH):
        raise FileNotFoundError(f"Silver events path does not exist: {SILVER_EVENTS_PATH}")

    try:
        silver_df = spark.read.parquet(SILVER_EVENTS_PATH)
    except AnalysisException as exc:
        raise RuntimeError(f"Cannot read Silver parquet at {SILVER_EVENTS_PATH}: {exc}") from exc

    if not silver_df.columns:
        raise RuntimeError(f"Silver parquet has no columns: {SILVER_EVENTS_PATH}")

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
        raise RuntimeError(f"Silver path is readable but empty: {SILVER_EVENTS_PATH}")
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
        .select(
            "time_id",
            "event_date",
            "event_year",
            "event_month",
            "event_day",
            "event_hour",
            "event_ts",
        )
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
            col("product_id"),
            col("category_id"),
            col("category_code"),
            col("category_l1"),
            col("category_l2"),
            col("category_l3"),
            col("brand"),
            col("first_seen_at"),
            col("last_seen_at"),
            col("avg_observed_price"),
            col("min_observed_price"),
            col("max_observed_price"),
            col("record_count"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


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
            spark_sum(
                when(col("event_type") == "remove_from_cart", 1).otherwise(0)
            ).cast("long").alias("total_remove_from_carts"),
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
        .withColumn(
            "conversion_rate",
            when(col("total_views") == 0, lit(0.0)).otherwise(
                col("total_purchases").cast("double") / col("total_views").cast("double")
            ),
        )
        .withColumn(
            "cart_to_purchase_rate",
            when(col("total_carts") == 0, lit(0.0)).otherwise(
                col("total_purchases").cast("double") / col("total_carts").cast("double")
            ),
        )
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


def validate_gold_tables_exist(spark):
    if not GOLD_VALIDATE_TABLES:
        log("GOLD_VALIDATE_TABLES is disabled. Skipping table existence validation.")
        return

    log(f"Validating Gold tables in {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}")
    table_rows = spark.sql(
        f"SHOW TABLES IN {ICEBERG_CATALOG_NAME}.{ICEBERG_NAMESPACE}"
    ).collect()
    existing_tables = {row["tableName"] for row in table_rows}
    missing_tables = [table for table in GOLD_TABLES if table not in existing_tables]

    if missing_tables:
        raise RuntimeError(
            "Missing Gold Iceberg tables: "
            f"{', '.join(missing_tables)}. Run Stage 2 gold_schema_init_pipeline first."
        )

    log(f"Found Gold tables: {', '.join(sorted(existing_tables))}")


def write_iceberg_table(df, full_name, mode):
    row_count = df.count()
    log(f"Output rows for {full_name}: {row_count}")
    df.printSchema()

    if row_count == 0:
        log(f"Skipping write for {full_name}; DataFrame is empty.")
        return row_count

    if GOLD_DRY_RUN:
        log(f"GOLD_DRY_RUN=true. Skipping write for {full_name}.")
        return row_count

    if mode == "append":
        log(f"Appending to {full_name}")
        df.writeTo(full_name).append()
    elif mode == "overwrite_partitions":
        log(f"Overwriting touched partitions in {full_name}")
        df.writeTo(full_name).overwritePartitions()
    else:
        raise ValueError(f"Unsupported write mode for {full_name}: {mode}")

    return row_count


def write_outputs(spark, outputs):
    if GOLD_WRITE_MODE not in ALLOWED_WRITE_MODES:
        raise ValueError(
            f"Invalid GOLD_WRITE_MODE={GOLD_WRITE_MODE!r}. "
            f"Allowed: {', '.join(sorted(ALLOWED_WRITE_MODES))}"
        )

    if GOLD_WRITE_MODE == "append":
        log("GOLD_WRITE_MODE=append. Re-running can duplicate rows.")

    if GOLD_WRITE_MODE == "overwrite_partitions":
        log("GOLD_WRITE_MODE=overwrite_partitions.")

    if RESET_DIMENSIONS and not GOLD_DRY_RUN:
        log("RESET_DIMENSIONS=true. Deleting dim_time and dim_product before append.")
        spark.sql(f"DELETE FROM {table_name('dim_time')}")
        spark.sql(f"DELETE FROM {table_name('dim_product')}")
    elif RESET_DIMENSIONS and GOLD_DRY_RUN:
        log("RESET_DIMENSIONS=true, but GOLD_DRY_RUN=true. Dimension deletes skipped.")

    write_plan = [
        ("fact_events", GOLD_WRITE_MODE),
        ("fact_sales", GOLD_WRITE_MODE),
        ("daily_event_summary", GOLD_WRITE_MODE),
        ("dim_time", "append" if RESET_DIMENSIONS else GOLD_WRITE_MODE),
        ("dim_product", "append"),
    ]

    if not RESET_DIMENSIONS:
        log(
            "RESET_DIMENSIONS=false. dim_product uses append and can duplicate "
            "across runs; Stage 4 can replace this with MERGE INTO."
        )

    written_counts = {}
    for short_name, mode in write_plan:
        full_name = table_name(short_name)
        df = outputs[short_name].cache()
        try:
            written_counts[short_name] = write_iceberg_table(df, full_name, mode)
        finally:
            df.unpersist()

    return written_counts


def log_counts(name_to_df):
    counts = {}
    for name, df in name_to_df.items():
        counts[name] = df.count()
        log(f"{name} count: {counts[name]}")
    return counts


def log_iceberg_counts(spark):
    if GOLD_DRY_RUN:
        log("GOLD_DRY_RUN=true. Skipping post-write Iceberg table counts.")
        return

    for short_name in GOLD_TABLES:
        full_name = table_name(short_name)
        row = spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_name}").collect()[0]
        log(f"Iceberg table count {full_name}: {row['row_count']}")


def main():
    spark = None
    base_df = None
    try:
        log(f"Input Silver path: {SILVER_EVENTS_PATH}")
        log(f"Catalog: {ICEBERG_CATALOG_NAME}")
        log(f"Namespace: {ICEBERG_NAMESPACE}")
        log(f"Warehouse: {ICEBERG_WAREHOUSE}")
        log(f"JDBC URI: {ICEBERG_JDBC_URI}")
        log(f"JDBC schema: {ICEBERG_JDBC_SCHEMA}")
        log(f"Write mode: {GOLD_WRITE_MODE}")
        log(f"Dry run: {GOLD_DRY_RUN}")
        log(f"Validate tables: {GOLD_VALIDATE_TABLES}")
        log(f"Reset dimensions: {RESET_DIMENSIONS}")

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        validate_gold_tables_exist(spark)
        silver_df = read_silver_events(spark)
        base_df = build_base_events_df(silver_df)

        outputs = {
            "fact_events": build_fact_events_df(base_df),
            "fact_sales": build_fact_sales_df(base_df),
            "dim_time": build_dim_time_df(base_df),
            "dim_product": build_dim_product_df(base_df),
            "daily_event_summary": build_daily_event_summary_df(base_df),
        }

        log_counts(outputs)
        write_outputs(spark, outputs)
        log_iceberg_counts(spark)
        log("SUCCESS")
    except Exception as exc:
        print(
            f"[GoldMvpJob] FAILED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        traceback.print_exc()
        raise
    finally:
        if base_df is not None:
            base_df.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
