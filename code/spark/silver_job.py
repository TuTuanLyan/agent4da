"""
Spark Silver Batch Job

Read Bronze e-commerce events from MinIO, normalize types, add data-quality
flags, and write clean Silver Parquet outputs.

Note on idempotency:
This job deduplicates records inside the current batch by source_event_id.
Because the output is plain Parquet in append mode, rerunning the same Bronze
input can still append duplicate records across runs. Use
SILVER_WRITE_MODE=overwrite only for local/test resets.
"""

import os

from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    dayofmonth,
    expr,
    hour,
    lit,
    lower,
    month,
    regexp_replace,
    row_number,
    to_date,
    trim,
    try_to_timestamp,
    when,
    year,
)
from pyspark.sql.window import Window


JOB_NAME = "SilverEcommerceEventsJob"


def log(message):
    print(f"[Silver] {message}", flush=True)


# ---------------------------------------------------------------------------
# Environment config with safe defaults for the Docker stack
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "Admin123!")
BRONZE_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "silver")
SILVER_WRITE_MODE = os.getenv("SILVER_WRITE_MODE", "append").strip().lower()

INPUT_PATH = f"s3a://{BRONZE_BUCKET}/ecommerce_events/"
VALID_OUTPUT_PATH = f"s3a://{SILVER_BUCKET}/ecommerce_events/"
INVALID_OUTPUT_PATH = f"s3a://{SILVER_BUCKET}/ecommerce_events_invalid/"

VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

SILVER_COLUMNS = [
    "source_event_id",
    "event_ts",
    "event_date",
    "event_year",
    "event_month",
    "event_day",
    "event_hour",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "category_l1",
    "category_l2",
    "category_l3",
    "brand",
    "price",
    "user_id",
    "user_session",
    "kafka_ts",
    "kafka_partition",
    "kafka_offset",
    "bronze_ingested_at",
    "silver_processed_at",
    "is_valid",
    "invalid_reason",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName(JOB_NAME)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def read_bronze_dataframe(spark):
    if not path_exists(spark, INPUT_PATH):
        log(f"No Bronze data found at {INPUT_PATH}. Exiting safely.")
        return None

    try:
        return spark.read.parquet(INPUT_PATH)
    except AnalysisException as exc:
        log(f"No readable Bronze parquet at {INPUT_PATH}.")
        log(f"Spark message: {exc}")
        return None


def normalize_empty_to_null(value_col):
    cleaned = lower(trim(value_col))
    return (
        when(value_col.isNull(), lit(None).cast("string"))
        .when(cleaned.isin("", "null", "none", "nan"), lit(None).cast("string"))
        .otherwise(cleaned)
    )


def normalize_session(value_col):
    cleaned = trim(value_col)
    return (
        when(value_col.isNull(), lit(None).cast("string"))
        .when(cleaned == "", lit(None).cast("string"))
        .otherwise(cleaned)
    )


def try_cast_column(column_name, target_type):
    return expr(f"try_cast({column_name} as {target_type})")


def build_silver_dataframe(bronze_df):
    event_time_clean = regexp_replace(col("event_time"), r"\s+UTC$", "")
    event_ts_col = try_to_timestamp(
        event_time_clean,
        lit("yyyy-MM-dd HH:mm:ss"),
    )
    category_code_col = normalize_empty_to_null(col("category_code"))
    brand_clean = normalize_empty_to_null(col("brand"))
    brand_col = when(brand_clean.isNull(), lit("unknown")).otherwise(brand_clean)

    silver = bronze_df.select(
        event_ts_col.alias("event_ts"),
        lower(trim(col("event_type"))).alias("event_type"),
        try_cast_column("product_id", "bigint").alias("product_id"),
        try_cast_column("category_id", "bigint").alias("category_id"),
        category_code_col.alias("category_code"),
        brand_col.alias("brand"),
        try_cast_column("price", "decimal(10,2)").alias("price"),
        try_cast_column("user_id", "bigint").alias("user_id"),
        normalize_session(col("user_session")).alias("user_session"),
        try_cast_column("kafka_ts", "timestamp").alias("kafka_ts"),
        try_cast_column("kafka_partition", "int").alias("kafka_partition"),
        try_cast_column("kafka_offset", "bigint").alias("kafka_offset"),
        try_cast_column("ingested_at", "timestamp").alias("bronze_ingested_at"),
        current_timestamp().alias("silver_processed_at"),
    )

    silver = (
        silver
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("event_year", year(col("event_ts")))
        .withColumn("event_month", month(col("event_ts")))
        .withColumn("event_day", dayofmonth(col("event_ts")))
        .withColumn("event_hour", hour(col("event_ts")))
        # Spark 4 ANSI mode fails on array[index] when the index is missing.
        # SQL get(array, index) is 0-based and returns NULL for out-of-range.
        .withColumn("category_l1", expr(r"get(split(category_code, '\\.'), 0)"))
        .withColumn("category_l2", expr(r"get(split(category_code, '\\.'), 1)"))
        .withColumn("category_l3", expr(r"get(split(category_code, '\\.'), 2)"))
        .withColumn(
            "source_event_id",
            concat_ws(
                "_",
                col("kafka_partition").cast("string"),
                col("kafka_offset").cast("string"),
            ),
        )
    )

    missing_event_ts = col("event_ts").isNull()
    invalid_event_type = (
        col("event_type").isNull() | (~col("event_type").isin(VALID_EVENT_TYPES))
    )
    missing_product_id = col("product_id").isNull()
    missing_category_id = col("category_id").isNull()
    missing_user_id = col("user_id").isNull()
    missing_user_session = col("user_session").isNull()
    invalid_price = col("price").isNull() | (col("price") < lit(0).cast("decimal(10,2)"))

    is_valid_col = ~(
        missing_event_ts
        | invalid_event_type
        | missing_product_id
        | missing_category_id
        | missing_user_id
        | missing_user_session
        | invalid_price
    )

    reason_cols = [
        when(missing_event_ts, lit("missing_event_ts")),
        when(invalid_event_type, lit("invalid_event_type")),
        when(missing_product_id, lit("missing_product_id")),
        when(missing_category_id, lit("missing_category_id")),
        when(missing_user_id, lit("missing_user_id")),
        when(missing_user_session, lit("missing_user_session")),
        when(invalid_price, lit("invalid_price")),
    ]

    silver = (
        silver
        .withColumn("is_valid", is_valid_col)
        .withColumn(
            "invalid_reason",
            when(col("is_valid"), lit(None).cast("string")).otherwise(
                concat_ws("; ", *reason_cols)
            ),
        )
    )

    return silver.select(*SILVER_COLUMNS)


def deduplicate_valid_events(valid_df):
    # Keep one record per Kafka partition/offset. If duplicates exist in this
    # batch, the latest processed row wins; kafka_ts is only a deterministic tie.
    window = Window.partitionBy("source_event_id").orderBy(
        col("silver_processed_at").desc(),
        col("kafka_ts").desc_nulls_last(),
    )
    return (
        valid_df
        .withColumn("_row_number", row_number().over(window))
        .where(col("_row_number") == 1)
        .drop("_row_number")
    )


def read_existing_source_event_ids(spark, path):
    if not path_exists(spark, path):
        log(f"No existing output found at {path}.")
        return None

    try:
        return (
            spark.read.parquet(path)
            .select("source_event_id")
            .where(col("source_event_id").isNotNull())
            .distinct()
        )
    except AnalysisException as exc:
        log(f"Cannot read existing output ids at {path}.")
        log(f"Spark message: {exc}")
        return None


def normalize_write_mode(mode):
    if mode in ("append", "overwrite"):
        return mode

    log(f"Unsupported SILVER_WRITE_MODE={mode!r}; fallback to append.")
    return "append"


def write_outputs(silver_df):
    spark = silver_df.sparkSession
    write_mode = normalize_write_mode(SILVER_WRITE_MODE)
    valid_df = silver_df.where(col("is_valid"))
    invalid_df = silver_df.where(~col("is_valid"))
    valid_dedup_df = deduplicate_valid_events(valid_df).cache()
    invalid_output_df = invalid_df.withColumn(
        "processing_date",
        to_date(col("silver_processed_at")),
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    valid_dedup_count = valid_dedup_df.count()
    valid_output_df = valid_dedup_df
    valid_output_count = valid_dedup_count
    skipped_existing_count = 0
    existing_valid_ids = None

    if write_mode == "append":
        existing_valid_ids = read_existing_source_event_ids(spark, VALID_OUTPUT_PATH)
        if existing_valid_ids is not None:
            existing_valid_ids.cache()
            existing_valid_ids.count()
            valid_output_df = valid_dedup_df.join(
                existing_valid_ids,
                on="source_event_id",
                how="left_anti",
            ).cache()
            valid_output_count = valid_output_df.count()
            skipped_existing_count = valid_dedup_count - valid_output_count

    log(f"Valid rows before dedup : {valid_count}")
    log(f"Invalid rows            : {invalid_count}")
    log(f"Valid rows after dedup  : {valid_dedup_count}")
    log(f"Valid rows already exist : {skipped_existing_count}")
    log(f"Valid rows to write      : {valid_output_count}")
    log(f"Write mode              : {write_mode}")
    log("Output schema:")
    valid_output_df.printSchema()
    log("Sample valid rows:")
    valid_output_df.show(5, truncate=False)

    (
        valid_output_df.write
        .mode(write_mode)
        .partitionBy("event_date")
        .parquet(VALID_OUTPUT_PATH)
    )

    (
        invalid_output_df.write
        .mode(write_mode)
        .partitionBy("processing_date")
        .parquet(INVALID_OUTPUT_PATH)
    )

    if existing_valid_ids is not None:
        existing_valid_ids.unpersist()
    if valid_output_df is not valid_dedup_df:
        valid_output_df.unpersist()
    valid_dedup_df.unpersist()


def main():
    print("=" * 70)
    print("  Spark Silver E-commerce Events Job")
    print(f"  Input         : {INPUT_PATH}")
    print(f"  Output valid  : {VALID_OUTPUT_PATH}")
    print(f"  Output invalid: {INVALID_OUTPUT_PATH}")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_df = read_bronze_dataframe(spark)
        if bronze_df is None:
            return

        if bronze_df.limit(1).count() == 0:
            log(f"Bronze path is empty at {INPUT_PATH}. Exiting safely.")
            return

        bronze_df.cache()
        total_rows = bronze_df.count()
        log(f"Total Bronze rows read: {total_rows}")

        silver_df = build_silver_dataframe(bronze_df).cache()
        write_outputs(silver_df)

        silver_df.unpersist()
        bronze_df.unpersist()
        log("Completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
