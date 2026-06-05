"""
Spark Silver Batch Job

Read pending Bronze event_date partitions from MinIO, normalize types, add
data-quality flags, and replace matching Silver Parquet partitions.

Note on idempotency:
source_event_id is kept for Kafka lineage. event_fingerprint is built from the
business event content and is used for deduplication. The default path reads
only manifest-pending event_date partitions, deletes the matching Silver output
partitions, then writes the replacement rows. Because the output is still plain
Parquet, this is intended for batch single-writer usage. Use Iceberg/Delta MERGE
later if multiple Silver jobs can write at the same time.
"""

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    date_format,
    dayofmonth,
    expr,
    hour,
    lit,
    lower,
    month,
    regexp_replace,
    row_number,
    sha2,
    to_date,
    trim,
    split,
    size,
    slice,
    try_to_timestamp,
    when,
    year,
)
from pyspark.sql.window import Window

from common.config import load_silver_config
from common.partition_state import (
    format_partition_date,
    mark_silver_done,
    mark_silver_pending_with_error,
    pending_silver_dates,
)
from common.s3a import apply_s3a_options


JOB_NAME = "SilverEcommerceEventsJob"


def log(message):
    print(f"[Silver] {message}", flush=True)


VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

SILVER_COLUMNS = [
    "source_event_id",
    "event_fingerprint",
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


def create_spark_session(config):
    builder = SparkSession.builder.appName(JOB_NAME)
    builder = apply_s3a_options(builder, config.minio)
    return (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .getOrCreate()
    )


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def delete_path_if_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(hadoop_path):
        return fs.delete(hadoop_path, True)
    return False


def event_date_partition_path(base_path, partition_date):
    return f"{base_path.rstrip('/')}/event_date={partition_date}"


def read_bronze_dataframe(spark, config):
    if not path_exists(spark, config.input_path):
        log(f"No Bronze data found at {config.input_path}. Exiting safely.")
        return None

    try:
        return spark.read.parquet(config.input_path)
    except AnalysisException as exc:
        log(f"No readable Bronze parquet at {config.input_path}.")
        log(f"Spark message: {exc}")
        return None


def read_bronze_dataframe_for_dates(spark, config, partition_dates):
    if not path_exists(spark, config.input_path):
        log(f"No Bronze data found at {config.input_path}. Exiting safely.")
        return None, [], list(partition_dates)

    existing_paths = []
    readable_dates = []
    missing_dates = []

    for partition_date in partition_dates:
        path = event_date_partition_path(config.input_path, partition_date)
        if path_exists(spark, path):
            existing_paths.append(path)
            readable_dates.append(partition_date)
        else:
            missing_dates.append(partition_date)

    if not existing_paths:
        return None, readable_dates, missing_dates

    try:
        bronze_df = spark.read.option("basePath", config.input_path).parquet(*existing_paths)
        return bronze_df, readable_dates, missing_dates
    except AnalysisException as exc:
        log(f"No readable Bronze event_date partitions at {config.input_path}.")
        log(f"Spark message: {exc}")
        return None, readable_dates, partition_dates


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


def column_exists(df, column_name):
    return column_name in df.columns


def optional_cast_column(df, column_name, target_type):
    if column_exists(df, column_name):
        return expr(f"try_cast({column_name} as {target_type})")
    return lit(None).cast(target_type)


def fingerprint_value(value_col):
    return coalesce(value_col.cast("string"), lit("_null_"))


def event_fingerprint_expr():
    event_parts = [
        coalesce(date_format(col("event_ts"), "yyyy-MM-dd HH:mm:ss"), lit("_null_")),
        fingerprint_value(col("event_type")),
        fingerprint_value(col("product_id")),
        fingerprint_value(col("category_id")),
        fingerprint_value(col("category_code")),
        fingerprint_value(col("brand")),
        fingerprint_value(col("price")),
        fingerprint_value(col("user_id")),
        fingerprint_value(col("user_session")),
    ]
    return sha2(concat_ws("||", *event_parts), 256)


def build_silver_dataframe(bronze_df):
    if column_exists(bronze_df, "event_ts"):
        event_ts_col = optional_cast_column(bronze_df, "event_ts", "timestamp")
    else:
        event_time_clean = regexp_replace(col("event_time"), r"\s+UTC$", "")
        event_ts_col = try_to_timestamp(
            event_time_clean,
            lit("yyyy-MM-dd HH:mm:ss"),
        )
    event_date_col = optional_cast_column(bronze_df, "event_date", "date")
    category_code_col = normalize_empty_to_null(col("category_code"))
    brand_clean = normalize_empty_to_null(col("brand"))
    brand_col = when(brand_clean.isNull(), lit("unknown")).otherwise(brand_clean)

    silver = bronze_df.select(
        event_ts_col.alias("event_ts"),
        event_date_col.alias("_bronze_event_date"),
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
        .withColumn("event_date", coalesce(col("_bronze_event_date"), to_date(col("event_ts"))))
        .withColumn("event_year", year(col("event_ts")))
        .withColumn("event_month", month(col("event_ts")))
        .withColumn("event_day", dayofmonth(col("event_ts")))
        .withColumn("event_hour", hour(col("event_ts")))
        .withColumn("category_parts", split(col("category_code"), r"\."))
        .withColumn("category_l1", expr("get(category_parts, 0)"))
        .withColumn("category_l2", expr("get(category_parts, 1)"))
        .withColumn(
            "category_l3",
            when(
                size(col("category_parts")) > 2,
                concat_ws(".", slice(col("category_parts"), 3, size(col("category_parts")) - 2))
            ).otherwise(lit(None).cast("string"))
        )
        .drop("category_parts")
        .withColumn(
            "source_event_id",
            concat_ws(
                "_",
                col("kafka_partition").cast("string"),
                col("kafka_offset").cast("string"),
            ),
        )
        .withColumn("event_fingerprint", event_fingerprint_expr())
        .drop("_bronze_event_date")
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
    # Keep one record per business event content. Kafka metadata is only used
    # as a tie-breaker when the same event appears more than once.
    window = Window.partitionBy("event_fingerprint").orderBy(
        col("bronze_ingested_at").desc_nulls_last(),
        col("kafka_ts").desc_nulls_last(),
        col("kafka_partition").desc_nulls_last(),
        col("kafka_offset").desc_nulls_last(),
    )
    return (
        valid_df
        .withColumn("_row_number", row_number().over(window))
        .where(col("_row_number") == 1)
        .drop("_row_number")
    )


def read_existing_event_fingerprints(spark, path):
    if not path_exists(spark, path):
        log(f"No existing output found at {path}.")
        return None

    try:
        existing_df = spark.read.option("mergeSchema", "true").parquet(path)

        if "event_fingerprint" not in existing_df.columns:
            log("Existing output has no event_fingerprint; computing it from existing rows.")
            existing_df = existing_df.withColumn(
                "event_fingerprint",
                event_fingerprint_expr(),
            )

        return existing_df.select("event_fingerprint").where(
            col("event_fingerprint").isNotNull()
        ).distinct()
    except AnalysisException as exc:
        log(f"Cannot read existing event_fingerprints at {path}.")
        log(f"Spark message: {exc}")
        return None


def normalize_write_mode(mode):
    if mode in ("append", "overwrite"):
        return mode

    log(f"Unsupported SILVER_WRITE_MODE={mode!r}; fallback to append.")
    return "append"


def collect_count_by_date(df):
    rows = df.groupBy("event_date").count().collect()
    counts = {}
    for row in rows:
        partition_date = format_partition_date(row["event_date"])
        if partition_date is not None:
            counts[partition_date] = int(row["count"])
    return counts


def write_df_if_not_empty(df, output_path):
    if df.limit(1).count() == 0:
        return 0
    row_count = df.count()
    (
        df.write
        .mode("append")
        .partitionBy("event_date")
        .parquet(output_path)
    )
    return row_count


def replace_event_date_partitions(spark, output_path, partition_dates):
    for partition_date in partition_dates:
        path = event_date_partition_path(output_path, partition_date)
        deleted = delete_path_if_exists(spark, path)
        if deleted:
            log(f"Deleted old Silver partition: {path}")


def write_partition_outputs(silver_df, config, partition_dates):
    spark = silver_df.sparkSession
    valid_df = silver_df.where(col("is_valid"))
    invalid_df = silver_df.where(~col("is_valid"))
    valid_dedup_df = deduplicate_valid_events(valid_df).cache()
    invalid_output_df = invalid_df.cache()

    try:
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        valid_dedup_count = valid_dedup_df.count()
        skipped_duplicate_count = valid_count - valid_dedup_count
        valid_counts_by_date = collect_count_by_date(valid_dedup_df)
        invalid_counts_by_date = collect_count_by_date(invalid_output_df)

        log(f"Valid rows before dedup : {valid_count}")
        log(f"Invalid rows            : {invalid_count}")
        log(f"Valid rows after dedup  : {valid_dedup_count}")
        log(f"Valid duplicate fingerprints skipped : {skipped_duplicate_count}")
        log(f"Replacing event_dates   : {partition_dates}")
        log("Output schema:")
        valid_dedup_df.printSchema()
        log("Sample valid rows:")
        valid_dedup_df.show(5, truncate=False)

        replace_event_date_partitions(spark, config.valid_output_path, partition_dates)
        replace_event_date_partitions(spark, config.invalid_output_path, partition_dates)

        written_valid = write_df_if_not_empty(valid_dedup_df, config.valid_output_path)
        written_invalid = write_df_if_not_empty(invalid_output_df, config.invalid_output_path)
        log(f"Valid rows written      : {written_valid}")
        log(f"Invalid rows written    : {written_invalid}")
        return valid_counts_by_date, invalid_counts_by_date
    finally:
        invalid_output_df.unpersist()
        valid_dedup_df.unpersist()


def write_outputs(silver_df, config):
    spark = silver_df.sparkSession
    write_mode = normalize_write_mode(config.write_mode)
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
    existing_fingerprints = None

    if write_mode == "append":
        existing_fingerprints = read_existing_event_fingerprints(
            spark,
            config.valid_output_path,
        )
        if existing_fingerprints is not None:
            existing_fingerprints.cache()
            existing_fingerprints.count()
            valid_output_df = valid_dedup_df.join(
                existing_fingerprints,
                on="event_fingerprint",
                how="left_anti",
            ).select(*SILVER_COLUMNS).cache()
            valid_output_count = valid_output_df.count()
            skipped_existing_count = valid_dedup_count - valid_output_count

    log(f"Valid rows before dedup : {valid_count}")
    log(f"Invalid rows            : {invalid_count}")
    log(f"Valid rows after dedup  : {valid_dedup_count}")
    log(f"Valid duplicate fingerprints skipped : {skipped_existing_count}")
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
        .parquet(config.valid_output_path)
    )

    (
        invalid_output_df.write
        .mode(write_mode)
        .partitionBy("processing_date")
        .parquet(config.invalid_output_path)
    )

    if existing_fingerprints is not None:
        existing_fingerprints.unpersist()
    if valid_output_df is not valid_dedup_df:
        valid_output_df.unpersist()
    valid_dedup_df.unpersist()


def main():
    config = load_silver_config()

    print("=" * 70)
    print("  Spark Silver E-commerce Events Job")
    print(f"  Input         : {config.input_path}")
    print(f"  Output valid  : {config.valid_output_path}")
    print(f"  Output invalid: {config.invalid_output_path}")
    print(f"  State         : {config.partition_state_path}")
    print(f"  Max dates/run : {config.max_dates_per_run}")
    print("=" * 70)

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    selected_dates = []
    readable_dates = []

    try:
        selected_dates = pending_silver_dates(
            spark,
            config.partition_state_path,
            config.max_dates_per_run,
        )
        log(f"Pending event_dates selected: {selected_dates}")

        if not selected_dates:
            if not config.full_scan_fallback:
                log("No pending event_date partitions. Exiting safely.")
                return

            log("SILVER_FULL_SCAN_FALLBACK enabled; running legacy full scan.")
            bronze_df = read_bronze_dataframe(spark, config)
            if bronze_df is None:
                return
            bronze_df.cache()
            total_rows = bronze_df.count()
            log(f"Total Bronze rows read: {total_rows}")
            silver_df = build_silver_dataframe(bronze_df).cache()
            write_outputs(silver_df, config)
            silver_df.unpersist()
            bronze_df.unpersist()
            log("Completed legacy full-scan Silver run.")
            return

        bronze_df, readable_dates, missing_dates = read_bronze_dataframe_for_dates(
            spark,
            config,
            selected_dates,
        )
        if missing_dates:
            message = "Missing Bronze event_date partition(s): " + ", ".join(missing_dates)
            log(message)
            mark_silver_pending_with_error(
                spark,
                config.partition_state_path,
                missing_dates,
                message,
            )

        if bronze_df is None or not readable_dates:
            raise RuntimeError("No readable Bronze partitions for selected pending dates.")

        bronze_df.cache()
        total_rows = bronze_df.count()
        log(f"Total Bronze rows read from pending partitions: {total_rows}")

        silver_df = (
            build_silver_dataframe(bronze_df)
            .where(col("event_date").cast("string").isin(readable_dates))
            .cache()
        )
        valid_counts, invalid_counts = write_partition_outputs(
            silver_df,
            config,
            readable_dates,
        )
        updated_dates = mark_silver_done(
            spark,
            config.partition_state_path,
            readable_dates,
            valid_counts,
            invalid_counts,
        )
        log(f"Marked Silver DONE for dates: {updated_dates}")

        silver_df.unpersist()
        bronze_df.unpersist()
        log("Completed successfully.")
    except Exception as exc:
        failed_dates = readable_dates or selected_dates
        if failed_dates:
            try:
                mark_silver_pending_with_error(
                    spark,
                    config.partition_state_path,
                    failed_dates,
                    exc,
                )
            except Exception as state_exc:
                log(f"Failed to update Silver error state: {state_exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
