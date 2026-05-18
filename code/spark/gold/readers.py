"""Readers and source normalization for the Gold job."""

from pyspark.errors import AnalysisException
from pyspark.sql.functions import col, current_timestamp, date_format, lit

from common.data_quality import required_not_null_filter
from common.logging_utils import log


def path_exists(spark, path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def read_silver_parquet(spark, config):
    log("GoldJob", f"Input Silver path: {config.silver_events_path}")
    if not path_exists(spark, config.silver_events_path):
        raise FileNotFoundError(
            f"Silver events path does not exist: {config.silver_events_path}"
        )

    try:
        silver_df = spark.read.parquet(config.silver_events_path)
    except AnalysisException as exc:
        message = f"Cannot read Silver parquet at {config.silver_events_path}: {exc}"
        raise RuntimeError(message) from exc

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

    required_columns = [
        "source_event_id",
        "event_ts",
        "event_date",
        "event_type",
        "product_id",
        "user_id",
        "user_session",
    ]
    required_df = required_not_null_filter(dedup_df, required_columns)
    required_df = (
        required_df
        .withColumn("time_id", date_format(col("event_ts"), "yyyyMMddHH"))
        .withColumn("gold_processed_at", current_timestamp())
    )

    base_df = required_df.cache()
    base_rows = base_df.count()

    log("GoldJob", f"Total Silver rows: {total_rows}")
    log("GoldJob", f"Valid Silver rows: {valid_rows}")
    log("GoldJob", f"Rows after source_event_id dedup: {dedup_rows}")
    log("GoldJob", f"Rows after base required filters: {base_rows}")

    if total_rows == 0:
        raise RuntimeError("Silver path is readable but empty.")
    if valid_rows == 0:
        raise RuntimeError("Silver data has no is_valid=true records.")
    if base_rows == 0:
        raise RuntimeError("No Gold-eligible records remain after required filters.")

    return base_df


def read_silver_events(spark, config):
    silver_df = read_silver_parquet(spark, config)
    return build_base_events_df(silver_df)

