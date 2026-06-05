"""
Spark Bronze Batch Job
- Đọc message mới từ Kafka dựa trên offset đã lưu ở MinIO
- Parse JSON, thêm metadata
- Ghi Parquet vào MinIO bucket bronze, partition theo date
- Cập nhật offset để lần sau không đọc trùng
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    regexp_replace,
    to_date,
    try_to_timestamp,
)
from pyspark.sql.types import StringType, StructField, StructType

from common.config import load_bronze_config
from common.partition_state import mark_bronze_done
from common.s3a import apply_s3a_options

# Schema của dữ liệu JSON (Bronze – giữ nguyên kiểu String)
ECOMMERCE_SCHEMA = StructType([
    StructField("event_time",    StringType(), True),
    StructField("event_type",    StringType(), True),
    StructField("product_id",    StringType(), True),
    StructField("category_id",   StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand",         StringType(), True),
    StructField("price",         StringType(), True),
    StructField("user_id",       StringType(), True),
    StructField("user_session",  StringType(), True),
    StructField("source_file",   StringType(), True),
    StructField("ingestion_batch_id", StringType(), True),
    StructField("chunk_id",      StringType(), True),
    StructField("ingest_time",   StringType(), True),
])

# ---------------------------------------------------------------------------
# Tạo SparkSession — jars được mount sẵn và truyền qua local classpath
# ---------------------------------------------------------------------------
def create_spark_session(config):
    builder = SparkSession.builder.appName("BronzeBatchJob")
    builder = apply_s3a_options(builder, config.minio)
    return (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Quản lý offset: đọc/ghi JSON trên MinIO
# Format lưu trên MinIO: {"ecommerce_events": {"0": 500, "1": 300, "2": 200}}
# Format truyền cho Spark: json string của dict trên
# ---------------------------------------------------------------------------
def read_offsets(spark, config):
    """
    Trả về dict {partition_int: offset_int} hoặc {} nếu chưa có file.
    """
    try:
        df    = spark.read.text(config.offset_file)
        lines = [r[0] for r in df.collect()]
        raw   = json.loads("".join(lines))          # {"topic": {"part": offset}}
        part_map = raw.get(config.kafka_topic, {})
        return {int(k): int(v) for k, v in part_map.items()}
    except Exception:
        return {}


def build_starting_offsets(offsets, kafka_topic):
    """
    Chuyển {0: 500, 1: 300} → '{"ecommerce_events":{"0":500,"1":300}}'
    Đây là format Spark Kafka connector yêu cầu.
    Trả về "earliest" nếu chưa có offset.
    """
    if not offsets:
        return "earliest"
    part_str = {str(k): v for k, v in offsets.items()}
    return json.dumps({kafka_topic: part_str})


def write_offsets(spark, config, offsets):
    """
    Ghi dict offset ra file JSON trên MinIO.
    """
    payload = json.dumps({
        config.kafka_topic: {str(k): v for k, v in sorted(offsets.items())}
    })
    spark.createDataFrame([payload], StringType()) \
         .write.mode("overwrite").text(config.offset_file)


# ---------------------------------------------------------------------------
# Transform: parse JSON, thêm metadata
# ---------------------------------------------------------------------------
def transform_bronze(raw_df):
    event_time_clean = regexp_replace(col("event_time"), r"\s+UTC$", "")
    event_ts_col = try_to_timestamp(event_time_clean, lit("yyyy-MM-dd HH:mm:ss"))

    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp", "partition", "offset")
        .withColumn("data", from_json(col("json_str"), ECOMMERCE_SCHEMA))
    )
    bronze = parsed.select(
        "data.*",
        col("timestamp").alias("kafka_ts"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )
    bronze = bronze.withColumn("event_ts", event_ts_col)
    bronze = bronze.withColumn("event_date", to_date(col("event_ts")))
    bronze = bronze.withColumn("ingested_at", current_timestamp())
    bronze = bronze.withColumn("kafka_date", to_date(col("kafka_ts")))
    # Backward-compatible column name; new physical partition is event_date.
    bronze = bronze.withColumn("date_partition", col("event_date"))
    return bronze


def collect_affected_dates(bronze_df):
    rows = (
        bronze_df
        .where(col("event_date").isNotNull())
        .groupBy("event_date")
        .count()
        .collect()
    )
    return {row["event_date"]: row["count"] for row in rows}


def collect_affected_batches(bronze_df):
    rows = (
        bronze_df
        .select("ingestion_batch_id")
        .where(col("ingestion_batch_id").isNotNull())
        .distinct()
        .collect()
    )
    return [row["ingestion_batch_id"] for row in rows]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    config = load_bronze_config()

    print("=" * 60)
    print("  Spark Bronze Batch Job")
    print(f"  Kafka  : {config.kafka_bootstrap} / {config.kafka_topic}")
    print(f"  Output : {config.output_path}")
    print(f"  State  : {config.partition_state_path}")
    print("=" * 60)

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc offset hiện tại
    current_offsets  = read_offsets(spark, config)
    starting_offsets = build_starting_offsets(current_offsets, config.kafka_topic)
    print(f"[Bronze] Starting offsets: {starting_offsets}")

    # 2. Đọc Kafka
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap)
        .option("subscribe",               config.kafka_topic)
        .option("startingOffsets",         starting_offsets)
        .option("endingOffsets",           "latest")
        .option("failOnDataLoss",          "false")   # tránh lỗi khi topic reset
        .load()
    )

    # 3. Kiểm tra dữ liệu mới
    if raw_df.limit(1).count() == 0:
        print("[Bronze] No new messages. Exiting.")
        spark.stop()
        return

    # Cache để dùng lại (count + max offset) mà không đọc lại Kafka
    raw_df.cache()

    # 4. Transform & ghi Parquet
    bronze_df = transform_bronze(raw_df).cache()
    affected_dates = collect_affected_dates(bronze_df)
    affected_batches = collect_affected_batches(bronze_df)

    bronze_df.write \
        .mode("append") \
        .partitionBy("event_date") \
        .parquet(config.output_path)

    row_count = raw_df.count()
    print(f"[Bronze] Written {row_count} rows to {config.output_path}")
    print(f"[Bronze] Affected event_dates: {sorted(str(date) for date in affected_dates)}")

    # Update partition state before committing Kafka offsets. If this fails,
    # the next Bronze run can safely reread offsets; Silver deduplicates by event.
    updated_dates = mark_bronze_done(
        spark,
        config.partition_state_path,
        affected_dates,
        affected_batches,
    )
    if updated_dates:
        print(f"[Bronze] Updated partition state for dates: {updated_dates}")
    else:
        print("[Bronze] No non-null event_date found; partition state unchanged.")

    # 5. Cập nhật offset (max_offset + 1 cho mỗi partition)
    max_offsets = {
        row["partition"]: row["max(offset)"] + 1
        for row in raw_df.groupBy("partition").max("offset").collect()
    }
    # Giữ lại partition cũ không có message mới
    for p, o in current_offsets.items():
        max_offsets.setdefault(p, o)

    write_offsets(spark, config, max_offsets)
    print(f"[Bronze] Updated offsets: {max_offsets}")

    bronze_df.unpersist()
    raw_df.unpersist()
    print("[Bronze] Completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
