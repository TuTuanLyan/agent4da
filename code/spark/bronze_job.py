"""
Spark Bronze Batch Job
- Đọc message mới từ Kafka dựa trên offset đã lưu ở MinIO
- Parse JSON, thêm metadata
- Ghi Parquet vào MinIO bucket bronze, partition theo date
- Cập nhật offset để lần sau không đọc trùng
"""

import json
from pyspark.sql.functions import col, current_timestamp, from_json, to_date
from pyspark.sql.types import StringType, StructField, StructType

from common.config import load_config
from common.spark_session import create_spark_session as build_spark_session

# ---------------------------------------------------------------------------
# Cấu hình từ biến môi trường (có fallback)
# ---------------------------------------------------------------------------
CONFIG = load_config(validate_gold=False)
KAFKA_BOOTSTRAP = CONFIG.kafka_bootstrap
KAFKA_TOPIC     = CONFIG.kafka_topic
BRONZE_BUCKET   = CONFIG.minio.bronze_bucket

OUTPUT_PATH  = f"s3a://{BRONZE_BUCKET}/ecommerce_events/"
OFFSET_FILE  = f"s3a://{BRONZE_BUCKET}/_offsets/ecommerce_events.json"

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
])

# ---------------------------------------------------------------------------
# Tạo SparkSession — jars được mount sẵn và truyền qua local classpath
# ---------------------------------------------------------------------------
def create_spark_session():
    return build_spark_session("BronzeBatchJob", pipeline_config=CONFIG)

# ---------------------------------------------------------------------------
# Quản lý offset: đọc/ghi JSON trên MinIO
# Format lưu trên MinIO: {"ecommerce_events": {"0": 500, "1": 300, "2": 200}}
# Format truyền cho Spark: json string của dict trên
# ---------------------------------------------------------------------------
def read_offsets(spark) -> dict:
    """
    Trả về dict {partition_int: offset_int} hoặc {} nếu chưa có file.
    """
    try:
        df    = spark.read.text(OFFSET_FILE)
        lines = [r[0] for r in df.collect()]
        raw   = json.loads("".join(lines))          # {"topic": {"part": offset}}
        part_map = raw.get(KAFKA_TOPIC, {})
        return {int(k): int(v) for k, v in part_map.items()}
    except Exception:
        return {}


def build_starting_offsets(offsets: dict) -> str:
    """
    Chuyển {0: 500, 1: 300} → '{"ecommerce_events":{"0":500,"1":300}}'
    Đây là format Spark Kafka connector yêu cầu.
    Trả về "earliest" nếu chưa có offset.
    """
    if not offsets:
        return "earliest"
    part_str = {str(k): v for k, v in offsets.items()}
    return json.dumps({KAFKA_TOPIC: part_str})


def write_offsets(spark, offsets: dict):
    """
    Ghi dict offset ra file JSON trên MinIO.
    """
    payload = json.dumps({KAFKA_TOPIC: {str(k): v for k, v in sorted(offsets.items())}})
    spark.createDataFrame([payload], StringType()) \
         .write.mode("overwrite").text(OFFSET_FILE)


# ---------------------------------------------------------------------------
# Transform: parse JSON, thêm metadata
# ---------------------------------------------------------------------------
def transform_bronze(raw_df):
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
    bronze = bronze.withColumn("ingested_at",     current_timestamp())
    bronze = bronze.withColumn("date_partition",  to_date(col("kafka_ts")))
    return bronze


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  Spark Bronze Batch Job")
    print(f"  Kafka  : {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")
    print(f"  Output : {OUTPUT_PATH}")
    print("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc offset hiện tại
    current_offsets  = read_offsets(spark)
    starting_offsets = build_starting_offsets(current_offsets)
    print(f"[Bronze] Starting offsets: {starting_offsets}")

    # 2. Đọc Kafka
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               KAFKA_TOPIC)
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
    bronze_df = transform_bronze(raw_df)
    bronze_df.write \
        .mode("append") \
        .partitionBy("date_partition") \
        .parquet(OUTPUT_PATH)

    row_count = raw_df.count()
    print(f"[Bronze] Written {row_count} rows to {OUTPUT_PATH}")

    # 5. Cập nhật offset (max_offset + 1 cho mỗi partition)
    max_offsets = {
        row["partition"]: row["max(offset)"] + 1
        for row in raw_df.groupBy("partition").max("offset").collect()
    }
    # Giữ lại partition cũ không có message mới
    for p, o in current_offsets.items():
        max_offsets.setdefault(p, o)

    write_offsets(spark, max_offsets)
    print(f"[Bronze] Updated offsets: {max_offsets}")

    raw_df.unpersist()
    print("[Bronze] Completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
