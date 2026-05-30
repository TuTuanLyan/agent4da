"""Staging transforms for Gold events."""

from pyspark.sql.functions import col, current_timestamp, date_format, lit, row_number
from pyspark.sql.window import Window

from gold.validators import require_columns


REQUIRED_SILVER_COLUMNS = [
    "event_fingerprint",
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
    "silver_processed_at",
    "is_valid",
]

STAGING_COLUMNS = [
    "event_fingerprint",
    "source_event_id",
    "time_id",
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
    "session_id",
    "kafka_ts",
    "kafka_partition",
    "kafka_offset",
    "silver_processed_at",
    "gold_processed_at",
]

STAGING_SCHEMA_SQL = """
event_fingerprint STRING,
source_event_id STRING,
time_id STRING,
event_ts TIMESTAMP,
event_date DATE,
event_year INT,
event_month INT,
event_day INT,
event_hour INT,
event_type STRING,
product_id BIGINT,
category_id BIGINT,
category_code STRING,
category_l1 STRING,
category_l2 STRING,
category_l3 STRING,
brand STRING,
price DECIMAL(18, 2),
user_id BIGINT,
session_id STRING,
kafka_ts TIMESTAMP,
kafka_partition INT,
kafka_offset BIGINT,
silver_processed_at TIMESTAMP,
gold_processed_at TIMESTAMP
""".strip()


def validate_silver_columns(silver_df):
    require_columns(silver_df, REQUIRED_SILVER_COLUMNS, "Silver events")


def filter_valid_events(silver_df):
    return silver_df.where(
        (col("is_valid") == lit(True)) & col("event_fingerprint").isNotNull()
    )


def deduplicate_by_fingerprint(events_df):
    order_columns = []
    if "bronze_ingested_at" in events_df.columns:
        order_columns.append(col("bronze_ingested_at").desc_nulls_last())

    order_columns.extend(
        [
            col("kafka_ts").desc_nulls_last(),
            col("kafka_partition").desc_nulls_last(),
            col("kafka_offset").desc_nulls_last(),
        ]
    )

    window = Window.partitionBy("event_fingerprint").orderBy(*order_columns)
    return (
        events_df
        .withColumn("_gold_row_number", row_number().over(window))
        .where(col("_gold_row_number") == lit(1))
        .drop("_gold_row_number")
    )


def select_staging_columns(dedup_df):
    return dedup_df.select(
        col("event_fingerprint").cast("string").alias("event_fingerprint"),
        col("source_event_id").cast("string").alias("source_event_id"),
        date_format(col("event_ts").cast("timestamp"), "yyyyMMddHH").alias("time_id"),
        col("event_ts").cast("timestamp").alias("event_ts"),
        col("event_date").cast("date").alias("event_date"),
        col("event_year").cast("int").alias("event_year"),
        col("event_month").cast("int").alias("event_month"),
        col("event_day").cast("int").alias("event_day"),
        col("event_hour").cast("int").alias("event_hour"),
        col("event_type").cast("string").alias("event_type"),
        col("product_id").cast("bigint").alias("product_id"),
        col("category_id").cast("bigint").alias("category_id"),
        col("category_code").cast("string").alias("category_code"),
        col("category_l1").cast("string").alias("category_l1"),
        col("category_l2").cast("string").alias("category_l2"),
        col("category_l3").cast("string").alias("category_l3"),
        col("brand").cast("string").alias("brand"),
        col("price").cast("decimal(18,2)").alias("price"),
        col("user_id").cast("bigint").alias("user_id"),
        col("user_session").cast("string").alias("session_id"),
        col("kafka_ts").cast("timestamp").alias("kafka_ts"),
        col("kafka_partition").cast("int").alias("kafka_partition"),
        col("kafka_offset").cast("bigint").alias("kafka_offset"),
        col("silver_processed_at").cast("timestamp").alias("silver_processed_at"),
        current_timestamp().alias("gold_processed_at"),
    )
