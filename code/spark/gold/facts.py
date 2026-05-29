"""Fact table transforms for Gold."""

from pyspark.sql.functions import col, current_timestamp, lit

from gold.validators import assert_count_equal, require_columns


REQUIRED_STAGING_COLUMNS = [
    "event_fingerprint",
    "source_event_id",
    "time_id",
    "event_ts",
    "event_date",
    "event_type",
    "product_id",
    "user_id",
    "session_id",
    "price",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
    "silver_processed_at",
    "gold_processed_at",
]

FACT_EVENTS_COLUMNS = [
    "event_fingerprint",
    "source_event_id",
    "time_id",
    "event_ts",
    "event_date",
    "event_type",
    "product_id",
    "user_id",
    "session_id",
    "price",
    "is_view",
    "is_cart",
    "is_remove_from_cart",
    "is_purchase",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
    "silver_processed_at",
    "gold_processed_at",
]

FACT_SALES_COLUMNS = [
    "sale_id",
    "event_fingerprint",
    "source_event_id",
    "time_id",
    "sale_ts",
    "sale_date",
    "product_id",
    "user_id",
    "session_id",
    "unit_price",
    "quantity",
    "gross_amount",
    "gold_processed_at",
]

FACT_EVENTS_SCHEMA_SQL = """
event_fingerprint STRING,
source_event_id STRING,
time_id STRING,
event_ts TIMESTAMP,
event_date DATE,
event_type STRING,
product_id BIGINT,
user_id BIGINT,
session_id STRING,
price DECIMAL(18, 2),
is_view BOOLEAN,
is_cart BOOLEAN,
is_remove_from_cart BOOLEAN,
is_purchase BOOLEAN,
kafka_partition INT,
kafka_offset BIGINT,
kafka_ts TIMESTAMP,
silver_processed_at TIMESTAMP,
gold_processed_at TIMESTAMP
""".strip()

FACT_SALES_SCHEMA_SQL = """
sale_id STRING,
event_fingerprint STRING,
source_event_id STRING,
time_id STRING,
sale_ts TIMESTAMP,
sale_date DATE,
product_id BIGINT,
user_id BIGINT,
session_id STRING,
unit_price DECIMAL(18, 2),
quantity INT,
gross_amount DECIMAL(18, 2),
gold_processed_at TIMESTAMP
""".strip()


def validate_staging_columns(staging_df):
    require_columns(staging_df, REQUIRED_STAGING_COLUMNS, "staging events")


def build_fact_events(staging_df):
    validate_staging_columns(staging_df)
    return staging_df.select(
        col("event_fingerprint").cast("string").alias("event_fingerprint"),
        col("source_event_id").cast("string").alias("source_event_id"),
        col("time_id").cast("string").alias("time_id"),
        col("event_ts").cast("timestamp").alias("event_ts"),
        col("event_date").cast("date").alias("event_date"),
        col("event_type").cast("string").alias("event_type"),
        col("product_id").cast("bigint").alias("product_id"),
        col("user_id").cast("bigint").alias("user_id"),
        col("session_id").cast("string").alias("session_id"),
        col("price").cast("decimal(18,2)").alias("price"),
        (col("event_type") == lit("view")).alias("is_view"),
        (col("event_type") == lit("cart")).alias("is_cart"),
        (col("event_type") == lit("remove_from_cart")).alias("is_remove_from_cart"),
        (col("event_type") == lit("purchase")).alias("is_purchase"),
        col("kafka_partition").cast("int").alias("kafka_partition"),
        col("kafka_offset").cast("bigint").alias("kafka_offset"),
        col("kafka_ts").cast("timestamp").alias("kafka_ts"),
        col("silver_processed_at").cast("timestamp").alias("silver_processed_at"),
        current_timestamp().alias("gold_processed_at"),
    )


def build_fact_sales(fact_events_df):
    return (
        fact_events_df
        .where(col("event_type") == lit("purchase"))
        .select(
            col("event_fingerprint").cast("string").alias("sale_id"),
            col("event_fingerprint").cast("string").alias("event_fingerprint"),
            col("source_event_id").cast("string").alias("source_event_id"),
            col("time_id").cast("string").alias("time_id"),
            col("event_ts").cast("timestamp").alias("sale_ts"),
            col("event_date").cast("date").alias("sale_date"),
            col("product_id").cast("bigint").alias("product_id"),
            col("user_id").cast("bigint").alias("user_id"),
            col("session_id").cast("string").alias("session_id"),
            col("price").cast("decimal(18,2)").alias("unit_price"),
            lit(1).cast("int").alias("quantity"),
            (col("price").cast("decimal(18,2)") * lit(1))
            .cast("decimal(18,2)")
            .alias("gross_amount"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def validate_fact_outputs(fact_events_df, fact_sales_df):
    event_count = fact_events_df.count()
    null_fingerprint_count = fact_events_df.where(
        col("event_fingerprint").isNull()
    ).count()
    distinct_fingerprint_count = fact_events_df.select("event_fingerprint").distinct().count()
    purchase_count = fact_events_df.where(col("event_type") == lit("purchase")).count()
    sales_count = fact_sales_df.count()

    if null_fingerprint_count > 0:
        raise RuntimeError(
            f"fact_events has {null_fingerprint_count} null event_fingerprint rows."
        )

    if distinct_fingerprint_count != event_count:
        raise RuntimeError(
            "fact_events is not unique by event_fingerprint; "
            f"rows={event_count}, distinct={distinct_fingerprint_count}."
        )

    assert_count_equal(
        sales_count,
        purchase_count,
        "fact_sales count does not match purchase count from fact_events",
    )

    return {
        "fact_events": event_count,
        "fact_sales": sales_count,
        "purchases": purchase_count,
    }
