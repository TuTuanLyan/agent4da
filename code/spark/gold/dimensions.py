"""Dimension table transforms for Gold."""

from pyspark.sql.functions import (
    avg,
    coalesce,
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
    unix_timestamp,
    when,
)

from gold.validators import require_columns


DIM_TIME_COLUMNS = [
    "time_id",
    "event_date",
    "event_year",
    "event_month",
    "event_day",
    "event_hour",
    "day_of_week",
    "day_name",
    "month_name",
    "quarter",
    "is_weekend",
    "created_at",
    "updated_at",
]

DIM_PRODUCT_COLUMNS = [
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
    "created_at",
    "updated_at",
]

DIM_USER_COLUMNS = [
    "user_id",
    "first_seen_at",
    "last_seen_at",
    "total_sessions",
    "total_events",
    "total_views",
    "total_cart_adds",
    "total_remove_from_carts",
    "total_purchases",
    "total_revenue",
    "created_at",
    "updated_at",
]

DIM_SESSION_COLUMNS = [
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
    "session_revenue",
    "has_purchase",
    "created_at",
    "updated_at",
]

REQUIRED_STAGING_COLUMNS = [
    "time_id",
    "event_date",
    "event_year",
    "event_month",
    "event_day",
    "event_hour",
    "event_ts",
    "product_id",
    "category_id",
    "category_code",
    "category_l1",
    "category_l2",
    "category_l3",
    "brand",
    "price",
]

REQUIRED_FACT_EVENTS_COLUMNS = [
    "event_ts",
    "event_type",
    "product_id",
    "user_id",
    "session_id",
    "price",
]

REQUIRED_FACT_SALES_COLUMNS = [
    "sale_id",
    "user_id",
    "session_id",
    "gross_amount",
]

DIM_TIME_SCHEMA_SQL = """
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
""".strip()

DIM_PRODUCT_SCHEMA_SQL = """
product_id BIGINT,
category_id BIGINT,
category_code STRING,
category_l1 STRING,
category_l2 STRING,
category_l3 STRING,
brand STRING,
first_seen_at TIMESTAMP,
last_seen_at TIMESTAMP,
avg_observed_price DECIMAL(18, 2),
min_observed_price DECIMAL(18, 2),
max_observed_price DECIMAL(18, 2),
record_count BIGINT,
created_at TIMESTAMP,
updated_at TIMESTAMP
""".strip()

DIM_USER_SCHEMA_SQL = """
user_id BIGINT,
first_seen_at TIMESTAMP,
last_seen_at TIMESTAMP,
total_sessions BIGINT,
total_events BIGINT,
total_views BIGINT,
total_cart_adds BIGINT,
total_remove_from_carts BIGINT,
total_purchases BIGINT,
total_revenue DECIMAL(18, 2),
created_at TIMESTAMP,
updated_at TIMESTAMP
""".strip()

DIM_SESSION_SCHEMA_SQL = """
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
session_revenue DECIMAL(18, 2),
has_purchase BOOLEAN,
created_at TIMESTAMP,
updated_at TIMESTAMP
""".strip()


def bool_count(event_type):
    return spark_sum(when(col("event_type") == lit(event_type), lit(1)).otherwise(lit(0)))


def validate_inputs(staging_df, fact_events_df, fact_sales_df):
    require_columns(staging_df, REQUIRED_STAGING_COLUMNS, "staging events")
    require_columns(fact_events_df, REQUIRED_FACT_EVENTS_COLUMNS, "fact_events")
    require_columns(fact_sales_df, REQUIRED_FACT_SALES_COLUMNS, "fact_sales")


def build_dim_time(staging_df):
    return (
        staging_df
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
            col("time_id").cast("string").alias("time_id"),
            col("event_date").cast("date").alias("event_date"),
            col("event_year").cast("int").alias("event_year"),
            col("event_month").cast("int").alias("event_month"),
            col("event_day").cast("int").alias("event_day"),
            col("event_hour").cast("int").alias("event_hour"),
            dayofweek(col("event_ts")).cast("int").alias("day_of_week"),
            date_format(col("event_ts"), "EEEE").alias("day_name"),
            date_format(col("event_ts"), "MMMM").alias("month_name"),
            quarter(col("event_ts")).cast("int").alias("quarter"),
            dayofweek(col("event_ts")).isin(1, 7).alias("is_weekend"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def build_dim_product(staging_df):
    return (
        staging_df
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
            avg("price").cast("decimal(18,2)").alias("avg_observed_price"),
            spark_min("price").cast("decimal(18,2)").alias("min_observed_price"),
            spark_max("price").cast("decimal(18,2)").alias("max_observed_price"),
            count(lit(1)).cast("bigint").alias("record_count"),
        )
        .select(
            col("product_id").cast("bigint").alias("product_id"),
            col("category_id").cast("bigint").alias("category_id"),
            col("category_code").cast("string").alias("category_code"),
            col("category_l1").cast("string").alias("category_l1"),
            col("category_l2").cast("string").alias("category_l2"),
            col("category_l3").cast("string").alias("category_l3"),
            col("brand").cast("string").alias("brand"),
            col("first_seen_at").cast("timestamp").alias("first_seen_at"),
            col("last_seen_at").cast("timestamp").alias("last_seen_at"),
            col("avg_observed_price").alias("avg_observed_price"),
            col("min_observed_price").alias("min_observed_price"),
            col("max_observed_price").alias("max_observed_price"),
            col("record_count").alias("record_count"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def build_dim_user(fact_events_df, fact_sales_df):
    event_agg = (
        fact_events_df
        .groupBy("user_id")
        .agg(
            spark_min("event_ts").alias("first_seen_at"),
            spark_max("event_ts").alias("last_seen_at"),
            countDistinct("session_id").cast("bigint").alias("total_sessions"),
            count(lit(1)).cast("bigint").alias("total_events"),
            bool_count("view").cast("bigint").alias("total_views"),
            bool_count("cart").cast("bigint").alias("total_cart_adds"),
            bool_count("remove_from_cart").cast("bigint").alias("total_remove_from_carts"),
            bool_count("purchase").cast("bigint").alias("total_purchases"),
        )
    )
    sales_agg = (
        fact_sales_df
        .groupBy("user_id")
        .agg(spark_sum("gross_amount").cast("decimal(18,2)").alias("total_revenue"))
    )

    return (
        event_agg
        .join(sales_agg, on="user_id", how="left")
        .select(
            col("user_id").cast("bigint").alias("user_id"),
            col("first_seen_at"),
            col("last_seen_at"),
            col("total_sessions"),
            col("total_events"),
            col("total_views"),
            col("total_cart_adds"),
            col("total_remove_from_carts"),
            col("total_purchases"),
            coalesce(col("total_revenue"), lit(0).cast("decimal(18,2)")).alias("total_revenue"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def build_dim_session(fact_events_df, fact_sales_df):
    event_agg = (
        fact_events_df
        .groupBy("session_id")
        .agg(
            first("user_id", ignorenulls=True).alias("user_id"),
            spark_min("event_ts").alias("session_start_at"),
            spark_max("event_ts").alias("session_end_at"),
            count(lit(1)).cast("bigint").alias("event_count"),
            bool_count("view").cast("bigint").alias("view_count"),
            bool_count("cart").cast("bigint").alias("cart_count"),
            bool_count("remove_from_cart").cast("bigint").alias("remove_from_cart_count"),
            bool_count("purchase").cast("bigint").alias("purchase_count"),
        )
    )
    sales_agg = (
        fact_sales_df
        .groupBy("session_id")
        .agg(spark_sum("gross_amount").cast("decimal(18,2)").alias("session_revenue"))
    )

    return (
        event_agg
        .join(sales_agg, on="session_id", how="left")
        .select(
            col("session_id").cast("string").alias("session_id"),
            col("user_id").cast("bigint").alias("user_id"),
            col("session_start_at"),
            col("session_end_at"),
            (
                unix_timestamp(col("session_end_at"))
                - unix_timestamp(col("session_start_at"))
            ).cast("bigint").alias("session_duration_sec"),
            col("event_count"),
            col("view_count"),
            col("cart_count"),
            col("remove_from_cart_count"),
            col("purchase_count"),
            coalesce(col("session_revenue"), lit(0).cast("decimal(18,2)")).alias("session_revenue"),
            (col("purchase_count") > lit(0)).alias("has_purchase"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )
