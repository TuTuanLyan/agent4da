"""Builders for MVP Gold tables."""

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

from common.data_quality import safe_divide


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
            spark_sum(when(col("event_type") == "view", 1).otherwise(0))
            .cast("long")
            .alias("total_views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0))
            .cast("long")
            .alias("total_carts"),
            spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0))
            .cast("long")
            .alias("total_remove_from_carts"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0))
            .cast("long")
            .alias("total_purchases"),
            countDistinct("user_id").cast("long").alias("unique_users"),
            countDistinct("user_session").cast("long").alias("unique_sessions"),
            countDistinct("product_id").cast("long").alias("unique_products"),
            spark_sum(purchase_amount).cast("decimal(18,2)").alias("total_revenue"),
            avg("price").cast("decimal(10,2)").alias("avg_event_price"),
        )
    )

    return (
        summary_df
        .withColumn("conversion_rate", safe_divide(col("total_purchases"), col("total_views")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("total_purchases"), col("total_carts")))
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


def build_mvp_outputs(base_df):
    return {
        "dim_time": build_dim_time_df(base_df),
        "dim_product": build_dim_product_df(base_df),
        "fact_events": build_fact_events_df(base_df),
        "fact_sales": build_fact_sales_df(base_df),
        "daily_event_summary": build_daily_event_summary_df(base_df),
    }

