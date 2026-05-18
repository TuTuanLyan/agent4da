"""Builders for extended Gold tables."""

from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    date_format,
    first,
    lit,
    max as spark_max,
    min as spark_min,
    sha2,
    sum as spark_sum,
    unix_timestamp,
    when,
)

from common.data_quality import bool_sum, safe_divide


def sales_by_user(fact_sales_df):
    return fact_sales_df.groupBy("user_id").agg(
        spark_sum("gross_amount").cast("decimal(18,2)").alias("total_revenue")
    )


def sales_by_session(fact_sales_df):
    return fact_sales_df.groupBy("session_id").agg(
        spark_sum("gross_amount").cast("decimal(18,2)").alias("session_revenue")
    )


def build_dim_user_df(fact_events_df, fact_sales_df):
    event_agg = fact_events_df.groupBy("user_id").agg(
        spark_min("event_ts").alias("first_seen_at"),
        spark_max("event_ts").alias("last_seen_at"),
        countDistinct("session_id").cast("long").alias("total_sessions"),
        count(lit(1)).cast("long").alias("total_events"),
        bool_sum("is_view").alias("total_views"),
        bool_sum("is_cart").alias("total_cart_adds"),
        bool_sum("is_remove_from_cart").alias("total_remove_from_carts"),
        bool_sum("is_purchase").alias("total_purchases"),
    )

    return (
        event_agg
        .join(sales_by_user(fact_sales_df), on="user_id", how="left")
        .select(
            "user_id",
            "first_seen_at",
            "last_seen_at",
            "total_sessions",
            "total_events",
            "total_views",
            "total_cart_adds",
            "total_remove_from_carts",
            "total_purchases",
            coalesce(col("total_revenue"), lit(0).cast("decimal(18,2)")).alias("total_revenue"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def build_dim_session_df(fact_events_df, fact_sales_df):
    event_agg = fact_events_df.groupBy("session_id").agg(
        first("user_id", ignorenulls=True).alias("user_id"),
        spark_min("event_ts").alias("session_start_at"),
        spark_max("event_ts").alias("session_end_at"),
        count(lit(1)).cast("long").alias("event_count"),
        bool_sum("is_view").alias("view_count"),
        bool_sum("is_cart").alias("cart_count"),
        bool_sum("is_remove_from_cart").alias("remove_from_cart_count"),
        bool_sum("is_purchase").alias("purchase_count"),
    )

    return (
        event_agg
        .join(sales_by_session(fact_sales_df), on="session_id", how="left")
        .withColumn(
            "session_duration_sec",
            (unix_timestamp("session_end_at") - unix_timestamp("session_start_at")).cast("long"),
        )
        .select(
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
            coalesce(col("session_revenue"), lit(0).cast("decimal(18,2)")).alias("session_revenue"),
            (col("purchase_count") > lit(0)).alias("has_purchase"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
    )


def product_enriched_events(fact_events_df, dim_product_df):
    product_lookup = (
        dim_product_df
        .groupBy("product_id")
        .agg(
            first("brand", ignorenulls=True).alias("brand"),
            first("category_l1", ignorenulls=True).alias("category_l1"),
            first("category_l2", ignorenulls=True).alias("category_l2"),
            first("category_l3", ignorenulls=True).alias("category_l3"),
        )
    )
    return fact_events_df.join(product_lookup, on="product_id", how="left")


def revenue_expr():
    return when(col("is_purchase"), col("price").cast("decimal(18,2)")).otherwise(
        lit(0).cast("decimal(18,2)")
    )


def build_daily_product_summary_df(fact_events_df, dim_product_df):
    grouped = product_enriched_events(fact_events_df, dim_product_df).groupBy(
        "event_date",
        "product_id",
        "brand",
        "category_l1",
        "category_l2",
        "category_l3",
    ).agg(
        bool_sum("is_view").alias("view_count"),
        bool_sum("is_cart").alias("cart_count"),
        bool_sum("is_purchase").alias("purchase_count"),
        bool_sum("is_remove_from_cart").alias("remove_from_cart_count"),
        countDistinct("user_id").cast("long").alias("unique_users"),
        countDistinct("session_id").cast("long").alias("unique_sessions"),
        spark_sum(revenue_expr()).cast("decimal(18,2)").alias("revenue"),
        avg("price").cast("decimal(10,2)").alias("avg_price"),
        spark_min("price").cast("decimal(10,2)").alias("min_price"),
        spark_max("price").cast("decimal(10,2)").alias("max_price"),
    )

    return (
        grouped
        .withColumn(
            "summary_id",
            concat_ws("_", date_format("event_date", "yyyyMMdd"), col("product_id").cast("string")),
        )
        .withColumn("conversion_rate", safe_divide(col("purchase_count"), col("view_count")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("purchase_count"), col("cart_count")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "summary_id",
            "event_date",
            "product_id",
            "brand",
            "category_l1",
            "category_l2",
            "category_l3",
            "view_count",
            "cart_count",
            "purchase_count",
            "remove_from_cart_count",
            "unique_users",
            "unique_sessions",
            "revenue",
            "avg_price",
            "min_price",
            "max_price",
            "conversion_rate",
            "cart_to_purchase_rate",
            "gold_processed_at",
        )
    )


def build_daily_category_summary_df(fact_events_df, dim_product_df):
    joined = (
        product_enriched_events(fact_events_df, dim_product_df)
        .withColumn("category_l1", coalesce(col("category_l1"), lit("unknown")))
        .withColumn("category_l2", coalesce(col("category_l2"), lit("unknown")))
        .withColumn("category_l3", coalesce(col("category_l3"), lit("unknown")))
    )

    grouped = joined.groupBy("event_date", "category_l1", "category_l2", "category_l3").agg(
        count(lit(1)).cast("long").alias("total_events"),
        bool_sum("is_view").alias("view_count"),
        bool_sum("is_cart").alias("cart_count"),
        bool_sum("is_purchase").alias("purchase_count"),
        bool_sum("is_remove_from_cart").alias("remove_from_cart_count"),
        countDistinct("user_id").cast("long").alias("unique_users"),
        countDistinct("session_id").cast("long").alias("unique_sessions"),
        countDistinct("product_id").cast("long").alias("unique_products"),
        spark_sum(revenue_expr()).cast("decimal(18,2)").alias("revenue"),
    )

    return (
        grouped
        .withColumn(
            "summary_id",
            sha2(
                concat_ws(
                    "||",
                    col("event_date").cast("string"),
                    "category_l1",
                    "category_l2",
                    "category_l3",
                ),
                256,
            ),
        )
        .withColumn("conversion_rate", safe_divide(col("purchase_count"), col("view_count")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("purchase_count"), col("cart_count")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "summary_id",
            "event_date",
            "category_l1",
            "category_l2",
            "category_l3",
            "total_events",
            "view_count",
            "cart_count",
            "purchase_count",
            "remove_from_cart_count",
            "unique_users",
            "unique_sessions",
            "unique_products",
            "revenue",
            "conversion_rate",
            "cart_to_purchase_rate",
            "gold_processed_at",
        )
    )


def build_daily_brand_summary_df(fact_events_df, dim_product_df):
    joined = product_enriched_events(fact_events_df, dim_product_df).withColumn(
        "brand", coalesce(col("brand"), lit("unknown"))
    )

    grouped = joined.groupBy("event_date", "brand").agg(
        bool_sum("is_view").alias("view_count"),
        bool_sum("is_cart").alias("cart_count"),
        bool_sum("is_purchase").alias("purchase_count"),
        bool_sum("is_remove_from_cart").alias("remove_from_cart_count"),
        countDistinct("user_id").cast("long").alias("unique_users"),
        countDistinct("session_id").cast("long").alias("unique_sessions"),
        countDistinct("product_id").cast("long").alias("unique_products"),
        spark_sum(revenue_expr()).cast("decimal(18,2)").alias("revenue"),
    )

    return (
        grouped
        .withColumn(
            "summary_id",
            sha2(concat_ws("||", col("event_date").cast("string"), "brand"), 256),
        )
        .withColumn("conversion_rate", safe_divide(col("purchase_count"), col("view_count")))
        .withColumn("cart_to_purchase_rate", safe_divide(col("purchase_count"), col("cart_count")))
        .withColumn("gold_processed_at", current_timestamp())
        .select(
            "summary_id",
            "event_date",
            "brand",
            "view_count",
            "cart_count",
            "purchase_count",
            "remove_from_cart_count",
            "unique_users",
            "unique_sessions",
            "unique_products",
            "revenue",
            "conversion_rate",
            "cart_to_purchase_rate",
            "gold_processed_at",
        )
    )


def build_extended_outputs(mvp_outputs):
    fact_events_df = mvp_outputs["fact_events"].cache()
    fact_sales_df = mvp_outputs["fact_sales"].cache()
    dim_product_df = mvp_outputs["dim_product"].cache()

    try:
        return {
            "dim_user": build_dim_user_df(fact_events_df, fact_sales_df),
            "dim_session": build_dim_session_df(fact_events_df, fact_sales_df),
            "daily_product_summary": build_daily_product_summary_df(
                fact_events_df,
                dim_product_df,
            ),
            "daily_category_summary": build_daily_category_summary_df(
                fact_events_df,
                dim_product_df,
            ),
            "daily_brand_summary": build_daily_brand_summary_df(
                fact_events_df,
                dim_product_df,
            ),
        }
    finally:
        fact_events_df.unpersist()
        fact_sales_df.unpersist()
        dim_product_df.unpersist()

