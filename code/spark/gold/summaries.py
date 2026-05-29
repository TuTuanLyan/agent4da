"""Summary table transforms for Gold."""

from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    date_format,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
)

from gold.validators import assert_unique_key, require_columns, require_non_null


DAILY_EVENT_SUMMARY_COLUMNS = [
    "event_date",
    "total_events",
    "total_views",
    "total_carts",
    "total_remove_from_carts",
    "total_purchases",
    "unique_users",
    "unique_sessions",
    "unique_products",
    "unique_events",
    "total_revenue",
    "avg_event_price",
    "conversion_rate",
    "cart_to_purchase_rate",
    "gold_processed_at",
]

DAILY_PRODUCT_SUMMARY_COLUMNS = [
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
    "unique_events",
    "unique_users",
    "unique_sessions",
    "revenue",
    "avg_price",
    "min_price",
    "max_price",
    "conversion_rate",
    "cart_to_purchase_rate",
    "gold_processed_at",
]

DAILY_CATEGORY_SUMMARY_COLUMNS = [
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
    "unique_events",
    "unique_users",
    "unique_products",
    "revenue",
    "conversion_rate",
    "cart_to_purchase_rate",
    "gold_processed_at",
]

DAILY_BRAND_SUMMARY_COLUMNS = [
    "summary_id",
    "event_date",
    "brand",
    "view_count",
    "cart_count",
    "purchase_count",
    "remove_from_cart_count",
    "unique_events",
    "unique_users",
    "unique_products",
    "revenue",
    "conversion_rate",
    "cart_to_purchase_rate",
    "gold_processed_at",
]

REQUIRED_FACT_EVENTS_COLUMNS = [
    "event_fingerprint",
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
]

REQUIRED_FACT_SALES_COLUMNS = [
    "sale_date",
    "product_id",
    "gross_amount",
]

REQUIRED_DIM_PRODUCT_COLUMNS = [
    "product_id",
    "brand",
    "category_l1",
    "category_l2",
    "category_l3",
]

DAILY_EVENT_SUMMARY_SCHEMA_SQL = """
event_date DATE,
total_events BIGINT,
total_views BIGINT,
total_carts BIGINT,
total_remove_from_carts BIGINT,
total_purchases BIGINT,
unique_users BIGINT,
unique_sessions BIGINT,
unique_products BIGINT,
unique_events BIGINT,
total_revenue DECIMAL(18, 2),
avg_event_price DECIMAL(18, 2),
conversion_rate DOUBLE,
cart_to_purchase_rate DOUBLE,
gold_processed_at TIMESTAMP
""".strip()

DAILY_PRODUCT_SUMMARY_SCHEMA_SQL = """
summary_id STRING,
event_date DATE,
product_id BIGINT,
brand STRING,
category_l1 STRING,
category_l2 STRING,
category_l3 STRING,
view_count BIGINT,
cart_count BIGINT,
purchase_count BIGINT,
remove_from_cart_count BIGINT,
unique_events BIGINT,
unique_users BIGINT,
unique_sessions BIGINT,
revenue DECIMAL(18, 2),
avg_price DECIMAL(18, 2),
min_price DECIMAL(18, 2),
max_price DECIMAL(18, 2),
conversion_rate DOUBLE,
cart_to_purchase_rate DOUBLE,
gold_processed_at TIMESTAMP
""".strip()

DAILY_CATEGORY_SUMMARY_SCHEMA_SQL = """
summary_id STRING,
event_date DATE,
category_l1 STRING,
category_l2 STRING,
category_l3 STRING,
total_events BIGINT,
view_count BIGINT,
cart_count BIGINT,
purchase_count BIGINT,
remove_from_cart_count BIGINT,
unique_events BIGINT,
unique_users BIGINT,
unique_products BIGINT,
revenue DECIMAL(18, 2),
conversion_rate DOUBLE,
cart_to_purchase_rate DOUBLE,
gold_processed_at TIMESTAMP
""".strip()

DAILY_BRAND_SUMMARY_SCHEMA_SQL = """
summary_id STRING,
event_date DATE,
brand STRING,
view_count BIGINT,
cart_count BIGINT,
purchase_count BIGINT,
remove_from_cart_count BIGINT,
unique_events BIGINT,
unique_users BIGINT,
unique_products BIGINT,
revenue DECIMAL(18, 2),
conversion_rate DOUBLE,
cart_to_purchase_rate DOUBLE,
gold_processed_at TIMESTAMP
""".strip()


def validate_inputs(fact_events_df, fact_sales_df, dim_product_df):
    require_columns(fact_events_df, REQUIRED_FACT_EVENTS_COLUMNS, "fact_events")
    require_columns(fact_sales_df, REQUIRED_FACT_SALES_COLUMNS, "fact_sales")
    require_columns(dim_product_df, REQUIRED_DIM_PRODUCT_COLUMNS, "dim_product")


def bool_sum(column_name):
    return spark_sum(when(col(column_name) == lit(True), lit(1)).otherwise(lit(0)))


def safe_rate(numerator_col, denominator_col):
    return (
        when(denominator_col.isNull() | (denominator_col == lit(0)), lit(0.0))
        .otherwise(numerator_col.cast("double") / denominator_col.cast("double"))
    )


def money_zero():
    return lit(0).cast("decimal(18,2)")


def revenue_by_date(fact_sales_df):
    return (
        fact_sales_df
        .select(
            col("sale_date").cast("date").alias("event_date"),
            col("gross_amount").cast("decimal(18,2)").alias("gross_amount"),
        )
        .groupBy("event_date")
        .agg(spark_sum("gross_amount").cast("decimal(18,2)").alias("total_revenue"))
    )


def revenue_by_product(fact_sales_df):
    return (
        fact_sales_df
        .select(
            col("sale_date").cast("date").alias("event_date"),
            col("product_id").cast("bigint").alias("product_id"),
            col("gross_amount").cast("decimal(18,2)").alias("gross_amount"),
        )
        .groupBy("event_date", "product_id")
        .agg(spark_sum("gross_amount").cast("decimal(18,2)").alias("revenue"))
    )


def enrich_with_product(df, dim_product_df):
    product_dim = dim_product_df.select(
        col("product_id").cast("bigint").alias("product_id"),
        col("brand").cast("string").alias("brand"),
        col("category_l1").cast("string").alias("category_l1"),
        col("category_l2").cast("string").alias("category_l2"),
        col("category_l3").cast("string").alias("category_l3"),
    ).dropDuplicates(["product_id"])
    return df.join(product_dim, on="product_id", how="left")


def revenue_by_category(fact_sales_df, dim_product_df):
    return (
        enrich_with_product(fact_sales_df, dim_product_df)
        .select(
            col("sale_date").cast("date").alias("event_date"),
            coalesce(col("category_l1"), lit("unknown")).alias("category_l1"),
            coalesce(col("category_l2"), lit("unknown")).alias("category_l2"),
            coalesce(col("category_l3"), lit("unknown")).alias("category_l3"),
            col("gross_amount").cast("decimal(18,2)").alias("gross_amount"),
        )
        .groupBy("event_date", "category_l1", "category_l2", "category_l3")
        .agg(spark_sum("gross_amount").cast("decimal(18,2)").alias("revenue"))
    )


def revenue_by_brand(fact_sales_df, dim_product_df):
    return (
        enrich_with_product(fact_sales_df, dim_product_df)
        .select(
            col("sale_date").cast("date").alias("event_date"),
            coalesce(col("brand"), lit("unknown")).alias("brand"),
            col("gross_amount").cast("decimal(18,2)").alias("gross_amount"),
        )
        .groupBy("event_date", "brand")
        .agg(spark_sum("gross_amount").cast("decimal(18,2)").alias("revenue"))
    )


def build_daily_event_summary(fact_events_df, fact_sales_df):
    event_agg = (
        fact_events_df
        .groupBy("event_date")
        .agg(
            count(lit(1)).cast("bigint").alias("total_events"),
            bool_sum("is_view").cast("bigint").alias("total_views"),
            bool_sum("is_cart").cast("bigint").alias("total_carts"),
            bool_sum("is_remove_from_cart").cast("bigint").alias("total_remove_from_carts"),
            bool_sum("is_purchase").cast("bigint").alias("total_purchases"),
            countDistinct("user_id").cast("bigint").alias("unique_users"),
            countDistinct("session_id").cast("bigint").alias("unique_sessions"),
            countDistinct("product_id").cast("bigint").alias("unique_products"),
            countDistinct("event_fingerprint").cast("bigint").alias("unique_events"),
            avg("price").cast("decimal(18,2)").alias("avg_event_price"),
        )
    )
    revenue_agg = revenue_by_date(fact_sales_df)

    return (
        event_agg
        .join(revenue_agg, on="event_date", how="left")
        .select(
            col("event_date").cast("date").alias("event_date"),
            col("total_events"),
            col("total_views"),
            col("total_carts"),
            col("total_remove_from_carts"),
            col("total_purchases"),
            col("unique_users"),
            col("unique_sessions"),
            col("unique_products"),
            col("unique_events"),
            coalesce(col("total_revenue"), money_zero()).alias("total_revenue"),
            col("avg_event_price"),
            safe_rate(col("total_purchases"), col("total_views")).alias("conversion_rate"),
            safe_rate(col("total_purchases"), col("total_carts")).alias("cart_to_purchase_rate"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_daily_product_summary(fact_events_df, fact_sales_df, dim_product_df):
    enriched_events = enrich_with_product(fact_events_df, dim_product_df)
    event_agg = (
        enriched_events
        .groupBy(
            "event_date",
            "product_id",
            "brand",
            "category_l1",
            "category_l2",
            "category_l3",
        )
        .agg(
            bool_sum("is_view").cast("bigint").alias("view_count"),
            bool_sum("is_cart").cast("bigint").alias("cart_count"),
            bool_sum("is_purchase").cast("bigint").alias("purchase_count"),
            bool_sum("is_remove_from_cart").cast("bigint").alias("remove_from_cart_count"),
            countDistinct("event_fingerprint").cast("bigint").alias("unique_events"),
            countDistinct("user_id").cast("bigint").alias("unique_users"),
            countDistinct("session_id").cast("bigint").alias("unique_sessions"),
            avg("price").cast("decimal(18,2)").alias("avg_price"),
            spark_min("price").cast("decimal(18,2)").alias("min_price"),
            spark_max("price").cast("decimal(18,2)").alias("max_price"),
        )
    )
    revenue_agg = revenue_by_product(fact_sales_df)

    return (
        event_agg
        .join(revenue_agg, on=["event_date", "product_id"], how="left")
        .withColumn(
            "summary_id",
            concat_ws(
                "_",
                date_format(col("event_date"), "yyyyMMdd"),
                col("product_id").cast("string"),
            ),
        )
        .select(
            col("summary_id").cast("string").alias("summary_id"),
            col("event_date").cast("date").alias("event_date"),
            col("product_id").cast("bigint").alias("product_id"),
            col("brand").cast("string").alias("brand"),
            col("category_l1").cast("string").alias("category_l1"),
            col("category_l2").cast("string").alias("category_l2"),
            col("category_l3").cast("string").alias("category_l3"),
            col("view_count"),
            col("cart_count"),
            col("purchase_count"),
            col("remove_from_cart_count"),
            col("unique_events"),
            col("unique_users"),
            col("unique_sessions"),
            coalesce(col("revenue"), money_zero()).alias("revenue"),
            col("avg_price"),
            col("min_price"),
            col("max_price"),
            safe_rate(col("purchase_count"), col("view_count")).alias("conversion_rate"),
            safe_rate(col("purchase_count"), col("cart_count")).alias("cart_to_purchase_rate"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_daily_category_summary(fact_events_df, fact_sales_df, dim_product_df):
    enriched_events = (
        enrich_with_product(fact_events_df, dim_product_df)
        .withColumn("category_l1", coalesce(col("category_l1"), lit("unknown")))
        .withColumn("category_l2", coalesce(col("category_l2"), lit("unknown")))
        .withColumn("category_l3", coalesce(col("category_l3"), lit("unknown")))
    )
    event_agg = (
        enriched_events
        .groupBy("event_date", "category_l1", "category_l2", "category_l3")
        .agg(
            count(lit(1)).cast("bigint").alias("total_events"),
            bool_sum("is_view").cast("bigint").alias("view_count"),
            bool_sum("is_cart").cast("bigint").alias("cart_count"),
            bool_sum("is_purchase").cast("bigint").alias("purchase_count"),
            bool_sum("is_remove_from_cart").cast("bigint").alias("remove_from_cart_count"),
            countDistinct("event_fingerprint").cast("bigint").alias("unique_events"),
            countDistinct("user_id").cast("bigint").alias("unique_users"),
            countDistinct("product_id").cast("bigint").alias("unique_products"),
        )
    )
    revenue_agg = revenue_by_category(fact_sales_df, dim_product_df)

    return (
        event_agg
        .join(
            revenue_agg,
            on=["event_date", "category_l1", "category_l2", "category_l3"],
            how="left",
        )
        .withColumn(
            "summary_id",
            concat_ws(
                "_",
                date_format(col("event_date"), "yyyyMMdd"),
                col("category_l1"),
                col("category_l2"),
                col("category_l3"),
            ),
        )
        .select(
            col("summary_id").cast("string").alias("summary_id"),
            col("event_date").cast("date").alias("event_date"),
            col("category_l1").cast("string").alias("category_l1"),
            col("category_l2").cast("string").alias("category_l2"),
            col("category_l3").cast("string").alias("category_l3"),
            col("total_events"),
            col("view_count"),
            col("cart_count"),
            col("purchase_count"),
            col("remove_from_cart_count"),
            col("unique_events"),
            col("unique_users"),
            col("unique_products"),
            coalesce(col("revenue"), money_zero()).alias("revenue"),
            safe_rate(col("purchase_count"), col("view_count")).alias("conversion_rate"),
            safe_rate(col("purchase_count"), col("cart_count")).alias("cart_to_purchase_rate"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def build_daily_brand_summary(fact_events_df, fact_sales_df, dim_product_df):
    enriched_events = enrich_with_product(fact_events_df, dim_product_df).withColumn(
        "brand",
        coalesce(col("brand"), lit("unknown")),
    )
    event_agg = (
        enriched_events
        .groupBy("event_date", "brand")
        .agg(
            bool_sum("is_view").cast("bigint").alias("view_count"),
            bool_sum("is_cart").cast("bigint").alias("cart_count"),
            bool_sum("is_purchase").cast("bigint").alias("purchase_count"),
            bool_sum("is_remove_from_cart").cast("bigint").alias("remove_from_cart_count"),
            countDistinct("event_fingerprint").cast("bigint").alias("unique_events"),
            countDistinct("user_id").cast("bigint").alias("unique_users"),
            countDistinct("product_id").cast("bigint").alias("unique_products"),
        )
    )
    revenue_agg = revenue_by_brand(fact_sales_df, dim_product_df)

    return (
        event_agg
        .join(revenue_agg, on=["event_date", "brand"], how="left")
        .withColumn(
            "summary_id",
            concat_ws(
                "_",
                date_format(col("event_date"), "yyyyMMdd"),
                col("brand"),
            ),
        )
        .select(
            col("summary_id").cast("string").alias("summary_id"),
            col("event_date").cast("date").alias("event_date"),
            col("brand").cast("string").alias("brand"),
            col("view_count"),
            col("cart_count"),
            col("purchase_count"),
            col("remove_from_cart_count"),
            col("unique_events"),
            col("unique_users"),
            col("unique_products"),
            coalesce(col("revenue"), money_zero()).alias("revenue"),
            safe_rate(col("purchase_count"), col("view_count")).alias("conversion_rate"),
            safe_rate(col("purchase_count"), col("cart_count")).alias("cart_to_purchase_rate"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def validate_daily_event_summary(summary_df, fact_events_df, fact_sales_df, table_name):
    assert_unique_key(summary_df, "event_date", table_name)

    expected_events = (
        fact_events_df
        .groupBy("event_date")
        .agg(
            count(lit(1)).cast("bigint").alias("expected_total_events"),
            bool_sum("is_purchase").cast("bigint").alias("expected_total_purchases"),
        )
    )
    event_mismatches = (
        summary_df
        .select("event_date", "total_events", "total_purchases")
        .join(expected_events, on="event_date", how="full_outer")
        .where(
            (coalesce(col("total_events"), lit(-1)) != coalesce(col("expected_total_events"), lit(-1)))
            | (
                coalesce(col("total_purchases"), lit(-1))
                != coalesce(col("expected_total_purchases"), lit(-1))
            )
        )
        .count()
    )
    if event_mismatches > 0:
        raise RuntimeError(
            f"{table_name} daily event counts do not match fact_events; "
            f"mismatch_dates={event_mismatches}."
        )

    expected_revenue = revenue_by_date(fact_sales_df).withColumnRenamed(
        "total_revenue",
        "expected_total_revenue",
    )
    revenue_mismatches = (
        summary_df
        .select("event_date", "total_revenue")
        .join(expected_revenue, on="event_date", how="full_outer")
        .where(
            coalesce(col("total_revenue"), money_zero())
            != coalesce(col("expected_total_revenue"), money_zero())
        )
        .count()
    )
    if revenue_mismatches > 0:
        raise RuntimeError(
            f"{table_name} total_revenue does not match fact_sales; "
            f"mismatch_dates={revenue_mismatches}."
        )


def validate_summary_id(summary_df, table_name):
    require_non_null(summary_df, "summary_id", table_name)
    assert_unique_key(summary_df, "summary_id", table_name)
