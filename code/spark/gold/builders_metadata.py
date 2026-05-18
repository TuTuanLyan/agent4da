"""Builders for Agent-facing metadata catalog tables."""

from pyspark.sql.functions import current_timestamp


def metadata_df_from_rows(spark, columns, rows):
    return (
        spark.createDataFrame(rows, columns)
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )


def build_metadata_table_catalog_df(spark, config):
    rows = [
        ("gold.dim_time", "gold", "dimension", "Time", "Hourly time dimension for ecommerce events.", "one row per event hour", "time_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.dim_product", "gold", "dimension", "Product", "Product category, brand, and observed price statistics.", "one row per product", "product_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.fact_events", "gold", "fact", "Events", "Clean ecommerce event fact table.", "one row per clean ecommerce event", "event_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.fact_sales", "gold", "fact", "Sales", "Purchase event fact table.", "one row per purchase event", "sale_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.daily_event_summary", "gold", "summary", "Daily Event Summary", "Daily funnel and revenue summary.", "one row per event date", "event_date", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.dim_user", "gold", "dimension", "User", "User behavior dimension.", "one row per user", "user_id", "Iceberg", "Spark", True, False, "per Gold run", "agent4da"),
        ("gold.dim_session", "gold", "dimension", "Session", "Session behavior dimension.", "one row per user session", "session_id", "Iceberg", "Spark", True, False, "per Gold run", "agent4da"),
        ("gold.daily_product_summary", "gold", "summary", "Daily Product Summary", "Daily product performance summary.", "one row per event date and product", "summary_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.daily_category_summary", "gold", "summary", "Daily Category Summary", "Daily category hierarchy performance summary.", "one row per event date and category hierarchy", "summary_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("gold.daily_brand_summary", "gold", "summary", "Daily Brand Summary", "Daily brand performance summary.", "one row per event date and brand", "summary_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.table_catalog", "metadata", "semantic_catalog", "Table Catalog", "Agent-facing catalog of tables.", "one row per table", "table_name", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.column_catalog", "metadata", "semantic_catalog", "Column Catalog", "Agent-facing catalog of important columns.", "one row per important column", "column_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.metric_catalog", "metadata", "semantic_catalog", "Metric Catalog", "Agent-facing catalog of metrics and formulas.", "one row per metric", "metric_name", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
        ("metadata.join_catalog", "metadata", "semantic_catalog", "Join Catalog", "Agent-facing catalog of supported joins.", "one row per join relationship", "join_id", "Iceberg", "Spark", True, True, "per Gold run", "agent4da"),
    ]
    columns = [
        "table_name",
        "layer",
        "table_type",
        "business_name",
        "description",
        "grain",
        "primary_key",
        "storage_format",
        "query_engine",
        "is_agent_visible",
        "recommended_for_agent",
        "refresh_frequency",
        "owner",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def column_row(
    table,
    column_name,
    data_type,
    business_name,
    description,
    source_table,
    source_column,
    logic,
    nullable,
    is_dimension,
    is_metric,
    is_time_column,
    is_join_key,
    examples,
    allowed,
    synonyms,
):
    return (
        f"{table}.{column_name}",
        table,
        column_name,
        data_type,
        business_name,
        description,
        source_table,
        source_column,
        logic,
        nullable,
        is_dimension,
        is_metric,
        is_time_column,
        is_join_key,
        examples,
        allowed,
        synonyms,
    )


def build_metadata_column_catalog_df(spark, config):
    rows = [
        column_row("gold.fact_events", "event_date", "DATE", "Event Date", "Calendar date of the event.", "silver.ecommerce_events", "event_date", "cast to date", False, True, False, True, False, "2019-10-01", "", "date,event day,ngay su kien"),
        column_row("gold.fact_events", "event_type", "STRING", "Event Type", "Type of ecommerce interaction.", "silver.ecommerce_events", "event_type", "clean valid event type", False, True, False, False, False, "view,cart,purchase", "view,cart,remove_from_cart,purchase", "event,action,hanh vi"),
        column_row("gold.fact_events", "product_id", "BIGINT", "Product ID", "Product identifier.", "silver.ecommerce_events", "product_id", "cast to bigint", False, True, False, False, True, "1005003", "", "product,san pham"),
        column_row("gold.fact_events", "user_id", "BIGINT", "User ID", "User identifier.", "silver.ecommerce_events", "user_id", "cast to bigint", False, True, False, False, True, "5128042", "", "user,customer,khach hang"),
        column_row("gold.fact_events", "session_id", "STRING", "Session ID", "User session identifier.", "silver.ecommerce_events", "user_session", "rename user_session to session_id", False, True, False, False, True, "abc-session", "", "session,phien"),
        column_row("gold.fact_events", "time_id", "STRING", "Time ID", "Hourly time key.", "silver.ecommerce_events", "event_ts", "date_format(event_ts, yyyyMMddHH)", False, True, False, True, True, "2019100100", "", "hour,time,gio"),
        column_row("gold.fact_sales", "gross_amount", "DECIMAL(18,2)", "Gross Amount", "Purchase revenue amount.", "silver.ecommerce_events", "price", "price for purchase events", True, False, True, False, False, "129.99", "", "revenue,sales,doanh thu"),
        column_row("gold.fact_sales", "sale_date", "DATE", "Sale Date", "Calendar date of purchase.", "silver.ecommerce_events", "event_date", "event_date where event_type purchase", False, True, False, True, False, "2019-10-01", "", "sale date,ngay ban"),
        column_row("gold.dim_product", "brand", "STRING", "Brand", "Observed product brand.", "silver.ecommerce_events", "brand", "first non-null brand by product", True, True, False, False, False, "samsung", "", "brand,thuong hieu"),
        column_row("gold.dim_product", "category_l1", "STRING", "Category Level 1", "Top-level product category.", "silver.ecommerce_events", "category_l1", "first non-null category_l1 by product", True, True, False, False, False, "electronics", "", "category,danh muc"),
        column_row("gold.dim_product", "category_l2", "STRING", "Category Level 2", "Second-level product category.", "silver.ecommerce_events", "category_l2", "first non-null category_l2 by product", True, True, False, False, False, "smartphone", "", "subcategory,danh muc cap 2"),
        column_row("gold.dim_product", "category_l3", "STRING", "Category Level 3", "Third-level product category.", "silver.ecommerce_events", "category_l3", "first non-null category_l3 by product", True, True, False, False, False, "android", "", "category leaf,danh muc cap 3"),
        column_row("gold.daily_event_summary", "total_revenue", "DECIMAL(18,2)", "Total Revenue", "Daily revenue from purchase events.", "gold.fact_events", "price", "sum purchase price by event_date", True, False, True, False, False, "1000.00", "", "doanh thu,revenue,sales"),
        column_row("gold.daily_event_summary", "total_views", "BIGINT", "Total Views", "Daily count of view events.", "gold.fact_events", "is_view", "sum view flag", False, False, True, False, False, "5000", "", "views,luot xem"),
        column_row("gold.daily_event_summary", "total_carts", "BIGINT", "Total Carts", "Daily count of cart events.", "gold.fact_events", "is_cart", "sum cart flag", False, False, True, False, False, "250", "", "cart,add to cart,gio hang"),
        column_row("gold.daily_event_summary", "total_purchases", "BIGINT", "Total Purchases", "Daily count of purchase events.", "gold.fact_events", "is_purchase", "sum purchase flag", False, False, True, False, False, "80", "", "purchases,orders,don hang"),
        column_row("gold.daily_event_summary", "conversion_rate", "DOUBLE", "Conversion Rate", "Purchases divided by views.", "gold.daily_event_summary", "total_purchases,total_views", "total_purchases / nullif(total_views, 0)", True, False, True, False, False, "0.04", "", "conversion,ty le chuyen doi"),
        column_row("gold.daily_event_summary", "cart_to_purchase_rate", "DOUBLE", "Cart To Purchase Rate", "Purchases divided by cart events.", "gold.daily_event_summary", "total_purchases,total_carts", "total_purchases / nullif(total_carts, 0)", True, False, True, False, False, "0.25", "", "cart conversion,checkout rate"),
        column_row("gold.daily_product_summary", "revenue", "DECIMAL(18,2)", "Product Revenue", "Daily revenue by product.", "gold.fact_events", "price", "sum purchase price by product and date", True, False, True, False, False, "120.00", "", "product revenue,doanh thu san pham"),
        column_row("gold.daily_product_summary", "view_count", "BIGINT", "Product Views", "Daily views by product.", "gold.fact_events", "is_view", "sum view flag by product", True, False, True, False, False, "42", "", "product views"),
        column_row("gold.daily_product_summary", "cart_count", "BIGINT", "Product Carts", "Daily carts by product.", "gold.fact_events", "is_cart", "sum cart flag by product", True, False, True, False, False, "5", "", "product carts"),
        column_row("gold.daily_product_summary", "purchase_count", "BIGINT", "Product Purchases", "Daily purchases by product.", "gold.fact_events", "is_purchase", "sum purchase flag by product", True, False, True, False, False, "2", "", "product purchases"),
        column_row("gold.daily_product_summary", "conversion_rate", "DOUBLE", "Product Conversion Rate", "Product purchases divided by product views.", "gold.daily_product_summary", "purchase_count,view_count", "purchase_count / nullif(view_count, 0)", True, False, True, False, False, "0.08", "", "product conversion"),
        column_row("gold.daily_product_summary", "cart_to_purchase_rate", "DOUBLE", "Product Cart To Purchase Rate", "Product purchases divided by product carts.", "gold.daily_product_summary", "purchase_count,cart_count", "purchase_count / nullif(cart_count, 0)", True, False, True, False, False, "0.40", "", "product cart conversion"),
        column_row("gold.daily_category_summary", "revenue", "DECIMAL(18,2)", "Category Revenue", "Daily revenue by category hierarchy.", "gold.fact_events", "price", "sum purchase price by category and date", True, False, True, False, False, "800.00", "", "category revenue,doanh thu danh muc"),
        column_row("gold.daily_brand_summary", "revenue", "DECIMAL(18,2)", "Brand Revenue", "Daily revenue by brand.", "gold.fact_events", "price", "sum purchase price by brand and date", True, False, True, False, False, "500.00", "", "brand revenue,doanh thu thuong hieu"),
        column_row("gold.dim_user", "total_revenue", "DECIMAL(18,2)", "User Revenue", "Total user revenue.", "gold.fact_sales", "gross_amount", "sum gross_amount by user", True, False, True, False, False, "250.00", "", "customer revenue,doanh thu nguoi dung"),
        column_row("gold.dim_session", "session_id", "STRING", "Session ID", "Session identifier.", "gold.fact_events", "session_id", "group by session_id", False, True, False, False, True, "abc-session", "", "session,phien"),
    ]
    columns = [
        "column_id",
        "table_name",
        "column_name",
        "data_type",
        "business_name",
        "description",
        "source_table",
        "source_column",
        "transformation_logic",
        "is_nullable",
        "is_dimension",
        "is_metric",
        "is_time_column",
        "is_join_key",
        "example_values",
        "allowed_values",
        "agent_synonyms",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def build_metadata_metric_catalog_df(spark, config):
    rows = [
        ("total_revenue", "Total Revenue", "Total sales revenue from purchase events.", "SUM(gross_amount)", "gold.fact_sales", "sale_date", "sum", "currency", "Doanh thu theo ngay la bao nhieu?"),
        ("purchase_count", "Purchase Count", "Number of purchase events.", "COUNT(*)", "gold.fact_sales", "sale_date", "count", "events", "Co bao nhieu purchase trong ngay?"),
        ("view_count", "View Count", "Number of product view events.", "SUM(total_views)", "gold.daily_event_summary", "event_date", "sum", "events", "Luot xem theo ngay la bao nhieu?"),
        ("conversion_rate", "Conversion Rate", "Purchases divided by views.", "SUM(total_purchases) / NULLIF(SUM(total_views), 0)", "gold.daily_event_summary", "event_date", "ratio", "ratio", "Ty le chuyen doi la bao nhieu?"),
        ("cart_to_purchase_rate", "Cart To Purchase Rate", "Purchases divided by cart events.", "SUM(total_purchases) / NULLIF(SUM(total_carts), 0)", "gold.daily_event_summary", "event_date", "ratio", "ratio", "Ty le gio hang sang mua hang la bao nhieu?"),
        ("active_users", "Active Users", "Distinct users that generated events.", "COUNT(DISTINCT user_id)", "gold.fact_events", "event_date", "count_distinct", "users", "Co bao nhieu user active?"),
        ("unique_sessions", "Unique Sessions", "Distinct sessions that generated events.", "COUNT(DISTINCT session_id)", "gold.fact_events", "event_date", "count_distinct", "sessions", "Co bao nhieu session?"),
        ("product_revenue", "Product Revenue", "Revenue grouped by product.", "SUM(revenue)", "gold.daily_product_summary", "event_date", "sum", "currency", "San pham nao co doanh thu cao nhat?"),
    ]
    columns = [
        "metric_name",
        "business_name",
        "description",
        "formula_sql",
        "base_table",
        "default_time_column",
        "aggregation_type",
        "unit",
        "example_question",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def build_metadata_join_catalog_df(spark, config):
    rows = [
        ("fact_events__dim_time", "gold.fact_events", "time_id", "gold.dim_time", "time_id", "many_to_one", "Join events to hourly time dimension."),
        ("fact_events__dim_product", "gold.fact_events", "product_id", "gold.dim_product", "product_id", "many_to_one", "Join events to product dimension."),
        ("fact_sales__dim_time", "gold.fact_sales", "time_id", "gold.dim_time", "time_id", "many_to_one", "Join sales to hourly time dimension."),
        ("fact_sales__dim_product", "gold.fact_sales", "product_id", "gold.dim_product", "product_id", "many_to_one", "Join sales to product dimension."),
        ("daily_product_summary__dim_product", "gold.daily_product_summary", "product_id", "gold.dim_product", "product_id", "many_to_one", "Join daily product metrics to product dimension."),
        ("fact_events__dim_user", "gold.fact_events", "user_id", "gold.dim_user", "user_id", "many_to_one", "Join events to user behavior dimension."),
        ("fact_events__dim_session", "gold.fact_events", "session_id", "gold.dim_session", "session_id", "many_to_one", "Join events to session behavior dimension."),
    ]
    columns = [
        "join_id",
        "left_table",
        "left_key",
        "right_table",
        "right_key",
        "relationship_type",
        "description",
    ]
    return metadata_df_from_rows(spark, columns, rows)


def build_metadata_outputs(spark, config):
    return {
        "table_catalog": build_metadata_table_catalog_df(spark, config),
        "column_catalog": build_metadata_column_catalog_df(spark, config),
        "metric_catalog": build_metadata_metric_catalog_df(spark, config),
        "join_catalog": build_metadata_join_catalog_df(spark, config),
    }

