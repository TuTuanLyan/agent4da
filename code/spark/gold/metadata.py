"""Semantic metadata catalog for Gold tables.

This module publishes compact business metadata for AI Agent use. It does not
describe Iceberg internals; it describes tables, columns, metrics, and safe joins.
"""

from datetime import datetime, timezone

from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gold.config import (
    COLUMN_CATALOG,
    DAILY_BRAND_SUMMARY,
    DAILY_CATEGORY_SUMMARY,
    DAILY_EVENT_SUMMARY,
    DAILY_PRODUCT_SUMMARY,
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_REFRESH_MODE,
    DIM_PRODUCT,
    DIM_SESSION,
    DIM_TIME,
    DIM_USER,
    FACT_EVENTS,
    FACT_SALES,
    JOIN_CATALOG,
    METRIC_CATALOG,
    STG_EVENTS,
    TABLE_CATALOG,
    table_location,
)
from gold.ddl import create_iceberg_table_if_not_exists, create_namespace_if_not_exists
from gold.identifiers import (
    assert_safe_table_location,
    table_identifier,
    validate_identifier_part,
)
from gold.writers import write_full_refresh


TABLE_CATALOG_COLUMNS = [
    "table_name",
    "layer",
    "table_type",
    "business_name",
    "description",
    "grain",
    "primary_key",
    "unique_key",
    "storage_format",
    "query_engine",
    "is_agent_visible",
    "recommended_for_agent",
    "refresh_frequency",
    "owner",
    "created_at",
    "updated_at",
]

COLUMN_CATALOG_COLUMNS = [
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
    "is_unique_key",
    "example_values",
    "allowed_values",
    "agent_synonyms",
]

METRIC_CATALOG_COLUMNS = [
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

JOIN_CATALOG_COLUMNS = [
    "join_id",
    "left_table",
    "left_key",
    "right_table",
    "right_key",
    "relationship_type",
    "description",
]

TABLE_CATALOG_SCHEMA_SQL = """
table_name STRING,
layer STRING,
table_type STRING,
business_name STRING,
description STRING,
grain STRING,
primary_key STRING,
unique_key STRING,
storage_format STRING,
query_engine STRING,
is_agent_visible BOOLEAN,
recommended_for_agent BOOLEAN,
refresh_frequency STRING,
owner STRING,
created_at TIMESTAMP,
updated_at TIMESTAMP
""".strip()

COLUMN_CATALOG_SCHEMA_SQL = """
column_id STRING,
table_name STRING,
column_name STRING,
data_type STRING,
business_name STRING,
description STRING,
source_table STRING,
source_column STRING,
transformation_logic STRING,
is_nullable BOOLEAN,
is_dimension BOOLEAN,
is_metric BOOLEAN,
is_time_column BOOLEAN,
is_join_key BOOLEAN,
is_unique_key BOOLEAN,
example_values STRING,
allowed_values STRING,
agent_synonyms STRING
""".strip()

METRIC_CATALOG_SCHEMA_SQL = """
metric_name STRING,
business_name STRING,
description STRING,
formula_sql STRING,
base_table STRING,
default_time_column STRING,
aggregation_type STRING,
unit STRING,
example_question STRING
""".strip()

JOIN_CATALOG_SCHEMA_SQL = """
join_id STRING,
left_table STRING,
left_key STRING,
right_table STRING,
right_key STRING,
relationship_type STRING,
description STRING
""".strip()

TABLE_CATALOG_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_type", StringType(), False),
        StructField("business_name", StringType(), False),
        StructField("description", StringType(), False),
        StructField("grain", StringType(), False),
        StructField("primary_key", StringType(), True),
        StructField("unique_key", StringType(), True),
        StructField("storage_format", StringType(), False),
        StructField("query_engine", StringType(), False),
        StructField("is_agent_visible", BooleanType(), False),
        StructField("recommended_for_agent", BooleanType(), False),
        StructField("refresh_frequency", StringType(), False),
        StructField("owner", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

COLUMN_CATALOG_SCHEMA = StructType(
    [
        StructField("column_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("business_name", StringType(), False),
        StructField("description", StringType(), False),
        StructField("source_table", StringType(), True),
        StructField("source_column", StringType(), True),
        StructField("transformation_logic", StringType(), True),
        StructField("is_nullable", BooleanType(), False),
        StructField("is_dimension", BooleanType(), False),
        StructField("is_metric", BooleanType(), False),
        StructField("is_time_column", BooleanType(), False),
        StructField("is_join_key", BooleanType(), False),
        StructField("is_unique_key", BooleanType(), False),
        StructField("example_values", StringType(), True),
        StructField("allowed_values", StringType(), True),
        StructField("agent_synonyms", StringType(), True),
    ]
)

METRIC_CATALOG_SCHEMA = StructType(
    [
        StructField("metric_name", StringType(), False),
        StructField("business_name", StringType(), False),
        StructField("description", StringType(), False),
        StructField("formula_sql", StringType(), False),
        StructField("base_table", StringType(), False),
        StructField("default_time_column", StringType(), False),
        StructField("aggregation_type", StringType(), False),
        StructField("unit", StringType(), False),
        StructField("example_question", StringType(), False),
    ]
)

JOIN_CATALOG_SCHEMA = StructType(
    [
        StructField("join_id", StringType(), False),
        StructField("left_table", StringType(), False),
        StructField("left_key", StringType(), False),
        StructField("right_table", StringType(), False),
        StructField("right_key", StringType(), False),
        StructField("relationship_type", StringType(), False),
        StructField("description", StringType(), False),
    ]
)

METADATA_TABLE_SPECS = {
    TABLE_CATALOG: (TABLE_CATALOG_SCHEMA_SQL, TABLE_CATALOG_COLUMNS),
    COLUMN_CATALOG: (COLUMN_CATALOG_SCHEMA_SQL, COLUMN_CATALOG_COLUMNS),
    METRIC_CATALOG: (METRIC_CATALOG_SCHEMA_SQL, METRIC_CATALOG_COLUMNS),
    JOIN_CATALOG: (JOIN_CATALOG_SCHEMA_SQL, JOIN_CATALOG_COLUMNS),
}

TABLE_DEFINITIONS = [
    {
        "table": STG_EVENTS,
        "namespace_role": "staging",
        "layer": "gold_staging",
        "table_type": "staging",
        "business_name": "Staging events",
        "description": "Clean deduplicated events prepared for Gold tables.",
        "grain": "1 row = 1 deduplicated valid event from Silver",
        "primary_key": "event_fingerprint",
        "unique_key": "event_fingerprint",
        "is_agent_visible": False,
        "recommended_for_agent": False,
    },
    {
        "table": FACT_EVENTS,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "fact",
        "business_name": "Event fact",
        "description": "Detailed ecommerce events with event-type flags.",
        "grain": "1 row = 1 unique business event",
        "primary_key": "event_fingerprint",
        "unique_key": "event_fingerprint",
        "is_agent_visible": True,
        "recommended_for_agent": False,
    },
    {
        "table": FACT_SALES,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "fact",
        "business_name": "Sales fact",
        "description": "Purchase events represented as sales rows.",
        "grain": "1 row = 1 purchase event",
        "primary_key": "sale_id",
        "unique_key": "sale_id",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DIM_TIME,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "dimension",
        "business_name": "Time dimension",
        "description": "Hourly event time attributes.",
        "grain": "1 row = 1 event hour",
        "primary_key": "time_id",
        "unique_key": "time_id",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DIM_PRODUCT,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "dimension",
        "business_name": "Product dimension",
        "description": "Product category, brand, and observed price attributes.",
        "grain": "1 row = 1 product",
        "primary_key": "product_id",
        "unique_key": "product_id",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DIM_USER,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "dimension",
        "business_name": "User dimension",
        "description": "User activity and purchase summary.",
        "grain": "1 row = 1 user",
        "primary_key": "user_id",
        "unique_key": "user_id",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DIM_SESSION,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "dimension",
        "business_name": "Session dimension",
        "description": "Session behavior and revenue summary.",
        "grain": "1 row = 1 session",
        "primary_key": "session_id",
        "unique_key": "session_id",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DAILY_EVENT_SUMMARY,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "summary",
        "business_name": "Daily event summary",
        "description": "Daily traffic, conversion, and revenue metrics.",
        "grain": "1 row = 1 event_date",
        "primary_key": "event_date",
        "unique_key": "event_date",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DAILY_PRODUCT_SUMMARY,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "summary",
        "business_name": "Daily product summary",
        "description": "Daily metrics by product, brand, and category.",
        "grain": "1 row = 1 event_date + 1 product",
        "primary_key": "summary_id",
        "unique_key": "event_date, product_id",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DAILY_CATEGORY_SUMMARY,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "summary",
        "business_name": "Daily category summary",
        "description": "Daily metrics by category hierarchy.",
        "grain": "1 row = 1 event_date + category hierarchy",
        "primary_key": "summary_id",
        "unique_key": "event_date, category_l1, category_l2, category_l3",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
    {
        "table": DAILY_BRAND_SUMMARY,
        "namespace_role": "gold",
        "layer": "gold",
        "table_type": "summary",
        "business_name": "Daily brand summary",
        "description": "Daily metrics by brand.",
        "grain": "1 row = 1 event_date + brand",
        "primary_key": "summary_id",
        "unique_key": "event_date, brand",
        "is_agent_visible": True,
        "recommended_for_agent": True,
    },
]

COMMON_COLUMN_OVERRIDES = {
    "event_fingerprint": {
        "business_name": "Event fingerprint",
        "description": "Stable event key used to deduplicate events.",
        "is_join_key": True,
        "is_unique_key": True,
        "agent_synonyms": "mã sự kiện, khóa sự kiện",
    },
    "source_event_id": {
        "business_name": "Source event id",
        "description": "Original source event id.",
        "is_dimension": True,
    },
    "sale_id": {
        "business_name": "Sale id",
        "description": "Unique sale row id.",
        "is_unique_key": True,
        "agent_synonyms": "mã giao dịch mua",
    },
    "summary_id": {
        "business_name": "Summary id",
        "description": "Unique summary row id.",
        "is_unique_key": True,
    },
    "time_id": {
        "business_name": "Time id",
        "description": "Hourly time key in yyyyMMddHH format.",
        "is_time_column": True,
        "is_join_key": True,
        "agent_synonyms": "giờ, khóa thời gian",
    },
    "event_ts": {
        "business_name": "Event timestamp",
        "description": "Timestamp when the event happened.",
        "is_time_column": True,
        "agent_synonyms": "thời điểm sự kiện",
    },
    "sale_ts": {
        "business_name": "Sale timestamp",
        "description": "Timestamp when the purchase event happened.",
        "is_time_column": True,
        "agent_synonyms": "thời điểm mua",
    },
    "event_date": {
        "business_name": "Event date",
        "description": "Calendar date of the event.",
        "is_dimension": True,
        "is_time_column": True,
        "agent_synonyms": "ngày, ngày sự kiện",
    },
    "sale_date": {
        "business_name": "Sale date",
        "description": "Calendar date of the purchase event.",
        "is_dimension": True,
        "is_time_column": True,
        "agent_synonyms": "ngày mua, ngày bán",
    },
    "event_year": {
        "business_name": "Event year",
        "description": "Year of the event.",
        "is_dimension": True,
        "is_time_column": True,
    },
    "event_month": {
        "business_name": "Event month",
        "description": "Month of the event.",
        "is_dimension": True,
        "is_time_column": True,
    },
    "event_day": {
        "business_name": "Event day",
        "description": "Day of month of the event.",
        "is_dimension": True,
        "is_time_column": True,
    },
    "event_hour": {
        "business_name": "Event hour",
        "description": "Hour of day of the event.",
        "is_dimension": True,
        "is_time_column": True,
    },
    "event_type": {
        "business_name": "Event type",
        "description": "User behavior type for the event.",
        "is_dimension": True,
        "example_values": "view, cart, remove_from_cart, purchase",
        "allowed_values": "view, cart, remove_from_cart, purchase",
        "agent_synonyms": "loại sự kiện, hành vi",
    },
    "product_id": {
        "business_name": "Product id",
        "description": "Product key.",
        "is_dimension": True,
        "is_join_key": True,
        "agent_synonyms": "sản phẩm, mã sản phẩm",
    },
    "category_id": {
        "business_name": "Category id",
        "description": "Source category id.",
        "is_dimension": True,
        "agent_synonyms": "mã danh mục",
    },
    "category_code": {
        "business_name": "Category code",
        "description": "Source category code.",
        "is_dimension": True,
        "agent_synonyms": "mã ngành hàng, mã danh mục",
    },
    "category_l1": {
        "business_name": "Category level 1",
        "description": "Top-level product category.",
        "is_dimension": True,
        "agent_synonyms": "danh mục, ngành hàng, loại sản phẩm",
    },
    "category_l2": {
        "business_name": "Category level 2",
        "description": "Second-level product category.",
        "is_dimension": True,
        "agent_synonyms": "danh mục, ngành hàng, loại sản phẩm",
    },
    "category_l3": {
        "business_name": "Category level 3",
        "description": "Third-level product category.",
        "is_dimension": True,
        "agent_synonyms": "danh mục, ngành hàng, loại sản phẩm",
    },
    "brand": {
        "business_name": "Brand",
        "description": "Product brand.",
        "is_dimension": True,
        "example_values": "unknown",
        "agent_synonyms": "thương hiệu, hãng",
    },
    "user_id": {
        "business_name": "User id",
        "description": "User key.",
        "is_dimension": True,
        "is_join_key": True,
        "agent_synonyms": "người dùng, khách hàng",
    },
    "session_id": {
        "business_name": "Session id",
        "description": "User session key.",
        "is_dimension": True,
        "is_join_key": True,
        "agent_synonyms": "phiên, phiên truy cập",
    },
    "price": {
        "business_name": "Event price",
        "description": "Observed product price on the event.",
        "is_metric": True,
        "unit": "currency",
        "agent_synonyms": "giá, giá sự kiện",
    },
    "unit_price": {
        "business_name": "Unit price",
        "description": "Unit price for a purchase event.",
        "is_metric": True,
        "agent_synonyms": "đơn giá, giá bán",
    },
    "gross_amount": {
        "business_name": "Gross amount",
        "description": "Purchase amount before adjustments.",
        "is_metric": True,
        "agent_synonyms": "doanh thu, tiền bán",
    },
    "quantity": {
        "business_name": "Quantity",
        "description": "Purchase quantity; current event data uses 1.",
        "is_metric": True,
        "agent_synonyms": "số lượng",
    },
    "revenue": {
        "business_name": "Revenue",
        "description": "Revenue at the table grain.",
        "is_metric": True,
        "agent_synonyms": "doanh thu, tổng doanh thu, tiền bán",
    },
    "total_revenue": {
        "business_name": "Total revenue",
        "description": "Total revenue at the table grain.",
        "is_metric": True,
        "agent_synonyms": "doanh thu, tổng doanh thu, tiền bán",
    },
    "view_count": {
        "business_name": "View count",
        "description": "Number of view events.",
        "is_metric": True,
        "agent_synonyms": "lượt xem, số lượt xem",
    },
    "total_views": {
        "business_name": "Total views",
        "description": "Total number of view events.",
        "is_metric": True,
        "agent_synonyms": "lượt xem, số lượt xem",
    },
    "cart_count": {
        "business_name": "Cart count",
        "description": "Number of add-to-cart events.",
        "is_metric": True,
        "agent_synonyms": "lượt thêm giỏ, số lượt thêm vào giỏ",
    },
    "total_carts": {
        "business_name": "Total carts",
        "description": "Total number of add-to-cart events.",
        "is_metric": True,
        "agent_synonyms": "lượt thêm giỏ, số lượt thêm vào giỏ",
    },
    "purchase_count": {
        "business_name": "Purchase count",
        "description": "Number of purchase events.",
        "is_metric": True,
        "agent_synonyms": "lượt mua, số lượt mua, số giao dịch mua",
    },
    "total_purchases": {
        "business_name": "Total purchases",
        "description": "Total number of purchase events.",
        "is_metric": True,
        "agent_synonyms": "lượt mua, số lượt mua, số giao dịch mua",
    },
    "remove_from_cart_count": {
        "business_name": "Remove from cart count",
        "description": "Number of remove-from-cart events.",
        "is_metric": True,
        "agent_synonyms": "lượt bỏ giỏ, số lượt bỏ khỏi giỏ",
    },
    "total_remove_from_carts": {
        "business_name": "Total remove from carts",
        "description": "Total number of remove-from-cart events.",
        "is_metric": True,
        "agent_synonyms": "lượt bỏ giỏ, số lượt bỏ khỏi giỏ",
    },
    "total_cart_adds": {
        "business_name": "Total cart adds",
        "description": "Total add-to-cart events for the user.",
        "is_metric": True,
        "agent_synonyms": "lượt thêm giỏ, số lượt thêm vào giỏ",
    },
    "conversion_rate": {
        "business_name": "Conversion rate",
        "description": "Purchases divided by views.",
        "is_metric": True,
        "agent_synonyms": "tỷ lệ chuyển đổi",
    },
    "cart_to_purchase_rate": {
        "business_name": "Cart to purchase rate",
        "description": "Purchases divided by add-to-cart events.",
        "is_metric": True,
        "agent_synonyms": "tỷ lệ giỏ hàng sang mua",
    },
    "unique_users": {
        "business_name": "Unique users",
        "description": "Distinct users at the table grain.",
        "is_metric": True,
        "agent_synonyms": "người dùng, khách hàng, số khách hàng",
    },
    "unique_sessions": {
        "business_name": "Unique sessions",
        "description": "Distinct sessions at the table grain.",
        "is_metric": True,
        "agent_synonyms": "phiên, phiên truy cập, số phiên",
    },
    "unique_products": {
        "business_name": "Unique products",
        "description": "Distinct products at the table grain.",
        "is_metric": True,
        "agent_synonyms": "sản phẩm, số sản phẩm",
    },
    "unique_events": {
        "business_name": "Unique events",
        "description": "Distinct events at the table grain.",
        "is_metric": True,
    },
    "total_events": {
        "business_name": "Total events",
        "description": "Total number of events.",
        "is_metric": True,
        "agent_synonyms": "số sự kiện, lượt sự kiện",
    },
    "event_count": {
        "business_name": "Event count",
        "description": "Number of events.",
        "is_metric": True,
        "agent_synonyms": "số sự kiện, lượt sự kiện",
    },
    "avg_event_price": {
        "business_name": "Average event price",
        "description": "Average observed event price.",
        "is_metric": True,
        "agent_synonyms": "giá trung bình",
    },
    "avg_price": {
        "business_name": "Average price",
        "description": "Average observed price.",
        "is_metric": True,
        "agent_synonyms": "giá trung bình",
    },
    "min_price": {
        "business_name": "Minimum price",
        "description": "Minimum observed price.",
        "is_metric": True,
    },
    "max_price": {
        "business_name": "Maximum price",
        "description": "Maximum observed price.",
        "is_metric": True,
    },
    "avg_observed_price": {
        "business_name": "Average observed price",
        "description": "Average observed product price.",
        "is_metric": True,
    },
    "min_observed_price": {
        "business_name": "Minimum observed price",
        "description": "Minimum observed product price.",
        "is_metric": True,
    },
    "max_observed_price": {
        "business_name": "Maximum observed price",
        "description": "Maximum observed product price.",
        "is_metric": True,
    },
    "record_count": {
        "business_name": "Record count",
        "description": "Number of source records behind this dimension row.",
        "is_metric": True,
    },
    "total_sessions": {
        "business_name": "Total sessions",
        "description": "Total sessions for the user.",
        "is_metric": True,
        "agent_synonyms": "số phiên, phiên truy cập",
    },
    "session_duration_sec": {
        "business_name": "Session duration seconds",
        "description": "Session duration in seconds.",
        "is_metric": True,
    },
    "session_revenue": {
        "business_name": "Session revenue",
        "description": "Revenue generated in the session.",
        "is_metric": True,
        "agent_synonyms": "doanh thu, tiền bán",
    },
    "has_purchase": {
        "business_name": "Has purchase",
        "description": "True when the session contains a purchase.",
        "is_dimension": True,
    },
    "is_view": {
        "business_name": "Is view",
        "description": "True when event_type is view.",
        "is_metric": True,
    },
    "is_cart": {
        "business_name": "Is cart",
        "description": "True when event_type is cart.",
        "is_metric": True,
    },
    "is_remove_from_cart": {
        "business_name": "Is remove from cart",
        "description": "True when event_type is remove_from_cart.",
        "is_metric": True,
    },
    "is_purchase": {
        "business_name": "Is purchase",
        "description": "True when event_type is purchase.",
        "is_metric": True,
    },
    "first_seen_at": {
        "business_name": "First seen at",
        "description": "First observed event timestamp.",
        "is_time_column": True,
    },
    "last_seen_at": {
        "business_name": "Last seen at",
        "description": "Last observed event timestamp.",
        "is_time_column": True,
    },
    "session_start_at": {
        "business_name": "Session start at",
        "description": "First event timestamp in the session.",
        "is_time_column": True,
    },
    "session_end_at": {
        "business_name": "Session end at",
        "description": "Last event timestamp in the session.",
        "is_time_column": True,
    },
}

COLUMN_OVERRIDES = {
    STG_EVENTS: {
        "event_fingerprint": {
            "source_table": "silver.ecommerce_events",
            "source_column": "event_fingerprint",
            "transformation_logic": "Filter valid events and deduplicate by event_fingerprint.",
        },
        "session_id": {
            "source_table": "silver.ecommerce_events",
            "source_column": "user_session",
            "transformation_logic": "Rename user_session to session_id.",
        },
        "time_id": {
            "transformation_logic": "date_format(event_ts, 'yyyyMMddHH')",
        },
    },
    FACT_EVENTS: {
        "event_fingerprint": {
            "transformation_logic": "Selected from stg_events as the unique event key.",
        },
        "is_view": {"transformation_logic": "event_type = 'view'"},
        "is_cart": {"transformation_logic": "event_type = 'cart'"},
        "is_remove_from_cart": {
            "transformation_logic": "event_type = 'remove_from_cart'",
        },
        "is_purchase": {"transformation_logic": "event_type = 'purchase'"},
    },
    FACT_SALES: {
        "sale_id": {
            "source_column": "event_fingerprint",
            "transformation_logic": "Use purchase event_fingerprint as sale_id.",
        },
        "gross_amount": {
            "transformation_logic": "unit_price * quantity; quantity is 1 for event data.",
        },
    },
    DIM_TIME: {
        "time_id": {"description": "Primary hourly time key."},
        "day_of_week": {
            "business_name": "Day of week",
            "description": "Day of week number.",
            "is_dimension": True,
            "is_time_column": True,
        },
        "day_name": {
            "business_name": "Day name",
            "description": "Day of week name.",
            "is_dimension": True,
            "is_time_column": True,
        },
        "month_name": {
            "business_name": "Month name",
            "description": "Month name.",
            "is_dimension": True,
            "is_time_column": True,
        },
        "quarter": {
            "business_name": "Quarter",
            "description": "Calendar quarter.",
            "is_dimension": True,
            "is_time_column": True,
        },
        "is_weekend": {
            "business_name": "Is weekend",
            "description": "True for Saturday or Sunday.",
            "is_dimension": True,
        },
    },
    DIM_PRODUCT: {
        "product_id": {
            "description": "Primary product key.",
            "is_unique_key": True,
        },
    },
    DIM_USER: {
        "user_id": {
            "description": "Primary user key.",
            "is_unique_key": True,
        },
    },
    DIM_SESSION: {
        "session_id": {
            "description": "Primary session key.",
            "is_unique_key": True,
        },
    },
    DAILY_EVENT_SUMMARY: {
        "event_date": {
            "description": "Daily summary date.",
            "is_unique_key": True,
        },
    },
    DAILY_PRODUCT_SUMMARY: {
        "summary_id": {
            "description": "Unique key for event_date and product_id.",
        },
    },
    DAILY_CATEGORY_SUMMARY: {
        "summary_id": {
            "description": "Unique key for event_date and category hierarchy.",
        },
    },
    DAILY_BRAND_SUMMARY: {
        "summary_id": {
            "description": "Unique key for event_date and brand.",
        },
    },
}

METRIC_DEFINITIONS = [
    {
        "metric_name": "total_revenue",
        "business_name": "Total revenue",
        "description": "Revenue from purchase events.",
        "formula_sql": "SUM(total_revenue)",
        "base_table": DAILY_EVENT_SUMMARY,
        "default_time_column": "event_date",
        "aggregation_type": "sum",
        "unit": "currency",
        "example_question": "Doanh thu theo ngày là bao nhiêu?",
    },
    {
        "metric_name": "purchase_count",
        "business_name": "Purchase count",
        "description": "Number of purchase events.",
        "formula_sql": "SUM(total_purchases)",
        "base_table": DAILY_EVENT_SUMMARY,
        "default_time_column": "event_date",
        "aggregation_type": "sum",
        "unit": "events",
        "example_question": "Có bao nhiêu lượt mua mỗi ngày?",
    },
    {
        "metric_name": "view_count",
        "business_name": "View count",
        "description": "Number of view events.",
        "formula_sql": "SUM(total_views)",
        "base_table": DAILY_EVENT_SUMMARY,
        "default_time_column": "event_date",
        "aggregation_type": "sum",
        "unit": "events",
        "example_question": "Số lượt xem theo ngày là bao nhiêu?",
    },
    {
        "metric_name": "cart_count",
        "business_name": "Cart count",
        "description": "Number of add-to-cart events.",
        "formula_sql": "SUM(total_carts)",
        "base_table": DAILY_EVENT_SUMMARY,
        "default_time_column": "event_date",
        "aggregation_type": "sum",
        "unit": "events",
        "example_question": "Số lượt thêm giỏ theo ngày là bao nhiêu?",
    },
    {
        "metric_name": "remove_from_cart_count",
        "business_name": "Remove from cart count",
        "description": "Number of remove-from-cart events.",
        "formula_sql": "SUM(total_remove_from_carts)",
        "base_table": DAILY_EVENT_SUMMARY,
        "default_time_column": "event_date",
        "aggregation_type": "sum",
        "unit": "events",
        "example_question": "Có bao nhiêu lượt bỏ khỏi giỏ?",
    },
    {
        "metric_name": "conversion_rate",
        "business_name": "Conversion rate",
        "description": "Purchases divided by views.",
        "formula_sql": "CASE WHEN total_views = 0 THEN 0 ELSE total_purchases * 1.0 / total_views END",
        "base_table": DAILY_EVENT_SUMMARY,
        "default_time_column": "event_date",
        "aggregation_type": "ratio",
        "unit": "ratio",
        "example_question": "Tỷ lệ chuyển đổi theo ngày là bao nhiêu?",
    },
    {
        "metric_name": "cart_to_purchase_rate",
        "business_name": "Cart to purchase rate",
        "description": "Purchases divided by add-to-cart events.",
        "formula_sql": "CASE WHEN total_carts = 0 THEN 0 ELSE total_purchases * 1.0 / total_carts END",
        "base_table": DAILY_EVENT_SUMMARY,
        "default_time_column": "event_date",
        "aggregation_type": "ratio",
        "unit": "ratio",
        "example_question": "Tỷ lệ từ giỏ hàng sang mua là bao nhiêu?",
    },
    {
        "metric_name": "active_users",
        "business_name": "Active users",
        "description": "Distinct users with events.",
        "formula_sql": "COUNT(DISTINCT user_id)",
        "base_table": FACT_EVENTS,
        "default_time_column": "event_date",
        "aggregation_type": "count_distinct",
        "unit": "users",
        "example_question": "Có bao nhiêu khách hàng hoạt động?",
    },
    {
        "metric_name": "unique_sessions",
        "business_name": "Unique sessions",
        "description": "Distinct sessions with events.",
        "formula_sql": "COUNT(DISTINCT session_id)",
        "base_table": FACT_EVENTS,
        "default_time_column": "event_date",
        "aggregation_type": "count_distinct",
        "unit": "sessions",
        "example_question": "Có bao nhiêu phiên truy cập?",
    },
    {
        "metric_name": "unique_products",
        "business_name": "Unique products",
        "description": "Distinct products with events.",
        "formula_sql": "COUNT(DISTINCT product_id)",
        "base_table": FACT_EVENTS,
        "default_time_column": "event_date",
        "aggregation_type": "count_distinct",
        "unit": "products",
        "example_question": "Có bao nhiêu sản phẩm được tương tác?",
    },
    {
        "metric_name": "avg_event_price",
        "business_name": "Average event price",
        "description": "Average observed event price.",
        "formula_sql": "AVG(price)",
        "base_table": FACT_EVENTS,
        "default_time_column": "event_date",
        "aggregation_type": "avg",
        "unit": "currency",
        "example_question": "Giá trung bình của sự kiện là bao nhiêu?",
    },
]

JOIN_DEFINITIONS = [
    {
        "left_table": FACT_EVENTS,
        "left_key": "time_id",
        "right_table": DIM_TIME,
        "right_key": "time_id",
        "relationship_type": "many_to_one",
        "description": "Attach hourly time attributes to event facts.",
    },
    {
        "left_table": FACT_EVENTS,
        "left_key": "product_id",
        "right_table": DIM_PRODUCT,
        "right_key": "product_id",
        "relationship_type": "many_to_one",
        "description": "Attach product attributes to event facts.",
    },
    {
        "left_table": FACT_EVENTS,
        "left_key": "user_id",
        "right_table": DIM_USER,
        "right_key": "user_id",
        "relationship_type": "many_to_one",
        "description": "Attach user attributes to event facts.",
    },
    {
        "left_table": FACT_EVENTS,
        "left_key": "session_id",
        "right_table": DIM_SESSION,
        "right_key": "session_id",
        "relationship_type": "many_to_one",
        "description": "Attach session attributes to event facts.",
    },
    {
        "left_table": FACT_SALES,
        "left_key": "event_fingerprint",
        "right_table": FACT_EVENTS,
        "right_key": "event_fingerprint",
        "relationship_type": "one_to_one",
        "description": "Link a sale row back to its purchase event.",
    },
    {
        "left_table": FACT_SALES,
        "left_key": "time_id",
        "right_table": DIM_TIME,
        "right_key": "time_id",
        "relationship_type": "many_to_one",
        "description": "Attach hourly time attributes to sales.",
    },
    {
        "left_table": FACT_SALES,
        "left_key": "product_id",
        "right_table": DIM_PRODUCT,
        "right_key": "product_id",
        "relationship_type": "many_to_one",
        "description": "Attach product attributes to sales.",
    },
    {
        "left_table": FACT_SALES,
        "left_key": "user_id",
        "right_table": DIM_USER,
        "right_key": "user_id",
        "relationship_type": "many_to_one",
        "description": "Attach user attributes to sales.",
    },
    {
        "left_table": FACT_SALES,
        "left_key": "session_id",
        "right_table": DIM_SESSION,
        "right_key": "session_id",
        "relationship_type": "many_to_one",
        "description": "Attach session attributes to sales.",
    },
    {
        "left_table": DAILY_PRODUCT_SUMMARY,
        "left_key": "product_id",
        "right_table": DIM_PRODUCT,
        "right_key": "product_id",
        "relationship_type": "many_to_one",
        "description": "Attach current product attributes to product summaries.",
    },
]


def log(message):
    print(f"[GoldMetadata] {message}", flush=True)


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _default_business_name(column_name):
    return str(column_name).replace("_", " ").title()


def _table_namespace(spec, gold_namespace, staging_namespace):
    if spec["namespace_role"] == "staging":
        return staging_namespace
    return gold_namespace


def resolve_table_name(table_key, gold_namespace, staging_namespace):
    for spec in TABLE_DEFINITIONS:
        if spec["table"] == table_key:
            return f"{_table_namespace(spec, gold_namespace, staging_namespace)}.{table_key}"
    raise ValueError(f"Unknown Gold metadata table key: {table_key}")


def expected_table_names(gold_namespace, staging_namespace):
    return {
        resolve_table_name(spec["table"], gold_namespace, staging_namespace)
        for spec in TABLE_DEFINITIONS
    }


def semantic_table_identifier(catalog_name, table_name):
    parts = str(table_name).split(".")
    if len(parts) != 2:
        raise ValueError(
            f"Expected semantic table name as namespace.table, got {table_name!r}."
        )
    return table_identifier(catalog_name, parts[0], parts[1])


def _validate_common_args(catalog_name, metadata_namespace, gold_namespace, staging_namespace):
    validate_identifier_part(catalog_name, "catalog")
    validate_identifier_part(metadata_namespace, "metadata namespace")
    validate_identifier_part(gold_namespace, "gold namespace")
    validate_identifier_part(staging_namespace, "staging namespace")


def _metadata_table_location(metadata_base_path, metadata_table):
    location = table_location(metadata_base_path, metadata_table)
    assert_safe_table_location(location, DEFAULT_ALLOWED_LOCATION_PREFIXES)
    return location


def _field_names(schema):
    return {field.name for field in schema.fields}


def _load_required_gold_schemas(spark, catalog_name, gold_namespace, staging_namespace):
    schemas = {}
    for spec in TABLE_DEFINITIONS:
        table_name = resolve_table_name(spec["table"], gold_namespace, staging_namespace)
        full_name = semantic_table_identifier(catalog_name, table_name)
        try:
            schemas[table_name] = spark.table(full_name).schema
        except Exception as exc:
            raise RuntimeError(
                "Required Gold table not found or unreadable: "
                f"{full_name}. Run gold_pipeline before gold_metadata_pipeline."
            ) from exc
    return schemas


def _read_schema_if_exists(spark, catalog_name, table_name):
    full_name = semantic_table_identifier(catalog_name, table_name)
    try:
        return spark.table(full_name).schema
    except Exception:
        return None


def _validate_declared_column_overrides(schemas, gold_namespace, staging_namespace):
    errors = []
    for spec in TABLE_DEFINITIONS:
        table_key = spec["table"]
        table_name = resolve_table_name(table_key, gold_namespace, staging_namespace)
        schema = schemas.get(table_name)
        if schema is None:
            continue

        real_columns = _field_names(schema)
        for column_name in COLUMN_OVERRIDES.get(table_key, {}):
            if column_name not in real_columns:
                errors.append(
                    f"COLUMN_OVERRIDES[{table_key!r}] declares missing column "
                    f"{column_name!r} for {table_name}."
                )
    if errors:
        raise RuntimeError("Invalid metadata column overrides:\n- " + "\n- ".join(errors))


def _column_metadata(table_key, column_name):
    metadata = {}
    metadata.update(COMMON_COLUMN_OVERRIDES.get(column_name, {}))
    metadata.update(COLUMN_OVERRIDES.get(table_key, {}).get(column_name, {}))
    return metadata


def build_table_catalog_df(spark, gold_namespace, staging_namespace):
    now = _utc_now()
    rows = []
    for spec in TABLE_DEFINITIONS:
        table_name = resolve_table_name(spec["table"], gold_namespace, staging_namespace)
        rows.append(
            {
                "table_name": table_name,
                "layer": spec["layer"],
                "table_type": spec["table_type"],
                "business_name": spec["business_name"],
                "description": spec["description"],
                "grain": spec["grain"],
                "primary_key": spec["primary_key"],
                "unique_key": spec["unique_key"],
                "storage_format": "iceberg",
                "query_engine": "spark_sql",
                "is_agent_visible": spec["is_agent_visible"],
                "recommended_for_agent": spec["recommended_for_agent"],
                "refresh_frequency": "manual when Gold semantics change",
                "owner": "agent4da",
                "created_at": now,
                "updated_at": now,
            }
        )
    return spark.createDataFrame(rows, schema=TABLE_CATALOG_SCHEMA)


def build_column_catalog_df(spark, schemas, gold_namespace, staging_namespace):
    rows = []
    for spec in TABLE_DEFINITIONS:
        table_key = spec["table"]
        table_name = resolve_table_name(table_key, gold_namespace, staging_namespace)
        schema = schemas[table_name]
        for field in schema.fields:
            column_name = field.name
            override = _column_metadata(table_key, column_name)
            business_name = override.get("business_name") or _default_business_name(
                column_name
            )
            rows.append(
                {
                    "column_id": f"{table_name}.{column_name}",
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": field.dataType.simpleString(),
                    "business_name": business_name,
                    "description": override.get("description")
                    or f"{business_name} in {table_name}.",
                    "source_table": override.get("source_table") or table_name,
                    "source_column": override.get("source_column") or column_name,
                    "transformation_logic": override.get("transformation_logic")
                    or "Selected or derived by the Gold pipeline.",
                    "is_nullable": bool(field.nullable),
                    "is_dimension": bool(override.get("is_dimension", False)),
                    "is_metric": bool(override.get("is_metric", False)),
                    "is_time_column": bool(override.get("is_time_column", False)),
                    "is_join_key": bool(override.get("is_join_key", False)),
                    "is_unique_key": bool(override.get("is_unique_key", False)),
                    "example_values": override.get("example_values"),
                    "allowed_values": override.get("allowed_values"),
                    "agent_synonyms": override.get("agent_synonyms"),
                }
            )
    return spark.createDataFrame(rows, schema=COLUMN_CATALOG_SCHEMA)


def build_metric_catalog_df(spark, gold_namespace, staging_namespace):
    rows = []
    for spec in METRIC_DEFINITIONS:
        row = dict(spec)
        row["base_table"] = resolve_table_name(
            row["base_table"],
            gold_namespace,
            staging_namespace,
        )
        rows.append(row)
    return spark.createDataFrame(rows, schema=METRIC_CATALOG_SCHEMA)


def build_join_catalog_df(spark, gold_namespace, staging_namespace):
    rows = []
    for spec in JOIN_DEFINITIONS:
        left_table = resolve_table_name(
            spec["left_table"],
            gold_namespace,
            staging_namespace,
        )
        right_table = resolve_table_name(
            spec["right_table"],
            gold_namespace,
            staging_namespace,
        )
        left_key = spec["left_key"]
        right_key = spec["right_key"]
        rows.append(
            {
                "join_id": f"{left_table}.{left_key}__{right_table}.{right_key}",
                "left_table": left_table,
                "left_key": left_key,
                "right_table": right_table,
                "right_key": right_key,
                "relationship_type": spec["relationship_type"],
                "description": spec["description"],
            }
        )
    return spark.createDataFrame(rows, schema=JOIN_CATALOG_SCHEMA)


def create_metadata_tables(spark, catalog_name, metadata_namespace, metadata_base_path):
    create_namespace_if_not_exists(spark, catalog_name, metadata_namespace)
    for metadata_table, (schema_sql, _columns) in METADATA_TABLE_SPECS.items():
        full_name = table_identifier(catalog_name, metadata_namespace, metadata_table)
        location = _metadata_table_location(metadata_base_path, metadata_table)
        log(f"Ensuring metadata table: {full_name} at {location}")
        create_iceberg_table_if_not_exists(
            spark,
            full_name,
            schema_sql,
            location,
        )


def build_metadata_catalogs(
    spark,
    catalog_name,
    metadata_namespace,
    gold_namespace,
    staging_namespace,
    metadata_base_path,
    refresh_mode=DEFAULT_REFRESH_MODE,
):
    mode = str(refresh_mode).strip().lower()
    if mode != DEFAULT_REFRESH_MODE:
        raise NotImplementedError(
            "Incremental metadata refresh is not implemented; "
            "use --refresh-mode full_refresh."
        )

    _validate_common_args(catalog_name, metadata_namespace, gold_namespace, staging_namespace)
    for metadata_table in METADATA_TABLE_SPECS:
        _metadata_table_location(metadata_base_path, metadata_table)

    log(f"Catalog name        : {catalog_name}")
    log(f"Metadata namespace  : {metadata_namespace}")
    log(f"Gold namespace      : {gold_namespace}")
    log(f"Staging namespace   : {staging_namespace}")
    log(f"Metadata base path  : {metadata_base_path}")
    log(f"Refresh mode        : {mode}")

    schemas = _load_required_gold_schemas(
        spark,
        catalog_name,
        gold_namespace,
        staging_namespace,
    )
    _validate_declared_column_overrides(schemas, gold_namespace, staging_namespace)
    create_metadata_tables(spark, catalog_name, metadata_namespace, metadata_base_path)

    outputs = {
        TABLE_CATALOG: build_table_catalog_df(
            spark,
            gold_namespace,
            staging_namespace,
        ),
        COLUMN_CATALOG: build_column_catalog_df(
            spark,
            schemas,
            gold_namespace,
            staging_namespace,
        ),
        METRIC_CATALOG: build_metric_catalog_df(
            spark,
            gold_namespace,
            staging_namespace,
        ),
        JOIN_CATALOG: build_join_catalog_df(
            spark,
            gold_namespace,
            staging_namespace,
        ),
    }

    row_counts = {}
    for metadata_table, df in outputs.items():
        full_name = table_identifier(catalog_name, metadata_namespace, metadata_table)
        _schema_sql, columns = METADATA_TABLE_SPECS[metadata_table]
        write_full_refresh(df, full_name, columns, mode=mode)
        row_counts[metadata_table] = df.count()

    log("Metadata catalog full refresh completed.")
    return row_counts


def _metadata_full_name(catalog_name, metadata_namespace, metadata_table):
    return table_identifier(catalog_name, metadata_namespace, metadata_table)


def _assert_metadata_table_exists(spark, catalog_name, metadata_namespace, metadata_table):
    full_name = _metadata_full_name(catalog_name, metadata_namespace, metadata_table)
    try:
        spark.table(full_name).schema
    except Exception as exc:
        raise RuntimeError(
            f"Required metadata table not found or unreadable: {full_name}"
        ) from exc
    return full_name


def _duplicate_values(df, key_column):
    return [
        row[key_column]
        for row in (
            df.groupBy(key_column)
            .count()
            .where(col("count") > 1)
            .select(key_column)
            .collect()
        )
    ]


def _collect_metadata_tables(spark, catalog_name, metadata_namespace):
    tables = {}
    for metadata_table in METADATA_TABLE_SPECS:
        full_name = _assert_metadata_table_exists(
            spark,
            catalog_name,
            metadata_namespace,
            metadata_table,
        )
        tables[metadata_table] = spark.table(full_name).cache()
    return tables


def validate_metadata_catalogs(
    spark,
    catalog_name,
    metadata_namespace,
    gold_namespace,
    staging_namespace,
):
    _validate_common_args(catalog_name, metadata_namespace, gold_namespace, staging_namespace)
    metadata_tables = _collect_metadata_tables(spark, catalog_name, metadata_namespace)
    errors = []

    table_df = metadata_tables[TABLE_CATALOG]
    column_df = metadata_tables[COLUMN_CATALOG]
    metric_df = metadata_tables[METRIC_CATALOG]
    join_df = metadata_tables[JOIN_CATALOG]

    duplicate_checks = [
        (table_df, "table_name", TABLE_CATALOG),
        (column_df, "column_id", COLUMN_CATALOG),
        (metric_df, "metric_name", METRIC_CATALOG),
        (join_df, "join_id", JOIN_CATALOG),
    ]
    for df, key_column, table_name in duplicate_checks:
        duplicates = _duplicate_values(df, key_column)
        if duplicates:
            errors.append(
                f"{table_name} has duplicate {key_column}: {', '.join(duplicates)}"
            )

    table_rows = table_df.select(
        "table_name",
        "is_agent_visible",
        "recommended_for_agent",
    ).collect()
    table_names = {row["table_name"] for row in table_rows}
    expected_tables = expected_table_names(gold_namespace, staging_namespace)
    missing_expected_tables = sorted(expected_tables - table_names)
    if missing_expected_tables:
        errors.append(
            "table_catalog is missing required tables: "
            + ", ".join(missing_expected_tables)
        )

    visible_tables = {
        row["table_name"] for row in table_rows if bool(row["is_agent_visible"])
    }
    schema_cache = {}
    for table_name in sorted(table_names):
        schema = _read_schema_if_exists(spark, catalog_name, table_name)
        schema_cache[table_name] = schema
        if table_name in visible_tables and schema is None:
            errors.append(
                "Agent-visible table does not exist or is unreadable: "
                f"{semantic_table_identifier(catalog_name, table_name)}"
            )

    existing_schemas = {
        table_name: schema
        for table_name, schema in schema_cache.items()
        if schema is not None
    }
    try:
        _validate_declared_column_overrides(
            existing_schemas,
            gold_namespace,
            staging_namespace,
        )
    except Exception as exc:
        errors.append(str(exc))

    for row in column_df.select("column_id", "table_name", "column_name").collect():
        column_id = row["column_id"]
        table_name = row["table_name"]
        column_name = row["column_name"]
        if table_name not in table_names:
            errors.append(f"column_catalog references unknown table: {table_name}")
            continue
        expected_column_id = f"{table_name}.{column_name}"
        if column_id != expected_column_id:
            errors.append(
                f"column_catalog column_id mismatch: {column_id} != {expected_column_id}"
            )
        schema = schema_cache.get(table_name)
        if schema is not None and column_name not in _field_names(schema):
            errors.append(
                f"column_catalog references missing column: {table_name}.{column_name}"
            )

    for row in metric_df.select("metric_name", "base_table").collect():
        if row["base_table"] not in table_names:
            errors.append(
                f"metric_catalog.{row['metric_name']} references unknown base_table: "
                f"{row['base_table']}"
            )

    for row in join_df.select(
        "join_id",
        "left_table",
        "left_key",
        "right_table",
        "right_key",
    ).collect():
        join_id = row["join_id"]
        left_table = row["left_table"]
        right_table = row["right_table"]
        left_key = row["left_key"]
        right_key = row["right_key"]
        for table_name, side in [(left_table, "left"), (right_table, "right")]:
            if table_name not in table_names:
                errors.append(
                    f"join_catalog.{join_id} references unknown {side}_table: "
                    f"{table_name}"
                )
        left_schema = schema_cache.get(left_table)
        right_schema = schema_cache.get(right_table)
        if left_schema is None:
            errors.append(
                f"join_catalog.{join_id} left_table is unreadable: {left_table}"
            )
        elif left_key not in _field_names(left_schema):
            errors.append(
                f"join_catalog.{join_id} left_key missing: {left_table}.{left_key}"
            )
        if right_schema is None:
            errors.append(
                f"join_catalog.{join_id} right_table is unreadable: {right_table}"
            )
        elif right_key not in _field_names(right_schema):
            errors.append(
                f"join_catalog.{join_id} right_key missing: {right_table}.{right_key}"
            )

    row_counts = {
        TABLE_CATALOG: table_df.count(),
        COLUMN_CATALOG: column_df.count(),
        METRIC_CATALOG: metric_df.count(),
        JOIN_CATALOG: join_df.count(),
    }

    for df in metadata_tables.values():
        df.unpersist()

    if errors:
        raise RuntimeError("Metadata validation failed:\n- " + "\n- ".join(errors))

    log("Metadata validation passed.")
    return row_counts
