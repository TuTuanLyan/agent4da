"""Output table schemas for the Gold semantic metadata catalog."""

from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gold.config import COLUMN_CATALOG, JOIN_CATALOG, METRIC_CATALOG, TABLE_CATALOG


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


@dataclass(frozen=True)
class MetadataTableSpec:
    schema_sql: str
    columns: list


METADATA_TABLE_SPECS = {
    TABLE_CATALOG: MetadataTableSpec(TABLE_CATALOG_SCHEMA_SQL, TABLE_CATALOG_COLUMNS),
    COLUMN_CATALOG: MetadataTableSpec(COLUMN_CATALOG_SCHEMA_SQL, COLUMN_CATALOG_COLUMNS),
    METRIC_CATALOG: MetadataTableSpec(METRIC_CATALOG_SCHEMA_SQL, METRIC_CATALOG_COLUMNS),
    JOIN_CATALOG: MetadataTableSpec(JOIN_CATALOG_SCHEMA_SQL, JOIN_CATALOG_COLUMNS),
}
