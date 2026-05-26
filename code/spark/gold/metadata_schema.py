"""Schemas for simple Gold semantic metadata tables used by the AI Agent."""

from dataclasses import dataclass

from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from gold.config import SEMANTIC_COLUMN_CATALOG, SEMANTIC_TABLE_CATALOG


SEMANTIC_TABLE_CATALOG_COLUMNS = [
    "table_name",
    "display_name",
    "purpose",
    "grain",
    "use_for",
    "query_notes",
    "is_agent_visible",
]

SEMANTIC_COLUMN_CATALOG_COLUMNS = [
    "table_name",
    "column_name",
    "data_type",
    "meaning",
    "business_terms",
    "example_usage",
    "is_agent_visible",
]

SEMANTIC_TABLE_CATALOG_SCHEMA_SQL = """
table_name STRING,
display_name STRING,
purpose STRING,
grain STRING,
use_for STRING,
query_notes STRING,
is_agent_visible BOOLEAN
""".strip()

SEMANTIC_COLUMN_CATALOG_SCHEMA_SQL = """
table_name STRING,
column_name STRING,
data_type STRING,
meaning STRING,
business_terms STRING,
example_usage STRING,
is_agent_visible BOOLEAN
""".strip()

SEMANTIC_TABLE_CATALOG_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("display_name", StringType(), False),
        StructField("purpose", StringType(), False),
        StructField("grain", StringType(), False),
        StructField("use_for", StringType(), False),
        StructField("query_notes", StringType(), False),
        StructField("is_agent_visible", BooleanType(), False),
    ]
)

SEMANTIC_COLUMN_CATALOG_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("meaning", StringType(), False),
        StructField("business_terms", StringType(), False),
        StructField("example_usage", StringType(), False),
        StructField("is_agent_visible", BooleanType(), False),
    ]
)


@dataclass(frozen=True)
class MetadataTableSpec:
    schema_sql: str
    columns: list


METADATA_TABLE_SPECS = {
    SEMANTIC_TABLE_CATALOG: MetadataTableSpec(
        SEMANTIC_TABLE_CATALOG_SCHEMA_SQL,
        SEMANTIC_TABLE_CATALOG_COLUMNS,
    ),
    SEMANTIC_COLUMN_CATALOG: MetadataTableSpec(
        SEMANTIC_COLUMN_CATALOG_SCHEMA_SQL,
        SEMANTIC_COLUMN_CATALOG_COLUMNS,
    ),
}
