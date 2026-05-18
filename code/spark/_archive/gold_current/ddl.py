"""DDL generation for Gold and metadata Iceberg tables."""

from common.iceberg import run_sql, table_name

from gold.schemas import GOLD_TABLES, METADATA_TABLES


def namespace_for_table(table_def, config):
    if table_def["namespace"] == "metadata":
        return config.metadata_namespace
    return config.gold_namespace


def quote_sql_value(value):
    return "'" + str(value).replace("'", "''") + "'"


def build_properties_sql(properties):
    lines = []
    for key, value in properties.items():
        lines.append(f"  {quote_sql_value(key)}={quote_sql_value(value)}")
    return ",\n".join(lines)


def build_create_table_sql(table_def, config):
    namespace = namespace_for_table(table_def, config)
    full_name = table_name(config.catalog_name, namespace, table_def["name"])
    columns_sql = ",\n  ".join(
        f"{column_name} {data_type}" for column_name, data_type in table_def["columns"]
    )
    partition = table_def.get("partition")
    partition_sql = f"\nPARTITIONED BY ({partition})" if partition else ""
    properties_sql = build_properties_sql(table_def["properties"])

    return f"""
CREATE TABLE IF NOT EXISTS {full_name} (
  {columns_sql}
)
USING iceberg{partition_sql}
TBLPROPERTIES (
{properties_sql}
)
"""


def create_tables(spark, config, table_defs, description):
    for table_def in table_defs:
        namespace = namespace_for_table(table_def, config)
        full_name = table_name(config.catalog_name, namespace, table_def["name"])
        run_sql(
            spark,
            build_create_table_sql(table_def, config),
            f"Creating {description} table if not exists: {full_name}",
        )


def create_gold_tables(spark, config):
    create_tables(spark, config, GOLD_TABLES, "Gold")


def create_metadata_tables(spark, config):
    create_tables(spark, config, METADATA_TABLES, "metadata")

