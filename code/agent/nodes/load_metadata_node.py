from code.agent.services.trino_helper import execute_query_to_dicts

def load_metadata(connection):
    tables = execute_query_to_dicts(
        connection,
        """
        SELECT table_name, table_type, business_name, description, grain,
               is_agent_visible, recommended_for_agent
        FROM iceberg.metadata.table_catalog
        WHERE is_agent_visible = true
        """,
    )
    columns = execute_query_to_dicts(
        connection,
        """
        SELECT table_name, column_name, data_type, business_name, description,
               is_dimension, is_metric, is_time_column, is_join_key, is_unique_key,
               agent_synonyms
        FROM iceberg.metadata.column_catalog
        """,
    )
    metrics = execute_query_to_dicts(
        connection,
        """
        SELECT metric_name, business_name, description, formula_sql,
               base_table, default_time_column, aggregation_type, unit, example_question
        FROM iceberg.metadata.metric_catalog
        """,
    )
    joins = execute_query_to_dicts(
        connection,
        """
        SELECT join_id, left_table, left_key, right_table, right_key,
               relationship_type, description
        FROM iceberg.metadata.join_catalog
        """,
    )
    return {"tables": tables, "columns": columns, "metrics": metrics, "joins": joins}

from helpers.metadata_formatter import build_schema_context

def build_schema_context(metadata):

    tables = metadata["tables"]
    columns = metadata["columns"]

    lines = []

    for table in tables:

        table_name = table["table_name"]

        lines.append(f"Table: {table_name}")
        lines.append(f"Purpose: {table['description']}")
        lines.append(f"Grain: {table['grain']}")

        table_columns = [
            c["column_name"]
            for c in columns
            if c["table_name"] == table_name
        ]

        lines.append(
            "Columns: " + ", ".join(table_columns[:15])
        )

        lines.append("")

    return "\n".join(lines)

def load_metadata_node(state):
    metadata = load_metadata()

    schema_context = build_schema_context(metadata)

    return {
        "metadata_context": schema_context
    }
