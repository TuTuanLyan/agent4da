from services.trino_service import execute_query_to_dicts, get_trino_connection


def load_metadata(connection=None):
    connection = connection or get_trino_connection()
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


def load_metadata_node(state):
    if state.get("full_metadata"):
        return {}

    return {
        "full_metadata": load_metadata()
    }
