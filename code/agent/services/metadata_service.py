from collections import defaultdict


def load_semantic_metadata(connection=None):
    from services.trino_service import execute_query_to_dicts, get_trino_connection

    connection = connection or get_trino_connection()

    tables = execute_query_to_dicts(
        connection,
        """
        SELECT table_name, display_name, purpose, grain, use_for, query_notes
        FROM iceberg.metadata.semantic_table_catalog
        WHERE is_agent_visible = true
        ORDER BY table_name
        """,
    )
    columns = execute_query_to_dicts(
        connection,
        """
        SELECT table_name, column_name, data_type, meaning, business_terms, example_usage
        FROM iceberg.metadata.semantic_column_catalog
        WHERE is_agent_visible = true
        ORDER BY table_name, column_name
        """,
    )

    columns_by_table = defaultdict(list)
    for column in columns:
        columns_by_table[column["table_name"]].append(column)

    table_rows = []
    for table in tables:
        row = dict(table)
        row["columns"] = columns_by_table.get(table["table_name"], [])
        table_rows.append(row)

    return {
        "tables": table_rows,
        "columns_by_table": dict(columns_by_table),
    }


def build_schema_context(metadata):
    lines = [
        "AVAILABLE GOLD TABLES",
        "",
        "COLUMN RULES",
        "- Columns are table-specific. Use a column only if it is listed under the table used in FROM/JOIN.",
        "- Do not reuse a similar column name from another table.",
        "- For revenue, use the exact listed column for the chosen table, such as total_revenue or revenue.",
        "",
    ]

    for table in metadata.get("tables", []):
        columns = table.get("columns", [])
        column_names = [column["column_name"] for column in columns]
        lines.extend(
            [
                f"Table: {table['table_name']}",
                f"Display name: {table.get('display_name') or ''}",
                f"Purpose: {table.get('purpose') or ''}",
                f"Grain: {table.get('grain') or ''}",
                f"Use for: {table.get('use_for') or ''}",
                "Exact columns: " + ", ".join(column_names),
                "Query notes:",
                f"- {table.get('query_notes') or ''}",
                "Columns:",
            ]
        )

        for column in columns:
            lines.append(
                "- {column_name} ({data_type}): {meaning} Terms: {terms} Usage: {usage}".format(
                    column_name=column["column_name"],
                    data_type=column["data_type"],
                    meaning=column.get("meaning") or "",
                    terms=column.get("business_terms") or "",
                    usage=column.get("example_usage") or "",
                )
            )

        lines.append("")

    return "\n".join(lines).strip()
