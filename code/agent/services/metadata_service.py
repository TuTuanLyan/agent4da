from collections import defaultdict
import os
from pathlib import Path
import sys


CODE_DIR = Path(__file__).resolve().parents[2]
SPARK_DIR = CODE_DIR / "spark"

for path in [SPARK_DIR]:
    path_text = str(path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)


def load_static_semantic_metadata(gold_namespace="gold"):
    from gold.metadata_definitions import (
        AGENT_VISIBLE_COLUMNS,
        COMMON_COLUMN_DEFINITIONS,
        TABLE_COLUMN_OVERRIDES,
        TABLE_DEFINITIONS,
    )

    tables = []
    for table_definition in TABLE_DEFINITIONS:
        table_key = table_definition["table"]
        table_name = f"{gold_namespace}.{table_key}"
        columns = []

        for column_name in AGENT_VISIBLE_COLUMNS.get(table_key, []):
            definition = dict(COMMON_COLUMN_DEFINITIONS.get(column_name, {}))
            definition.update(
                TABLE_COLUMN_OVERRIDES.get(table_key, {}).get(column_name, {})
            )
            columns.append(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": definition.get("data_type") or "unknown",
                    "meaning": definition.get("meaning") or "",
                    "business_terms": definition.get("business_terms") or "",
                    "example_usage": definition.get("example_usage") or "",
                }
            )

        tables.append(
            {
                "table_name": table_name,
                "display_name": table_definition.get("display_name") or "",
                "purpose": table_definition.get("purpose") or "",
                "grain": table_definition.get("grain") or "",
                "use_for": table_definition.get("use_for") or "",
                "query_notes": table_definition.get("query_notes") or "",
                "columns": columns,
            }
        )

    return {
        "source": "static_definitions",
        "tables": tables,
        "columns_by_table": {
            table["table_name"]: table.get("columns", [])
            for table in tables
        },
    }


def load_semantic_metadata(connection=None, fallback_to_static=True):
    from services.trino_service import execute_query_to_dicts, get_trino_connection

    if os.getenv("AGENT_METADATA_SOURCE", "").lower() == "static":
        return load_static_semantic_metadata()

    try:
        connection = connection or get_trino_connection()

        tables = execute_query_to_dicts(
            connection,
            """
            SELECT table_name, display_name, purpose, grain, use_for, query_notes
            FROM iceberg.metadata.semantic_table_catalog
            WHERE is_agent_visible = true
            ORDER BY table_name
            """,
            raise_on_error=True,
        )
        columns = execute_query_to_dicts(
            connection,
            """
            SELECT table_name, column_name, data_type, meaning, business_terms, example_usage
            FROM iceberg.metadata.semantic_column_catalog
            WHERE is_agent_visible = true
            ORDER BY table_name, column_name
            """,
            raise_on_error=True,
        )
    except Exception as exc:
        if not fallback_to_static:
            raise

        metadata = load_static_semantic_metadata()
        metadata["warning"] = (
            "Không đọc được metadata từ Trino nên dùng metadata tĩnh trong "
            f"code/spark/gold/metadata_definitions.py. Lỗi: {type(exc).__name__}: {exc}"
        )
        return metadata

    if not tables and fallback_to_static:
        metadata = load_static_semantic_metadata()
        metadata["warning"] = (
            "Metadata table trên Trino không có bảng agent-visible nên dùng "
            "metadata tĩnh trong code/spark/gold/metadata_definitions.py."
        )
        return metadata

    columns_by_table = defaultdict(list)
    for column in columns:
        columns_by_table[column["table_name"]].append(column)

    table_rows = []
    for table in tables:
        row = dict(table)
        row["columns"] = columns_by_table.get(table["table_name"], [])
        table_rows.append(row)

    return {
        "source": "trino",
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


def summarize_metadata(metadata):
    tables = metadata.get("tables") or []
    return {
        "source": metadata.get("source") or "unknown",
        "warning": metadata.get("warning"),
        "table_count": len(tables),
        "column_count": sum(len(table.get("columns") or []) for table in tables),
        "tables": [
            {
                "table_name": table.get("table_name"),
                "display_name": table.get("display_name"),
                "grain": table.get("grain"),
                "use_for": table.get("use_for"),
                "columns": [
                    column.get("column_name")
                    for column in table.get("columns") or []
                ],
            }
            for table in tables
        ],
    }
