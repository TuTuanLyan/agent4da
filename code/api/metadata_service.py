import re
import time
from typing import Any

from trino_client import execute_query


GOLD_CATALOG = "iceberg_catalog"
GOLD_SCHEMA = "gold"
METADATA_SCHEMA = "metadata"
SEMANTIC_TABLE_CATALOG = "semantic_table_catalog"
SEMANTIC_COLUMN_CATALOG = "semantic_column_catalog"
CACHE_TTL_SECONDS = 300
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SEMANTIC_TABLE_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_table_catalog"
SEMANTIC_COLUMN_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_column_catalog"
_CACHE: dict[str, Any] = {
    "expires_at": 0.0,
    "metadata": None,
}


def _validate_table_name(table_name: str) -> str:
    normalized = table_name.strip().lower()
    schema_prefix = f"{GOLD_SCHEMA}."
    if normalized.startswith(schema_prefix):
        normalized = normalized[len(schema_prefix):]
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"Invalid table name: {table_name}")
    return normalized


def _semantic_table_names(table_name: str) -> tuple[str, str]:
    table_name = _validate_table_name(table_name)
    return table_name, f"{GOLD_SCHEMA}.{table_name}"


def _fetch_gold_tables() -> list[str]:
    sql = (
        "SELECT table_name "
        f"FROM {SEMANTIC_TABLE_CATALOG} "
        "WHERE is_agent_visible = true "
        "ORDER BY table_name"
    )
    _columns, rows = execute_query(sql)
    return sorted({_validate_table_name(str(row["table_name"])) for row in rows})


def _fetch_table_columns(table_name: str) -> list[dict[str, str]]:
    table_name, qualified_table_name = _semantic_table_names(table_name)
    sql = (
        "SELECT DISTINCT column_name, data_type "
        f"FROM {SEMANTIC_COLUMN_CATALOG} "
        "WHERE is_agent_visible = true "
        f"AND table_name IN ('{table_name}', '{qualified_table_name}') "
        "ORDER BY column_name"
    )
    _columns, rows = execute_query(sql)
    return [
        {
            "name": str(row["column_name"]),
            "type": str(row["data_type"]),
        }
        for row in rows
    ]


def _table_key(table_name: str) -> str:
    return str(table_name).strip().lower().split(".")[-1]


def _empty_semantic_metadata(reason: str | None = None) -> dict[str, Any]:
    metadata = {
        "semantic_available": False,
        "metadata_source": "technical",
        "semantic_tables": [],
        "semantic_columns": {},
    }
    if reason:
        metadata["semantic_unavailable_reason"] = reason
    return metadata


def _semantic_table_names() -> set[str]:
    try:
        _columns, rows = execute_query(f"SHOW TABLES FROM {GOLD_CATALOG}.{METADATA_SCHEMA}")
    except Exception as exc:
        return set()
    return {str(_first_value(row)).lower() for row in rows}


def _fetch_semantic_metadata() -> dict[str, Any]:
    semantic_table_names = _semantic_table_names()
    required_tables = {SEMANTIC_TABLE_CATALOG, SEMANTIC_COLUMN_CATALOG}
    if not required_tables.issubset(semantic_table_names):
        return _empty_semantic_metadata("Semantic metadata tables are not available in Trino runtime.")

    try:
        _table_columns, table_rows = execute_query(
            "SELECT table_name, display_name, purpose, grain, use_for, query_notes "
            f"FROM {GOLD_CATALOG}.{METADATA_SCHEMA}.{SEMANTIC_TABLE_CATALOG} "
            "WHERE is_agent_visible = true "
            "ORDER BY table_name"
        )
        _column_columns, column_rows = execute_query(
            "SELECT table_name, column_name, data_type, meaning, business_terms, example_usage "
            f"FROM {GOLD_CATALOG}.{METADATA_SCHEMA}.{SEMANTIC_COLUMN_CATALOG} "
            "WHERE is_agent_visible = true "
            "ORDER BY table_name, column_name"
        )
    except Exception as exc:
        return _empty_semantic_metadata(f"Semantic metadata query failed: {type(exc).__name__}")

    semantic_tables = []
    for row in table_rows:
        table_name = str(row["table_name"]).lower()
        semantic_tables.append(
            {
                "table_name": table_name,
                "table_key": _table_key(table_name),
                "display_name": str(row.get("display_name") or ""),
                "purpose": str(row.get("purpose") or ""),
                "grain": str(row.get("grain") or ""),
                "use_for": str(row.get("use_for") or ""),
                "query_notes": str(row.get("query_notes") or ""),
            }
        )

    semantic_columns: dict[str, list[dict[str, str]]] = {}
    for row in column_rows:
        table_name = str(row["table_name"]).lower()
        table_key = _table_key(table_name)
        semantic_columns.setdefault(table_key, []).append(
            {
                "table_name": table_name,
                "table_key": table_key,
                "column_name": str(row["column_name"]),
                "name": str(row["column_name"]),
                "data_type": str(row.get("data_type") or ""),
                "type": str(row.get("data_type") or ""),
                "meaning": str(row.get("meaning") or ""),
                "business_terms": str(row.get("business_terms") or ""),
                "example_usage": str(row.get("example_usage") or ""),
            }
        )

    return {
        "semantic_available": bool(semantic_tables and semantic_columns),
        "metadata_source": "semantic" if semantic_tables and semantic_columns else "technical",
        "semantic_tables": semantic_tables,
        "semantic_columns": semantic_columns,
    }


def _cache_is_valid() -> bool:
    return _CACHE["metadata"] is not None and time.time() < float(_CACHE["expires_at"])


def get_gold_metadata(refresh: bool = False) -> dict[str, Any]:
    if not refresh and _cache_is_valid():
        return _CACHE["metadata"]

    tables = _fetch_gold_tables()
    columns = {table: _fetch_table_columns(table) for table in tables}
    semantic_metadata = _fetch_semantic_metadata()
    metadata = {
        "tables": tables,
        "columns": columns,
        **semantic_metadata,
    }
    _CACHE["metadata"] = metadata
    _CACHE["expires_at"] = time.time() + CACHE_TTL_SECONDS
    return metadata


def refresh_metadata() -> dict[str, Any]:
    return get_gold_metadata(refresh=True)


def get_gold_tables(refresh: bool = False) -> list[str]:
    return list(get_gold_metadata(refresh=refresh)["tables"])


def get_table_columns(table_name: str, refresh: bool = False) -> list[dict[str, str]]:
    table_name = _validate_table_name(table_name)
    metadata = get_gold_metadata(refresh=refresh)
    if table_name not in metadata["tables"]:
        raise ValueError(f"Gold table not found: {table_name}")
    return list(metadata["columns"].get(table_name, []))


def select_metadata(table_candidates: list[str]) -> dict[str, Any]:
    metadata = get_gold_metadata()
    selected_tables = [table for table in table_candidates if table in metadata["tables"]]
    semantic_tables = [
        table
        for table in metadata.get("semantic_tables", [])
        if table.get("table_key") in selected_tables
    ]
    selected_tables = []
    for candidate in table_candidates:
        try:
            table = _validate_table_name(candidate)
        except ValueError:
            continue
        if table in metadata["tables"] and table not in selected_tables:
            selected_tables.append(table)
    return {
        "tables": selected_tables,
        "columns": {
            table: metadata["columns"].get(table, [])
            for table in selected_tables
        },
        "semantic_available": bool(metadata.get("semantic_available") and semantic_tables),
        "metadata_source": "semantic" if metadata.get("semantic_available") and semantic_tables else "technical",
        "semantic_tables": semantic_tables,
        "semantic_columns": {
            table: metadata.get("semantic_columns", {}).get(table, [])
            for table in selected_tables
        },
        "semantic_unavailable_reason": metadata.get("semantic_unavailable_reason"),
    }
