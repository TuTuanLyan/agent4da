"""Gold metadata service (ported from code/api/metadata_service.py).

Reads the project-owned semantic metadata tables instead of `SHOW TABLES` or
`information_schema`. The Trino Iceberg JDBC catalog runs with schema-version
V0 for Spark compatibility, and those generic metadata paths try to list views.
Caches the table/column map for a few minutes.
"""

from __future__ import annotations

import re
import time
from typing import Any

from .config import GOLD_CATALOG, GOLD_SCHEMA
from .trino_exec import execute_query

CACHE_TTL_SECONDS = 300
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CACHE: dict[str, Any] = {"expires_at": 0.0, "metadata": None}
SEMANTIC_TABLE_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_table_catalog"
SEMANTIC_COLUMN_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_column_catalog"


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
        {"name": str(row["column_name"]), "type": str(row["data_type"])}
        for row in rows
    ]


def _cache_is_valid() -> bool:
    return _CACHE["metadata"] is not None and time.time() < float(_CACHE["expires_at"])


def get_gold_metadata(refresh: bool = False) -> dict[str, Any]:
    if not refresh and _cache_is_valid():
        return _CACHE["metadata"]

    tables = _fetch_gold_tables()
    columns = {table: _fetch_table_columns(table) for table in tables}
    metadata = {"tables": tables, "columns": columns}
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
        "columns": {table: metadata["columns"].get(table, []) for table in selected_tables},
    }
