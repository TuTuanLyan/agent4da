import re
import time
from typing import Any

from trino_client import execute_query


GOLD_CATALOG = "iceberg_catalog"
GOLD_SCHEMA = "gold"
CACHE_TTL_SECONDS = 300
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CACHE: dict[str, Any] = {
    "expires_at": 0.0,
    "metadata": None,
}


def _validate_table_name(table_name: str) -> str:
    normalized = table_name.strip().lower()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"Invalid table name: {table_name}")
    return normalized


def _first_value(row: dict[str, Any]) -> Any:
    return next(iter(row.values()))


def _fetch_gold_tables() -> list[str]:
    _columns, rows = execute_query(f"SHOW TABLES FROM {GOLD_CATALOG}.{GOLD_SCHEMA}")
    return sorted(str(_first_value(row)).lower() for row in rows)


def _fetch_table_columns(table_name: str) -> list[dict[str, str]]:
    table_name = _validate_table_name(table_name)
    sql = (
        "SELECT column_name, data_type "
        f"FROM {GOLD_CATALOG}.information_schema.columns "
        f"WHERE table_schema = '{GOLD_SCHEMA}' "
        f"AND table_name = '{table_name}' "
        "ORDER BY ordinal_position"
    )
    _columns, rows = execute_query(sql)
    return [
        {
            "name": str(row["column_name"]),
            "type": str(row["data_type"]),
        }
        for row in rows
    ]


def _cache_is_valid() -> bool:
    return _CACHE["metadata"] is not None and time.time() < float(_CACHE["expires_at"])


def get_gold_metadata(refresh: bool = False) -> dict[str, Any]:
    if not refresh and _cache_is_valid():
        return _CACHE["metadata"]

    tables = _fetch_gold_tables()
    columns = {table: _fetch_table_columns(table) for table in tables}
    metadata = {
        "tables": tables,
        "columns": columns,
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
    return {
        "tables": selected_tables,
        "columns": {
            table: metadata["columns"].get(table, [])
            for table in selected_tables
        },
    }
