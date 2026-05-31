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

from .config import GOLD_CATALOG, GOLD_SCHEMA, GOLD_TABLES
from .trino_exec import execute_query

CACHE_TTL_SECONDS = 300
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CACHE: dict[str, Any] = {"expires_at": 0.0, "metadata": None}
# Separate cache for the richer semantic overview used by the conversational
# agent (purpose/grain/use_for + column meanings).
_OVERVIEW_CACHE: dict[str, Any] = {"expires_at": 0.0, "overview": None}
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


# ---------------------------------------------------------------------------
# Rich semantic overview (the "data model" the conversational agent learns)
# ---------------------------------------------------------------------------
#
# get_gold_metadata() above returns only table names + column name/type, which
# is all the SQL generator needs. The conversational assistant needs the
# business semantics too (display name, purpose, grain, use_for, column
# meanings) so it can explain the data and propose good questions. This is a
# separate, cached read so it never slows the deterministic SQL path.


def _normalize_catalog_table(raw_table_name: str) -> str:
    name = str(raw_table_name).strip()
    schema_prefix = f"{GOLD_SCHEMA}."
    if name.lower().startswith(schema_prefix):
        name = name[len(schema_prefix):]
    return name


def _clean_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _fetch_semantic_tables_rich() -> list[dict[str, Any]]:
    sql = (
        "SELECT table_name, display_name, purpose, grain, use_for, query_notes "
        f"FROM {SEMANTIC_TABLE_CATALOG} "
        "WHERE is_agent_visible = true "
        "ORDER BY table_name"
    )
    _columns, rows = execute_query(sql)
    return rows


def _fetch_semantic_columns_rich() -> list[dict[str, Any]]:
    sql = (
        "SELECT table_name, column_name, data_type, meaning, business_terms "
        f"FROM {SEMANTIC_COLUMN_CATALOG} "
        "WHERE is_agent_visible = true "
        "ORDER BY table_name, column_name"
    )
    _columns, rows = execute_query(sql)
    return rows


def _fallback_overview(error: str | None = None) -> dict[str, Any]:
    """Best-effort overview when the semantic catalog is unreachable/empty.

    Keeps the conversational agent useful (it can still name the Gold tables)
    without inventing business semantics it does not have.
    """
    return {
        "source": "fallback",
        "error": error,
        "tables": [
            {
                "table_name": table,
                "display_name": "",
                "purpose": "",
                "grain": "",
                "use_for": "",
                "columns": [],
            }
            for table in GOLD_TABLES
        ],
    }


def get_semantic_overview(refresh: bool = False) -> dict[str, Any]:
    """Cached, business-facing description of every agent-visible Gold table.

    Returns {"source": "catalog"|"fallback", "tables": [{table_name,
    display_name, purpose, grain, use_for, columns:[{name,type,meaning,
    business_terms}]}], ...}. Never raises; falls back to a table-name-only
    overview when Trino/the catalog is unavailable. Only a real catalog read is
    cached, so a transient outage does not get pinned for the full TTL.
    """
    now = time.time()
    if (
        not refresh
        and _OVERVIEW_CACHE["overview"] is not None
        and now < float(_OVERVIEW_CACHE["expires_at"])
    ):
        return _OVERVIEW_CACHE["overview"]

    try:
        table_rows = _fetch_semantic_tables_rich()
        column_rows = _fetch_semantic_columns_rich()
    except Exception as exc:  # noqa: BLE001 - overview is best-effort
        return _fallback_overview(error=str(exc))

    columns_by_table: dict[str, list[dict[str, str]]] = {}
    for row in column_rows:
        table = _normalize_catalog_table(row.get("table_name"))
        columns_by_table.setdefault(table, []).append(
            {
                "name": _clean_text(row.get("column_name")),
                "type": _clean_text(row.get("data_type")),
                "meaning": _clean_text(row.get("meaning")),
                "business_terms": _clean_text(row.get("business_terms")),
            }
        )

    tables = []
    for row in table_rows:
        table = _normalize_catalog_table(row.get("table_name"))
        tables.append(
            {
                "table_name": table,
                "display_name": _clean_text(row.get("display_name")),
                "purpose": _clean_text(row.get("purpose")),
                "grain": _clean_text(row.get("grain")),
                "use_for": _clean_text(row.get("use_for")),
                "columns": columns_by_table.get(table, []),
            }
        )

    if not tables:
        return _fallback_overview(error="semantic_table_catalog returned no agent-visible tables")

    overview = {"source": "catalog", "error": None, "tables": tables}
    _OVERVIEW_CACHE["overview"] = overview
    _OVERVIEW_CACHE["expires_at"] = now + CACHE_TTL_SECONDS
    return overview
