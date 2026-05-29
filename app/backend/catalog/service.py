"""Trino-backed reads for the semantic catalog.

The catalog tables live in `iceberg.metadata.semantic_table_catalog` and
`iceberg.metadata.semantic_column_catalog`. We expose only rows where
`is_agent_visible = true` because those are the public surface for both
the LangGraph agent and humans.

`kind` is derived locally from the table name so the UI can render a
fact/dim/summary/semantic badge without that field being on the source
table. The mapping is intentionally simple:

  fact_*               -> fact
  dim_*                -> dimension
  daily_*, *_summary   -> summary
  everything else      -> semantic
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional

import structlog

from trino_client import TTLCache, execute_query_to_dicts

from .schemas import (
    CatalogColumn,
    CatalogSearchHit,
    CatalogSearchResponse,
    CatalogTable,
    CatalogTableDetail,
    TableKind,
)


log = structlog.get_logger("catalog.service")

_TABLES_CACHE = TTLCache(ttl_seconds=300)  # 5 minutes


def classify(table_name: str) -> TableKind:
    name = (table_name or "").lower()
    if name.startswith("fact_"):
        return "fact"
    if name.startswith("dim_"):
        return "dimension"
    if name.startswith("daily_") or name.endswith("_summary"):
        return "summary"
    return "semantic"


# ---------------------------------------------------------------------------
# Raw Trino reads
# ---------------------------------------------------------------------------


def _load_tables() -> List[CatalogTable]:
    tables = execute_query_to_dicts(
        """
        SELECT table_name, display_name, purpose, grain, use_for, query_notes
        FROM iceberg.metadata.semantic_table_catalog
        WHERE is_agent_visible = true
        ORDER BY table_name
        """,
    )
    columns = execute_query_to_dicts(
        """
        SELECT table_name, column_name
        FROM iceberg.metadata.semantic_column_catalog
        WHERE is_agent_visible = true
        """,
    )
    counts: Dict[str, int] = defaultdict(int)
    for c in columns:
        counts[c["table_name"]] += 1

    return [
        CatalogTable(
            table_name=t["table_name"],
            display_name=t.get("display_name"),
            purpose=t.get("purpose"),
            grain=t.get("grain"),
            use_for=t.get("use_for"),
            query_notes=t.get("query_notes"),
            kind=classify(t["table_name"]),
            column_count=counts.get(t["table_name"], 0),
        )
        for t in tables
    ]


def list_tables() -> List[CatalogTable]:
    return _TABLES_CACHE.get(_load_tables)


def get_table_detail(table_name: str) -> Optional[CatalogTableDetail]:
    tables = {t.table_name: t for t in list_tables()}
    base = tables.get(table_name)
    if base is None:
        return None
    columns_raw = execute_query_to_dicts(
        """
        SELECT table_name, column_name, data_type, meaning, business_terms, example_usage
        FROM iceberg.metadata.semantic_column_catalog
        WHERE is_agent_visible = true AND table_name = ?
        ORDER BY column_name
        """,
        params=[table_name],
    )
    cols = [
        CatalogColumn(
            table_name=c["table_name"],
            column_name=c["column_name"],
            data_type=c.get("data_type"),
            meaning=c.get("meaning"),
            business_terms=c.get("business_terms"),
            example_usage=c.get("example_usage"),
        )
        for c in columns_raw
    ]
    return CatalogTableDetail(**base.model_dump(), columns=cols)


def list_columns(
    table_name: Optional[str] = None, q: Optional[str] = None
) -> List[CatalogColumn]:
    where = ["is_agent_visible = true"]
    params: List = []
    if table_name:
        where.append("table_name = ?")
        params.append(table_name)
    if q:
        where.append("(LOWER(column_name) LIKE ? OR LOWER(business_terms) LIKE ?)")
        like = f"%{q.lower()}%"
        params.extend([like, like])

    sql = f"""
        SELECT table_name, column_name, data_type, meaning, business_terms, example_usage
        FROM iceberg.metadata.semantic_column_catalog
        WHERE {' AND '.join(where)}
        ORDER BY table_name, column_name
        LIMIT 1000
    """
    rows = execute_query_to_dicts(sql, params=params or None)
    return [
        CatalogColumn(
            table_name=r["table_name"],
            column_name=r["column_name"],
            data_type=r.get("data_type"),
            meaning=r.get("meaning"),
            business_terms=r.get("business_terms"),
            example_usage=r.get("example_usage"),
        )
        for r in rows
    ]


def search(q: str, limit: int = 20) -> CatalogSearchResponse:
    """Search across both catalogs and return ranked hits with snippets."""
    q_norm = q.strip()
    if not q_norm:
        return CatalogSearchResponse(query=q, hits=[])

    like = f"%{q_norm.lower()}%"

    # Search tables.
    tables = execute_query_to_dicts(
        """
        SELECT table_name, display_name, purpose, use_for
        FROM iceberg.metadata.semantic_table_catalog
        WHERE is_agent_visible = true
          AND (LOWER(table_name) LIKE ?
            OR LOWER(COALESCE(display_name,'')) LIKE ?
            OR LOWER(COALESCE(purpose,'')) LIKE ?)
        LIMIT 50
        """,
        params=[like, like, like],
    )

    # Search columns.
    columns = execute_query_to_dicts(
        """
        SELECT table_name, column_name, meaning, business_terms
        FROM iceberg.metadata.semantic_column_catalog
        WHERE is_agent_visible = true
          AND (LOWER(column_name) LIKE ?
            OR LOWER(COALESCE(business_terms,'')) LIKE ?
            OR LOWER(COALESCE(meaning,'')) LIKE ?)
        LIMIT 100
        """,
        params=[like, like, like],
    )

    hits: List[CatalogSearchHit] = []
    for t in tables:
        snippet_source = t.get("purpose") or t.get("display_name") or t["table_name"]
        hits.append(
            CatalogSearchHit(
                kind="table",
                table_name=t["table_name"],
                display_name=t.get("display_name"),
                snippet=_snippet(str(snippet_source), q_norm),
            )
        )
    for c in columns:
        source = c.get("business_terms") or c.get("meaning") or c["column_name"]
        hits.append(
            CatalogSearchHit(
                kind="column",
                table_name=c["table_name"],
                column_name=c["column_name"],
                snippet=_snippet(str(source), q_norm),
            )
        )

    return CatalogSearchResponse(query=q_norm, hits=hits[:limit])


def _snippet(text: str, q: str, radius: int = 60) -> str:
    if not text:
        return ""
    lower = text.lower()
    idx = lower.find(q.lower())
    if idx < 0:
        return text[: radius * 2] + ("..." if len(text) > radius * 2 else "")
    start = max(0, idx - radius)
    end = min(len(text), idx + len(q) + radius)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(text) else ""
    return f"{prefix}{text[start:end]}{suffix}"


def invalidate_cache() -> None:
    _TABLES_CACHE.invalidate()
