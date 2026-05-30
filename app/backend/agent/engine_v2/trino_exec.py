"""Trino execution for the v2 engine.

Reuses the backend's shared, process-scoped Trino connection
(`trino_client.get_connection`, catalog `iceberg`). All v2 SQL is fully
qualified (`iceberg.gold.*`, `iceberg.metadata.*`) so the
connection's default catalog/schema is irrelevant.

Returns `(columns, rows)` with JSON-safe values, matching the shape the ported
nodes expect.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional, Tuple

import trino_client


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _register_query_id(run_id: Optional[str], cursor: Any) -> None:
    """Best-effort: record the Trino query id so POST /agent/stop can cancel it."""
    if not run_id:
        return
    query_id = getattr(cursor, "query_id", None)
    if not query_id:
        stats = getattr(cursor, "stats", None)
        if isinstance(stats, dict):
            query_id = stats.get("queryId") or stats.get("query_id")
    if not query_id:
        return
    try:
        from agent import cancellation

        cancellation.set_trino_query_id(str(run_id), str(query_id))
    except Exception:
        return


def execute_query(sql: str, run_id: Optional[str] = None) -> Tuple[List[str], List[dict]]:
    conn = trino_client.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        _register_query_id(run_id, cursor)
        columns = [str(desc[0]) for desc in (cursor.description or [])]
        rows = [
            {column: _json_value(value) for column, value in zip(columns, row)}
            for row in cursor.fetchall()
        ]
        return columns, rows
    finally:
        cursor.close()
