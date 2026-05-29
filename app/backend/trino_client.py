"""Shared Trino client for the backend.

The legacy CLI agent uses `code/agent/services/trino_service.py` which reads
TRINO_HOST/TRINO_PORT/TRINO_USER from env. We bridge the APP_* settings to
those names once at import so the existing module + this app-local client
both pick up the same values.

Used by:
- agent.service     (already self-bridges, kept for backward compatibility)
- catalog.service   (Phase 5)
- quickstats.router (Phase 5)
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional

import structlog

from api.settings import Settings, get_settings


log = structlog.get_logger("trino_client")

_LOCK = threading.Lock()
_CONN = None


def _bridge_env_once(settings: Settings) -> None:
    os.environ.setdefault("TRINO_HOST", settings.trino_host)
    os.environ.setdefault("TRINO_PORT", str(settings.trino_port))
    os.environ.setdefault("TRINO_USER", settings.trino_user)


def get_connection():
    """Returns a Trino dbapi connection. Lazy + cached at process scope.

    The trino-python-client connection is thread-safe enough for our case
    (per-cursor work). For multi-tenant scale we would pool one connection
    per (host, user) tuple - good enough for V1.
    """
    global _CONN
    with _LOCK:
        if _CONN is not None:
            return _CONN
        settings = get_settings()
        _bridge_env_once(settings)
        try:
            from trino.dbapi import connect
        except ImportError as exc:
            log.error("trino_client.import_failed", error=str(exc))
            raise
        _CONN = connect(
            host=settings.trino_host,
            port=settings.trino_port,
            user=settings.trino_user,
            catalog="iceberg",
            schema="metadata",
            session_properties={"query_max_execution_time": "30s"},
        )
        log.info("trino_client.connected", host=settings.trino_host, port=settings.trino_port)
        return _CONN


def _row_to_dict(cursor, row) -> Dict[str, Any]:
    names = [d[0] for d in cursor.description]
    return dict(zip(names, row))


def execute_query_to_dicts(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Run a SELECT and return a list of column-name -> value dicts.

    Raises on connection or execution errors; callers decide whether to
    surface that to the user or downgrade gracefully (e.g. quickstats does
    the latter).
    """
    conn = get_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return [_row_to_dict(cursor, row) for row in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


# ---------------------------------------------------------------------------
# Simple TTL cache used by catalog (5 min) and quickstats (60 s)
# ---------------------------------------------------------------------------


class TTLCache:
    """One-slot in-process cache.

    Not LRU - we only have one cached value per cache instance. That fits
    catalog (single dict of tables) and quickstats (single payload).
    Thread-safe via a small lock.
    """

    def __init__(self, ttl_seconds: float):
        self.ttl = ttl_seconds
        self._value: Any = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def get(self, factory):
        """Returns the cached value, or invokes `factory()` to refresh it."""
        now = time.time()
        with self._lock:
            if self._value is not None and now < self._expires_at:
                return self._value
        # Compute outside the lock so concurrent readers don't block the world.
        value = factory()
        with self._lock:
            self._value = value
            self._expires_at = time.time() + self.ttl
        return value

    def invalidate(self) -> None:
        with self._lock:
            self._value = None
            self._expires_at = 0.0
