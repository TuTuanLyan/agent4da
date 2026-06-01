from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .db import json_ready
from .settings import bridge_agent_env, get_settings

log = logging.getLogger("agent4da.integrations")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CODE_DIR = PROJECT_ROOT / "code"
AGENT_DIR = CODE_DIR / "agent"
SPARK_DIR = CODE_DIR / "spark"


def ensure_code_paths() -> None:
    for path in (CODE_DIR, AGENT_DIR, SPARK_DIR):
        text = str(path)
        if text not in sys.path:
            sys.path.insert(0, text)


def trino_query(
    sql: str,
    *,
    catalog: str = "iceberg",
    schema: str = "gold",
    raise_on_error: bool = False,
) -> List[Dict[str, Any]]:
    settings = get_settings()
    bridge_agent_env()
    try:
        from trino.dbapi import connect

        conn = connect(
            host=settings.trino_host,
            port=settings.trino_port,
            user=settings.trino_user,
            catalog=catalog,
            schema=schema,
            max_attempts=1,
            request_timeout=8,
        )
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            names = [item[0] for item in cursor.description or []]
            return [json_ready(dict(zip(names, row))) for row in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()
    except Exception as exc:  # noqa: BLE001
        log.info("trino query failed: %s", exc)
        if raise_on_error:
            raise
        return []


def load_catalog_metadata() -> dict:
    ensure_code_paths()
    bridge_agent_env()
    try:
        from services.metadata_service import load_semantic_metadata

        return load_semantic_metadata(fallback_to_static=True)
    except Exception as exc:  # noqa: BLE001
        log.warning("metadata fallback failed: %s", exc)
        return {"source": "unavailable", "warning": str(exc), "tables": [], "columns_by_table": {}}


def probe_http(url: str, timeout: float = 2.0) -> tuple[str, Optional[int], Optional[float], Optional[str]]:
    started = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.get(url)
        latency = round((time.perf_counter() - started) * 1000, 2)
        if response.status_code < 500:
            return "ok", response.status_code, latency, None
        return "degraded", response.status_code, latency, f"HTTP {response.status_code}"
    except Exception as exc:  # noqa: BLE001
        return "down", None, None, f"{exc.__class__.__name__}: {exc}"


def airflow_request(method: str, path: str, *, json_body: Optional[dict] = None) -> dict:
    settings = get_settings()
    if not settings.airflow_user or not settings.airflow_password:
        raise RuntimeError("Airflow credentials are not configured.")
    url = settings.airflow_base_url.rstrip("/") + path
    with httpx.Client(timeout=8, auth=(settings.airflow_user, settings.airflow_password)) as client:
        response = client.request(method, url, json=json_body)
    response.raise_for_status()
    if not response.content:
        return {}
    return response.json()

