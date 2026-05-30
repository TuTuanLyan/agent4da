"""Combined health probe for the Topbar pills.

GET /ops/health probes:
  - Trino  : /v1/info + SELECT 1
  - Spark  : Spark master /json/ (or /api/v1/applications)
  - Airflow: /api/v1/health (basic auth)
  - Groq   : configured-or-missing only (no live API call)

Each probe has a tight timeout (~3s) and we never fail the whole endpoint
because one service is down - a single field reports 'down' instead. The
response is cached for 15 seconds so the topbar polling 30s -> 2 probe
rounds per minute at worst.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

import httpx
import structlog
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from api.settings import Settings, get_settings
from auth.deps import current_user
from db.models import User
from trino_client import TTLCache, execute_query_to_dicts


log = structlog.get_logger("ops.health")
router = APIRouter(prefix="/ops", tags=["ops"])


Status = Literal["ok", "degraded", "down", "configured", "missing", "unknown"]


class ServiceStatus(BaseModel):
    status: Status
    version: Optional[str] = None
    workers: Optional[int] = None
    latency_ms: Optional[int] = None
    detail: Optional[str] = None


class HealthResponse(BaseModel):
    trino: ServiceStatus
    spark: ServiceStatus
    airflow: ServiceStatus
    groq: ServiceStatus
    checked_at: datetime


_CACHE = TTLCache(ttl_seconds=15)


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------


def _probe_trino() -> ServiceStatus:
    settings = get_settings()
    base = f"http://{settings.trino_host}:{settings.trino_port}"
    start = time.perf_counter()
    info_ok = False
    version: Optional[str] = None
    try:
        with httpx.Client(timeout=3.0) as c:
            r = c.get(f"{base}/v1/info")
            if r.is_success:
                info_ok = True
                version = (r.json().get("nodeVersion") or {}).get("version")
    except Exception as exc:
        return ServiceStatus(status="down", detail=f"{exc.__class__.__name__}")
    if not info_ok:
        return ServiceStatus(status="down", detail="HTTP error from /v1/info")

    # Cheap query to confirm SQL execution path.
    try:
        rows = execute_query_to_dicts("SELECT 1 AS x")
        if rows and int(rows[0].get("x", 0)) == 1:
            latency = int((time.perf_counter() - start) * 1000)
            return ServiceStatus(status="ok", version=version, latency_ms=latency)
        return ServiceStatus(status="degraded", version=version, detail="SELECT 1 returned unexpected shape")
    except Exception as exc:
        return ServiceStatus(status="degraded", version=version, detail=f"SQL: {exc.__class__.__name__}")


def _probe_spark() -> ServiceStatus:
    settings = get_settings()
    base = settings.spark_master_url.rstrip("/")
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=3.0) as c:
            r = c.get(f"{base}/json/")
            if not r.is_success:
                return ServiceStatus(status="down", detail=f"HTTP {r.status_code}")
            payload: Dict[str, Any] = r.json()
    except Exception as exc:
        return ServiceStatus(status="down", detail=f"{exc.__class__.__name__}")
    workers = payload.get("workers")
    alive = payload.get("aliveworkers") or payload.get("aliveWorkers")
    if isinstance(workers, list):
        worker_count = len(workers)
    elif isinstance(alive, int):
        worker_count = alive
    else:
        worker_count = 0
    latency = int((time.perf_counter() - start) * 1000)
    status: Status = "ok" if worker_count > 0 else "degraded"
    return ServiceStatus(
        status=status,
        workers=worker_count,
        version=payload.get("status"),
        latency_ms=latency,
        detail=None if status == "ok" else "No alive workers reported",
    )


def _probe_airflow() -> ServiceStatus:
    settings = get_settings()
    base = settings.airflow_base_url.rstrip("/") + "/api/v1"
    auth = None
    if settings.airflow_auth == "basic" and settings.airflow_user:
        auth = (settings.airflow_user, settings.airflow_password)
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=3.0, auth=auth) as c:
            r = c.get(f"{base}/health")
            if not r.is_success:
                return ServiceStatus(status="down", detail=f"HTTP {r.status_code}")
            payload: Dict[str, Any] = r.json()
    except Exception as exc:
        return ServiceStatus(status="down", detail=f"{exc.__class__.__name__}")
    latency = int((time.perf_counter() - start) * 1000)
    meta = (payload.get("metadatabase") or {}).get("status")
    sched = (payload.get("scheduler") or {}).get("status")
    if meta == "healthy" and sched == "healthy":
        return ServiceStatus(status="ok", latency_ms=latency)
    return ServiceStatus(
        status="degraded",
        latency_ms=latency,
        detail=f"metadatabase={meta}, scheduler={sched}",
    )


def _probe_groq(settings: Settings) -> ServiceStatus:
    # Plan: configured-or-missing only, no live call. Saves a Groq quota hit
    # and avoids leaking key validity to anyone who can hit /ops/health.
    if settings.groq_api_key:
        return ServiceStatus(status="configured")
    return ServiceStatus(status="missing")


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


def _build_health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        trino=_probe_trino(),
        spark=_probe_spark(),
        airflow=_probe_airflow(),
        groq=_probe_groq(settings),
        checked_at=datetime.now(timezone.utc),
    )


@router.get("/health", response_model=HealthResponse)
def get_health(user: User = Depends(current_user)) -> HealthResponse:
    return _CACHE.get(_build_health)
