"""
Prometheus instrumentation for the FastAPI / AI-Agent backend.

This module is the backend half of the SEPARATE monitoring/observability layer.
It:
  * defines HTTP, /ask, and ETL-pipeline metrics,
  * exposes the scrape endpoint ``GET /metrics`` (for Prometheus only — NOT a
    user-facing App UI route; it has no auth and returns plaintext exposition),
  * runs a tiny background thread that mirrors Airflow DAG run state (sourced
    via the existing Airflow REST integration) into Prometheus gauges, so the
    scrape latency is never coupled to Airflow availability.

Agent-side metrics (Trino query / SQL-generation timings) are registered
separately by ``code/agent/services/obs_metrics.py`` in the same process and
the same default registry, so they appear at this same ``/metrics`` endpoint.

If ``prometheus_client`` is not installed, the endpoint still responds and every
helper degrades to a no-op, so the backend never breaks.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime

from fastapi import APIRouter, Response

log = logging.getLogger("agent4da.obs")

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    _ENABLED = True
except Exception:  # pragma: no cover
    _ENABLED = False

router = APIRouter(tags=["observability"])


# ---------------------------------------------------------------------------
# Metric definitions (Counters are named without the _total suffix; the client
# appends it, producing e.g. agent4da_ask_requests_total).
# ---------------------------------------------------------------------------
if _ENABLED:
    HTTP_REQUESTS = Counter(
        "agent4da_http_requests", "Total HTTP requests", ["method", "path", "status"]
    )
    HTTP_LATENCY = Histogram(
        "agent4da_http_request_duration_seconds",
        "HTTP request duration in seconds",
        ["method", "path"],
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    )

    ASK_REQUESTS = Counter("agent4da_ask_requests", "Total /agent/ask requests")
    ASK_SUCCESS = Counter("agent4da_ask_success", "Successful /agent/ask requests")
    ASK_ERRORS = Counter(
        "agent4da_ask_errors", "Failed /agent/ask requests", ["error_type"]
    )
    ASK_DURATION = Histogram(
        "agent4da_ask_duration_seconds",
        "Total AI Agent /ask response time in seconds",
        buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120),
    )
    ASK_RETRIES = Counter(
        "agent4da_ask_retries", "Total agent SQL retries accumulated across /ask"
    )
    ASK_IN_PROGRESS = Gauge("agent4da_ask_in_progress", "In-flight /ask requests")

    # ETL pipeline gauges (populated by the background refresher below).
    ETL_STATUS = Gauge(
        "agent4da_etl_pipeline_last_run_status",
        "Last DAG run status (1=success, 0=failed, -1=unknown/running/none)",
        ["dag_id", "layer"],
    )
    ETL_DURATION = Gauge(
        "agent4da_etl_pipeline_last_duration_seconds",
        "Duration of the last DAG run in seconds",
        ["dag_id", "layer"],
    )
    ETL_LAST_SUCCESS = Gauge(
        "agent4da_etl_pipeline_last_success_timestamp_seconds",
        "Unix timestamp of the last SUCCESSFUL DAG run",
        ["dag_id", "layer"],
    )
    ETL_PAUSED = Gauge(
        "agent4da_etl_pipeline_paused", "1 if the DAG is paused", ["dag_id", "layer"]
    )
    ETL_UP = Gauge(
        "agent4da_etl_collector_up", "1 if Airflow is reachable from the backend"
    )


# ---------------------------------------------------------------------------
# Scrape endpoint
# ---------------------------------------------------------------------------
if _ENABLED:

    @router.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

else:  # pragma: no cover

    @router.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(
            "# prometheus_client is not installed; install it to expose metrics.\n",
            media_type="text/plain",
        )


# ---------------------------------------------------------------------------
# HTTP + /ask helpers
# ---------------------------------------------------------------------------
def observe_http(method: str, path: str, status: int, duration: float) -> None:
    if not _ENABLED:
        return
    try:
        HTTP_REQUESTS.labels(method=method, path=path, status=str(status)).inc()
        HTTP_LATENCY.labels(method=method, path=path).observe(duration)
    except Exception:  # pragma: no cover
        pass


def classify_ask_error(payload: dict) -> str:
    err = (payload.get("error") or "")
    guard = (payload.get("guard_status") or "")
    low = err.lower()
    if guard == "blocked" or "guard" in low or "read-only" in low or "readonly" in low or "not allowed" in low:
        return "sql_guard"
    if low.startswith("trino query failed") or "trino" in low:
        return "trino"
    if "groq" in low or "llm" in low or "openai" in low or "api key" in low or "rate limit" in low:
        return "llm"
    if "persistence failed" in low:
        return "persistence"
    if "metadata" in low:
        return "metadata"
    if payload.get("needs_clarification"):
        return "validation"
    return "other" if err else "unknown"


def observe_ask(payload: dict, duration: float) -> None:
    if not _ENABLED:
        return
    try:
        ASK_REQUESTS.inc()
        ASK_DURATION.observe(max(duration, 0.0))
        retries = payload.get("retry_count")
        if isinstance(retries, (int, float)) and retries > 0:
            ASK_RETRIES.inc(retries)
        if payload.get("status") == "success" and not payload.get("error"):
            ASK_SUCCESS.inc()
        else:
            ASK_ERRORS.labels(error_type=classify_ask_error(payload)).inc()
    except Exception:  # pragma: no cover
        pass


# ---------------------------------------------------------------------------
# ETL pipeline metrics — mirror Airflow DAG run state into gauges.
# Reuses the existing Airflow REST integration so no new Airflow setup is
# required. Runs in a daemon thread to keep /metrics scrapes fast.
# ---------------------------------------------------------------------------
def _epoch(value) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _duration_seconds(start, end) -> float | None:
    s, e = _epoch(start), _epoch(end)
    if s is None or e is None:
        return None
    return max(e - s, 0.0)


def _refresh_etl_once() -> None:
    # Imported lazily to avoid import cycles at module load.
    from .integrations import airflow_request
    from .pipelines import PIPELINES

    reachable = 0
    for dag_id, _label, layer in PIPELINES:
        try:
            dag = airflow_request("GET", f"/api/v1/dags/{dag_id}")
            runs = (
                airflow_request(
                    "GET",
                    f"/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date",
                ).get("dag_runs")
                or []
            )
            reachable = 1
            run = runs[0] if runs else {}
            state = (run.get("state") or "").lower()
            status = 1.0 if state == "success" else (0.0 if state in ("failed", "upstream_failed") else -1.0)
            ETL_STATUS.labels(dag_id=dag_id, layer=layer).set(status)
            ETL_PAUSED.labels(dag_id=dag_id, layer=layer).set(1.0 if dag.get("is_paused") else 0.0)
            dur = _duration_seconds(run.get("start_date"), run.get("end_date"))
            if dur is not None:
                ETL_DURATION.labels(dag_id=dag_id, layer=layer).set(dur)
            # Last successful run timestamp (for freshness / staleness alerts).
            sruns = (
                airflow_request(
                    "GET",
                    f"/api/v1/dags/{dag_id}/dagRuns?limit=1&state=success&order_by=-execution_date",
                ).get("dag_runs")
                or []
            )
            if sruns:
                ts = _epoch(sruns[0].get("end_date") or sruns[0].get("start_date"))
                if ts is not None:
                    ETL_LAST_SUCCESS.labels(dag_id=dag_id, layer=layer).set(ts)
        except Exception:
            # Airflow unreachable / DAG missing: mark unknown, keep series present.
            ETL_STATUS.labels(dag_id=dag_id, layer=layer).set(-1.0)
    ETL_UP.set(reachable)


_REFRESHER_STARTED = False


def start_etl_refresher() -> None:
    """Start the background ETL metrics refresher (idempotent)."""
    global _REFRESHER_STARTED
    if not _ENABLED or _REFRESHER_STARTED:
        return
    _REFRESHER_STARTED = True
    interval = float(os.getenv("ETL_METRICS_REFRESH_SECONDS", "30"))

    def _loop() -> None:
        while True:
            try:
                _refresh_etl_once()
            except Exception as exc:  # pragma: no cover
                log.debug("ETL metrics refresh failed: %s", exc)
            time.sleep(interval)

    threading.Thread(target=_loop, name="etl-metrics-refresher", daemon=True).start()
    log.info("ETL metrics refresher started (interval=%ss)", interval)
