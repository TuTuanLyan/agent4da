"""Thin httpx wrapper around the Airflow REST API.

Targets Airflow 2.10.x (`/api/v1`). Reads APP_AIRFLOW_BASE_URL,
APP_AIRFLOW_USER, APP_AIRFLOW_PASSWORD from settings. Only basic auth in
V1; the JWT branch (`APP_AIRFLOW_AUTH=jwt`) is a placeholder for future
deployments.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import httpx
import structlog

from api.settings import Settings, get_settings


log = structlog.get_logger("airflow_client")


class AirflowError(RuntimeError):
    """Raised when Airflow rejects a request or is unreachable.

    Carries the upstream status so the router can map it to an HTTP code.
    """

    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


def _settings() -> Settings:
    return get_settings()


def _client() -> httpx.Client:
    settings = _settings()
    auth: Optional[Tuple[str, str]] = None
    if settings.airflow_auth == "basic" and settings.airflow_user:
        auth = (settings.airflow_user, settings.airflow_password)
    return httpx.Client(
        base_url=settings.airflow_base_url.rstrip("/") + "/api/v1",
        auth=auth,
        timeout=httpx.Timeout(10.0, read=20.0),
        headers={"Accept": "application/json"},
    )


def _raise_for_response(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    try:
        detail = resp.json().get("detail") or resp.json().get("title")
    except Exception:
        detail = resp.text[:500]
    log.warning(
        "airflow.bad_response",
        url=str(resp.request.url),
        status=resp.status_code,
        detail=detail,
    )
    raise AirflowError(
        f"Airflow {resp.status_code}: {detail or 'request failed'}",
        status=503 if resp.status_code >= 500 else 502,
    )


# ---------------------------------------------------------------------------
# DAG metadata
# ---------------------------------------------------------------------------


def get_dag(dag_id: str) -> Dict[str, Any]:
    try:
        with _client() as c:
            r = c.get(f"/dags/{dag_id}")
        if r.status_code == 404:
            raise AirflowError(f"Unknown DAG '{dag_id}'", status=404)
        _raise_for_response(r)
        return r.json()
    except httpx.HTTPError as exc:
        raise AirflowError(f"Airflow unreachable: {exc.__class__.__name__}", status=503)


# ---------------------------------------------------------------------------
# DAG runs
# ---------------------------------------------------------------------------


def list_runs(
    dag_id: str,
    *,
    limit: int = 20,
    order_by: str = "-execution_date",
) -> List[Dict[str, Any]]:
    try:
        with _client() as c:
            r = c.get(
                f"/dags/{dag_id}/dagRuns",
                params={"limit": limit, "order_by": order_by},
            )
        _raise_for_response(r)
        return r.json().get("dag_runs", [])
    except httpx.HTTPError as exc:
        raise AirflowError(f"Airflow unreachable: {exc.__class__.__name__}", status=503)


def get_run(dag_id: str, run_id: str) -> Dict[str, Any]:
    try:
        with _client() as c:
            r = c.get(f"/dags/{dag_id}/dagRuns/{run_id}")
        if r.status_code == 404:
            raise AirflowError("Run not found", status=404)
        _raise_for_response(r)
        return r.json()
    except httpx.HTTPError as exc:
        raise AirflowError(f"Airflow unreachable: {exc.__class__.__name__}", status=503)


# ---------------------------------------------------------------------------
# Task instances + logs
# ---------------------------------------------------------------------------


def list_tasks(dag_id: str, run_id: str) -> List[Dict[str, Any]]:
    try:
        with _client() as c:
            r = c.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        _raise_for_response(r)
        return r.json().get("task_instances", [])
    except httpx.HTTPError as exc:
        raise AirflowError(f"Airflow unreachable: {exc.__class__.__name__}", status=503)


def get_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = 1,
    max_bytes: int = 256 * 1024,
) -> Tuple[str, bool, int]:
    """Returns (text, truncated, total_size_bytes).

    Airflow returns text/plain by default for this endpoint.
    """
    try:
        with _client() as c:
            r = c.get(
                f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
                headers={"Accept": "text/plain"},
            )
        if r.status_code == 404:
            raise AirflowError("Log not found (task may not have run yet)", status=404)
        _raise_for_response(r)
        text = r.text or ""
        total = len(text.encode("utf-8"))
        if total <= max_bytes:
            return text, False, total
        # Trim by characters from the end (most recent log content is at the bottom).
        trimmed = text[-max_bytes:]
        return trimmed, True, total
    except httpx.HTTPError as exc:
        raise AirflowError(f"Airflow unreachable: {exc.__class__.__name__}", status=503)


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------


def trigger_run(dag_id: str, conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if conf:
        payload["conf"] = conf
    try:
        with _client() as c:
            r = c.post(f"/dags/{dag_id}/dagRuns", json=payload)
        if r.status_code == 404:
            raise AirflowError(f"Unknown DAG '{dag_id}'", status=404)
        if r.status_code == 409:
            raise AirflowError("DAG run already exists (clash on run_id)", status=409)
        _raise_for_response(r)
        return r.json()
    except httpx.HTTPError as exc:
        raise AirflowError(f"Airflow unreachable: {exc.__class__.__name__}", status=503)
