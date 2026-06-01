from __future__ import annotations

from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from .auth import current_user, require_admin
from .integrations import airflow_request
from .settings import get_settings

router = APIRouter(prefix="/pipelines", tags=["pipelines"])

PIPELINES = [
    ("bronze_pipeline", "Bronze ingest", "bronze"),
    ("silver_pipeline", "Silver transform", "silver"),
    ("gold_pipeline", "Gold tables", "gold"),
    ("gold_metadata_pipeline", "Gold metadata", "metadata"),
]
DAG_IDS = tuple(item[0] for item in PIPELINES)


def _duration(start: Optional[str], end: Optional[str]) -> Optional[float]:
    if not start or not end:
        return None
    try:
        from datetime import datetime

        return (datetime.fromisoformat(end.replace("Z", "+00:00")) - datetime.fromisoformat(start.replace("Z", "+00:00"))).total_seconds()
    except Exception:
        return None


def _schedule_text(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        raw_value = value.get("value")
        if raw_value is not None:
            return str(raw_value)
        raw_type = value.get("__type")
        return str(raw_type) if raw_type is not None else None
    return str(value)


def _rollup_error(dag_id: str, label: str, layer: str, error: str) -> dict:
    return {
        "dag_id": dag_id,
        "label": label,
        "layer": layer,
        "schedule": None,
        "is_paused": False,
        "last_run_id": None,
        "last_run_at": None,
        "last_run_state": None,
        "last_duration_sec": None,
        "next_run_at": None,
        "row_count_after_last_run": None,
        "error": error,
    }


@router.get("")
def list_pipelines(_user: dict = Depends(current_user)) -> list[dict]:
    rows = []
    for dag_id, label, layer in PIPELINES:
        try:
            dag = airflow_request("GET", f"/api/v1/dags/{dag_id}")
            runs = airflow_request(
                "GET",
                f"/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date",
            ).get("dag_runs") or []
            run = runs[0] if runs else {}
            rows.append(
                {
                    "dag_id": dag_id,
                    "label": label,
                    "layer": layer,
                    "schedule": _schedule_text(dag.get("timetable_summary") or dag.get("schedule_interval")),
                    "is_paused": bool(dag.get("is_paused")),
                    "last_run_id": run.get("dag_run_id"),
                    "last_run_at": run.get("start_date") or run.get("logical_date"),
                    "last_run_state": run.get("state"),
                    "last_duration_sec": _duration(run.get("start_date"), run.get("end_date")),
                    "next_run_at": dag.get("next_dagrun"),
                    "row_count_after_last_run": None,
                    "error": None,
                }
            )
        except Exception as exc:  # noqa: BLE001
            rows.append(_rollup_error(dag_id, label, layer, f"Airflow unavailable: {exc.__class__.__name__}"))
    return rows


@router.get("/debug-command")
def debug_command(dag: str = Query(...), _user: dict = Depends(current_user)) -> dict:
    if dag not in DAG_IDS:
        raise HTTPException(status_code=404, detail="Unknown DAG.")
    all_dags = " ".join(DAG_IDS)
    commands = [
        "docker compose -f docker-compose.airflow.yml logs -f airflow",
        "docker exec -u root airflow sh -lc 'ls -la /opt/project/dags /opt/project/code /opt/project/data /opt/project/jars'",
        "docker exec airflow airflow dags list",
        "docker exec airflow airflow dags list-import-errors",
        f"docker exec airflow airflow dags details {dag}",
        f"docker exec airflow airflow dags list-runs -d {dag}",
        f"docker exec airflow airflow tasks list {dag} --tree",
        f"for dag in {all_dags}; do echo \"===== $dag =====\"; docker exec airflow airflow dags details \"$dag\"; docker exec airflow airflow dags list-runs -d \"$dag\"; docker exec airflow airflow tasks list \"$dag\" --tree; done",
    ]
    return {"commands": commands}


@router.post("/{dag_id}/trigger", status_code=202)
def trigger_pipeline(dag_id: str, _admin: dict = Depends(require_admin)) -> dict:
    if dag_id not in DAG_IDS:
        raise HTTPException(status_code=404, detail="Unknown DAG.")
    try:
        result = airflow_request("POST", f"/api/v1/dags/{dag_id}/dagRuns", json_body={})
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Airflow trigger failed: {exc.__class__.__name__}") from exc
    return {"dag_id": dag_id, "run_id": result.get("dag_run_id"), "state": result.get("state")}


@router.get("/{dag_id}/runs/{run_id}")
def get_run(dag_id: str, run_id: str, _user: dict = Depends(current_user)) -> dict:
    try:
        run = airflow_request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"Run not available: {exc.__class__.__name__}") from exc
    return {
        "dag_id": dag_id,
        "run_id": run.get("dag_run_id") or run_id,
        "logical_date": run.get("logical_date"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "state": run.get("state"),
        "duration_sec": _duration(run.get("start_date"), run.get("end_date")),
        "run_type": run.get("run_type"),
        "note": run.get("note"),
    }


@router.get("/{dag_id}/runs/{run_id}/tasks")
def get_tasks(dag_id: str, run_id: str, _user: dict = Depends(current_user)) -> list[dict]:
    try:
        payload = airflow_request(
            "GET",
            f"/api/v1/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances",
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"Tasks not available: {exc.__class__.__name__}") from exc
    rows = []
    for item in payload.get("task_instances") or []:
        rows.append(
            {
                "task_id": item.get("task_id"),
                "state": item.get("state"),
                "try_number": item.get("try_number") or 1,
                "max_tries": item.get("max_tries") or 1,
                "start_date": item.get("start_date"),
                "end_date": item.get("end_date"),
                "duration_sec": _duration(item.get("start_date"), item.get("end_date")),
                "operator": item.get("operator"),
            }
        )
    return rows


@router.get("/{dag_id}/runs/{run_id}/tasks/{task_id}/logs")
def get_task_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = Query(default=1, ge=1),
    _user: dict = Depends(current_user),
) -> dict:
    settings = get_settings()
    if not settings.airflow_user or not settings.airflow_password:
        raise HTTPException(status_code=503, detail="Airflow credentials are not configured.")
    path = (
        f"/api/v1/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}"
        f"/taskInstances/{task_id}/logs/{try_number}"
    )
    url = settings.airflow_base_url.rstrip("/") + path
    try:
        response = httpx.get(url, timeout=8, auth=(settings.airflow_user, settings.airflow_password))
        response.raise_for_status()
        text = response.text
    except Exception as exc:  # noqa: BLE001
        text = f"Airflow log unavailable: {exc.__class__.__name__}"
    max_chars = 100_000
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "try_number": try_number,
        "content": text[:max_chars],
        "truncated": len(text) > max_chars,
        "size_bytes": len(text.encode("utf-8")),
    }
