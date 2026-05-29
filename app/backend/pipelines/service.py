"""Pipeline domain logic: DAG roster + Airflow response translation.

The 4 DAGs are hardcoded because they map 1:1 to the medallion layers in
this project. Future flexibility: read this map from a settings table.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import structlog

from . import airflow_client
from .airflow_client import AirflowError
from .schemas import (
    Layer,
    PipelineRollup,
    PipelineRun,
    TaskInstance,
)
from ops.scheduler import latest_row_count


log = structlog.get_logger("pipelines.service")


# Order matters: this is how the UI renders the 4 cards.
DAG_ROSTER: List[Dict[str, str]] = [
    {"dag_id": "bronze_pipeline",         "label": "Bronze",   "layer": "bronze"},
    {"dag_id": "silver_pipeline",         "label": "Silver",   "layer": "silver"},
    {"dag_id": "gold_pipeline",           "label": "Gold",     "layer": "gold"},
    {"dag_id": "gold_metadata_pipeline",  "label": "Metadata", "layer": "metadata"},
]


def _to_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _duration_sec(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    if not start or not end:
        return None
    return round((end - start).total_seconds(), 2)


def _entry_for(dag_id: str) -> Optional[Dict[str, str]]:
    for d in DAG_ROSTER:
        if d["dag_id"] == dag_id:
            return d
    return None


# ---------------------------------------------------------------------------
# Rollup
# ---------------------------------------------------------------------------


def _rollup_for(entry: Dict[str, str]) -> PipelineRollup:
    dag_id = entry["dag_id"]
    base = PipelineRollup(
        dag_id=dag_id,
        label=entry["label"],
        layer=entry["layer"],  # type: ignore[arg-type]
    )
    try:
        base.row_count_after_last_run = latest_row_count(entry["layer"])
    except Exception as exc:
        log.warning("pipelines.layer_stats_unavailable", layer=entry["layer"], error=str(exc))

    try:
        dag = airflow_client.get_dag(dag_id)
    except AirflowError as exc:
        # If the DAG is unknown (404), surface that on the card instead of
        # failing the whole rollup. Other errors degrade the same way.
        base.error = str(exc)
        return base

    base.schedule = (
        dag.get("schedule_interval", {}).get("value")
        if isinstance(dag.get("schedule_interval"), dict)
        else dag.get("schedule_interval")
    )
    base.is_paused = bool(dag.get("is_paused"))
    base.next_run_at = _to_dt(dag.get("next_dagrun"))

    try:
        runs = airflow_client.list_runs(dag_id, limit=1)
    except AirflowError as exc:
        base.error = str(exc)
        return base

    if runs:
        r = runs[0]
        start = _to_dt(r.get("start_date"))
        end = _to_dt(r.get("end_date"))
        base.last_run_id = r.get("dag_run_id")
        base.last_run_at = start or _to_dt(r.get("execution_date"))
        base.last_run_state = r.get("state")
        base.last_duration_sec = _duration_sec(start, end)

    # row_count_after_last_run is populated by the Phase 7 scheduler from
    # app.layer_stats once that lands.
    return base


def rollup_all() -> List[PipelineRollup]:
    return [_rollup_for(entry) for entry in DAG_ROSTER]


# ---------------------------------------------------------------------------
# Runs / tasks / logs
# ---------------------------------------------------------------------------


def list_runs(dag_id: str, limit: int = 20) -> List[PipelineRun]:
    if _entry_for(dag_id) is None:
        raise AirflowError(f"DAG '{dag_id}' is not part of this project", status=404)
    raw = airflow_client.list_runs(dag_id, limit=limit)
    out: List[PipelineRun] = []
    for r in raw:
        start = _to_dt(r.get("start_date"))
        end = _to_dt(r.get("end_date"))
        out.append(
            PipelineRun(
                dag_id=dag_id,
                run_id=r.get("dag_run_id"),
                logical_date=_to_dt(r.get("logical_date") or r.get("execution_date")),
                start_date=start,
                end_date=end,
                state=r.get("state"),
                duration_sec=_duration_sec(start, end),
                run_type=r.get("run_type"),
                note=r.get("note"),
            )
        )
    return out


def get_run(dag_id: str, run_id: str) -> PipelineRun:
    if _entry_for(dag_id) is None:
        raise AirflowError(f"DAG '{dag_id}' is not part of this project", status=404)
    r = airflow_client.get_run(dag_id, run_id)
    start = _to_dt(r.get("start_date"))
    end = _to_dt(r.get("end_date"))
    return PipelineRun(
        dag_id=dag_id,
        run_id=r.get("dag_run_id"),
        logical_date=_to_dt(r.get("logical_date") or r.get("execution_date")),
        start_date=start,
        end_date=end,
        state=r.get("state"),
        duration_sec=_duration_sec(start, end),
        run_type=r.get("run_type"),
        note=r.get("note"),
    )


def list_tasks(dag_id: str, run_id: str) -> List[TaskInstance]:
    raw = airflow_client.list_tasks(dag_id, run_id)
    out: List[TaskInstance] = []
    for t in raw:
        start = _to_dt(t.get("start_date"))
        end = _to_dt(t.get("end_date"))
        out.append(
            TaskInstance(
                task_id=t.get("task_id"),
                state=t.get("state"),
                try_number=int(t.get("try_number") or 1),
                max_tries=int(t.get("max_tries") or 0),
                start_date=start,
                end_date=end,
                duration_sec=_duration_sec(start, end),
                operator=t.get("operator"),
            )
        )
    return out


def get_logs(
    dag_id: str, run_id: str, task_id: str, try_number: int = 1
) -> Tuple[str, bool, int]:
    return airflow_client.get_logs(dag_id, run_id, task_id, try_number=try_number)


def trigger(dag_id: str, conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if _entry_for(dag_id) is None:
        raise AirflowError(f"DAG '{dag_id}' is not part of this project", status=404)
    return airflow_client.trigger_run(dag_id, conf=conf)


# ---------------------------------------------------------------------------
# Debug command (host-side)
# ---------------------------------------------------------------------------


def debug_commands_for(dag_id: str) -> List[str]:
    """Returns host shell commands a power user can copy to debug a DAG.

    Designed for the existing docker-compose layout. The compose file is
    `docker-compose.airflow.yml` (top-level in the repo).
    """
    entry = _entry_for(dag_id)
    if entry is None:
        raise AirflowError(f"DAG '{dag_id}' is not part of this project", status=404)
    layer = entry["layer"]
    return [
        # 1. Show recent runs (state + dates).
        f"docker exec -it airflow airflow dags list-runs -d {dag_id}",
        # 2. Tail scheduler logs to watch the next tick.
        "docker compose -f docker-compose.airflow.yml logs -f airflow",
        # 3. One-off trigger from the CLI (skip the UI).
        f"docker exec -it airflow airflow dags trigger {dag_id}",
        # 4. Tail the Spark master log (where the job actually runs).
        "docker compose -f docker-compose.spark.yml logs -f spark-master",
        # 5. Sanity-check MinIO bucket for this layer.
        f"docker exec -it minio mc ls --summarize local/{layer}",
    ]
