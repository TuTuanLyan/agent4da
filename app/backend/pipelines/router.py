"""/pipelines router."""

from __future__ import annotations

from typing import List

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from auth.deps import current_user, require_role
from db.base import get_db
from db.models import PipelineTriggerAudit, User

from . import service
from .airflow_client import AirflowError
from .schemas import (
    DebugCommandResponse,
    PipelineRollup,
    PipelineRun,
    TaskInstance,
    TaskLogResponse,
    TriggerRequest,
    TriggerResponse,
)


log = structlog.get_logger("pipelines.router")
router = APIRouter(prefix="/pipelines", tags=["pipelines"])


def _bubble(exc: AirflowError) -> HTTPException:
    return HTTPException(status_code=exc.status, detail=str(exc))


# ---------------------------------------------------------------------------
# GET /pipelines  (rollup of the 4 DAGs)
# ---------------------------------------------------------------------------


@router.get("", response_model=List[PipelineRollup])
def rollup(user: User = Depends(current_user)) -> List[PipelineRollup]:
    return service.rollup_all()


# ---------------------------------------------------------------------------
# GET /pipelines/debug-command?dag=...
# (Declared before the dag_id path routes so FastAPI matches it.)
# ---------------------------------------------------------------------------


@router.get("/debug-command", response_model=DebugCommandResponse)
def debug_command(
    dag: str = Query(..., min_length=1, max_length=128),
    user: User = Depends(current_user),
) -> DebugCommandResponse:
    try:
        commands = service.debug_commands_for(dag)
    except AirflowError as exc:
        raise _bubble(exc)
    return DebugCommandResponse(dag_id=dag, commands=commands)


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


@router.get("/{dag_id}/runs", response_model=List[PipelineRun])
def list_runs(
    dag_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    user: User = Depends(current_user),
) -> List[PipelineRun]:
    try:
        return service.list_runs(dag_id, limit=limit)
    except AirflowError as exc:
        raise _bubble(exc)


@router.get("/{dag_id}/runs/{run_id}", response_model=PipelineRun)
def get_run(
    dag_id: str,
    run_id: str,
    user: User = Depends(current_user),
) -> PipelineRun:
    try:
        return service.get_run(dag_id, run_id)
    except AirflowError as exc:
        raise _bubble(exc)


# ---------------------------------------------------------------------------
# Tasks + logs
# ---------------------------------------------------------------------------


@router.get("/{dag_id}/runs/{run_id}/tasks", response_model=List[TaskInstance])
def list_tasks(
    dag_id: str,
    run_id: str,
    user: User = Depends(current_user),
) -> List[TaskInstance]:
    try:
        return service.list_tasks(dag_id, run_id)
    except AirflowError as exc:
        raise _bubble(exc)


@router.get(
    "/{dag_id}/runs/{run_id}/tasks/{task_id}/logs",
    response_model=TaskLogResponse,
)
def get_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = Query(default=1, ge=1, le=20),
    user: User = Depends(current_user),
) -> TaskLogResponse:
    try:
        content, truncated, size = service.get_logs(
            dag_id, run_id, task_id, try_number=try_number
        )
    except AirflowError as exc:
        raise _bubble(exc)
    return TaskLogResponse(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        content=content,
        truncated=truncated,
        size_bytes=size,
    )


# ---------------------------------------------------------------------------
# POST /pipelines/{dag_id}/trigger (admin only)
# ---------------------------------------------------------------------------


@router.post("/{dag_id}/trigger", response_model=TriggerResponse)
def trigger(
    dag_id: str,
    body: TriggerRequest,
    session: Session = Depends(get_db),
    user: User = Depends(require_role("admin")),
) -> TriggerResponse:
    audit = PipelineTriggerAudit(
        user_id=user.id,
        dag_id=dag_id,
        conf=body.conf or None,
        status="triggered",
    )
    session.add(audit)
    session.flush()  # We want the audit row even if Airflow fails next.
    try:
        run = service.trigger(dag_id, conf=body.conf)
    except AirflowError as exc:
        audit.status = "failed"
        audit.error = str(exc)
        session.commit()
        log.warning(
            "pipelines.trigger.failed",
            dag_id=dag_id,
            user_id=str(user.id),
            error=str(exc),
        )
        raise _bubble(exc)

    audit.airflow_run_id = run.get("dag_run_id")
    session.add(audit)
    session.commit()
    log.info(
        "pipelines.trigger.ok",
        dag_id=dag_id,
        airflow_run_id=audit.airflow_run_id,
        user_id=str(user.id),
    )
    return TriggerResponse(
        dag_id=dag_id,
        airflow_run_id=audit.airflow_run_id or "",
        audit_id=str(audit.id),
    )
