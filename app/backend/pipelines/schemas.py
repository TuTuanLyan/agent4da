"""Pydantic schemas for the pipelines router."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


Layer = Literal["bronze", "silver", "gold", "metadata"]


class PipelineRollup(BaseModel):
    dag_id: str
    label: str
    layer: Layer
    schedule: Optional[str] = None
    is_paused: bool = False
    last_run_id: Optional[str] = None
    last_run_at: Optional[datetime] = None
    last_run_state: Optional[str] = None
    last_duration_sec: Optional[float] = None
    next_run_at: Optional[datetime] = None
    row_count_after_last_run: Optional[int] = None  # populated by Phase 7 scheduler.
    error: Optional[str] = None  # set when Airflow couldn't be reached for this DAG only.


class PipelineRun(BaseModel):
    dag_id: str
    run_id: str
    logical_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    state: Optional[str] = None
    duration_sec: Optional[float] = None
    run_type: Optional[str] = None
    note: Optional[str] = None


class TaskInstance(BaseModel):
    task_id: str
    state: Optional[str] = None
    try_number: int = 1
    max_tries: int = 0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration_sec: Optional[float] = None
    operator: Optional[str] = None


class TaskLogResponse(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    try_number: int
    content: str
    truncated: bool
    size_bytes: int


class TriggerRequest(BaseModel):
    conf: Optional[Dict[str, Any]] = Field(default=None)


class TriggerResponse(BaseModel):
    dag_id: str
    airflow_run_id: str
    audit_id: str


class DebugCommandResponse(BaseModel):
    dag_id: str
    commands: List[str]
