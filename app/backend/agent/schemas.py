"""Pydantic schemas for the agent router."""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class AskRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)
    summarize: Optional[bool] = None
    chart_type: Optional[Literal["auto", "bar", "line", "pie", "table", "scatter"]] = None


class AskKeyNumber(BaseModel):
    label: str
    value: Any
    delta: Optional[str] = None


class ChartSuggestion(BaseModel):
    chart_type: Literal["bar", "line", "pie", "scatter"]
    x: str
    y: str
    series: Optional[List[str]] = None
    sort: Optional[str] = None


class AskResponse(BaseModel):
    run_id: UUID
    question: str
    generated_sql: Optional[str] = None
    guard_status: Optional[str] = None
    columns: List[str] = Field(default_factory=list)
    rows: List[dict] = Field(default_factory=list)
    row_count: int = 0
    error: Optional[str] = None
    latency_ms: Optional[int] = None
    summary: Optional[str] = None
    key_numbers: List[AskKeyNumber] = Field(default_factory=list)
    chart_suggestion: Optional[ChartSuggestion] = None
    status: Literal["running", "success", "failed", "stopped", "blocked"] = "success"
    created_at: datetime


class StopRequest(BaseModel):
    run_id: UUID


class SampleQuestionOut(BaseModel):
    id: UUID
    label: str
    question: str
    sort_order: int
