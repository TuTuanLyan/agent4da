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
    session_id: Optional[UUID] = None


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


class ClarificationSuggestion(BaseModel):
    label: str
    question: str
    reason: str
    intent: str
    confidence: Literal["low", "medium", "high"] = "medium"


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
    insights: List[str] = Field(default_factory=list)
    key_numbers: List[AskKeyNumber] = Field(default_factory=list)
    chart_suggestion: Optional[ChartSuggestion] = None
    agent_engine: str = "legacy"
    status: Literal["running", "success", "failed", "stopped", "blocked"] = "success"
    session_id: Optional[UUID] = None
    turn_index: Optional[int] = None
    created_at: datetime

    # --- Agent v2 fields (additive; legacy responses leave them at defaults) ---
    # `summary` continues to carry the main answer text for backward
    # compatibility; `answer` mirrors it for v2 clients.
    answer: Optional[str] = None
    chart_type: Optional[str] = None
    chart: Optional[dict] = None
    chart_data: List[dict] = Field(default_factory=list)
    retry_count: Optional[int] = None
    model_used: Optional[str] = None
    intent: Optional[str] = None
    used_tables: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    validation_notes: List[str] = Field(default_factory=list)
    confidence: Optional[str] = None
    context_used: bool = False
    resolved_question: Optional[str] = None
    agent_trace: Optional[dict] = None
    answer_type: Literal["answer", "clarification", "empty_result", "blocked", "metadata"] = "answer"
    needs_clarification: bool = False
    clarification_suggestions: List[ClarificationSuggestion] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)


class StopRequest(BaseModel):
    run_id: UUID


class SampleQuestionOut(BaseModel):
    id: UUID
    label: str
    question: str
    sort_order: int


class SessionOut(BaseModel):
    id: UUID
    title: Optional[str] = None
    is_pinned: bool = False
    pinned_at: Optional[datetime] = None
    created_at: datetime
    last_used_at: datetime


class SessionSummaryOut(SessionOut):
    run_count: int = 0
    last_question: Optional[str] = None
    last_status: Optional[Literal["running", "success", "failed", "stopped", "blocked"]] = None


class SessionUpdateRequest(BaseModel):
    title: Optional[str] = Field(default=None, max_length=200)
    is_pinned: Optional[bool] = None


class AgentFeedbackRequest(BaseModel):
    run_id: Optional[UUID] = None
    session_id: Optional[UUID] = None
    feedback_type: Literal["positive", "negative", "suggestion_click", "free_text"]
    selected_suggestion: Optional[ClarificationSuggestion] = None
    free_text: Optional[str] = Field(default=None, max_length=2000)


class AgentFeedbackResponse(BaseModel):
    id: UUID
    created_at: datetime
