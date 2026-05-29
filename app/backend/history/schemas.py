"""Pydantic schemas for the history router."""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from agent.schemas import AskResponse


class HistoryItem(BaseModel):
    run_id: UUID
    question: str
    status: Literal["running", "success", "failed", "stopped", "blocked"]
    guard_status: Optional[str] = None
    row_count: int = 0
    latency_ms: Optional[int] = None
    is_favorite: bool = False
    has_summary: bool = False
    created_at: datetime


class HistoryPage(BaseModel):
    items: List[HistoryItem]
    total: int
    page: int = Field(ge=1)
    limit: int = Field(ge=1, le=100)
    has_next: bool


class FavoriteResponse(BaseModel):
    run_id: UUID
    is_favorite: bool


# Re-export so the detail route can declare its response model crisply.
__all__ = ["HistoryItem", "HistoryPage", "FavoriteResponse", "AskResponse"]
