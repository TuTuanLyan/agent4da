"""History router.

Endpoints (all auth-required, scoped to current_user):

  GET    /history                         - paged list with filters.
  GET    /history/{run_id}                - full AskResponse-shaped detail.
  POST   /history/{run_id}/favorite       - star (idempotent).
  DELETE /history/{run_id}/favorite       - unstar (idempotent).
"""

from __future__ import annotations

from datetime import datetime, time, timezone
from typing import List, Optional
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from agent.schemas import AskKeyNumber, AskResponse, ChartSuggestion
from auth.deps import current_user
from db.base import get_db
from db.models import QueryRun, User

from .schemas import FavoriteResponse, HistoryItem, HistoryPage


log = structlog.get_logger("history.router")
router = APIRouter(prefix="/history", tags=["history"])


_VALID_STATUSES = {"running", "success", "failed", "stopped", "blocked"}


def _parse_status_filter(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    items = [s.strip().lower() for s in raw.split(",") if s.strip()]
    unknown = [s for s in items if s not in _VALID_STATUSES]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown status filter values: {', '.join(unknown)}",
        )
    return items or None


def _parse_iso_date(value: Optional[str], field: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"`{field}` must be ISO date or datetime (e.g. 2026-05-01).",
        )


def _truncate(value: Optional[str], limit: int = 240) -> str:
    if not value:
        return ""
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


# ---------------------------------------------------------------------------
# GET /history
# ---------------------------------------------------------------------------


@router.get("", response_model=HistoryPage)
def list_history(
    from_: Optional[str] = Query(default=None, alias="from"),
    to: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    favorite: Optional[bool] = Query(default=None),
    q: Optional[str] = Query(default=None, min_length=1, max_length=200),
    page: int = Query(default=1, ge=1, le=1000),
    limit: int = Query(default=25, ge=1, le=100),
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> HistoryPage:
    stmt = select(QueryRun).where(QueryRun.user_id == user.id)
    count_stmt = select(func.count(QueryRun.id)).where(QueryRun.user_id == user.id)

    dt_from = _parse_iso_date(from_, "from")
    if dt_from is not None:
        stmt = stmt.where(QueryRun.created_at >= dt_from)
        count_stmt = count_stmt.where(QueryRun.created_at >= dt_from)

    dt_to = _parse_iso_date(to, "to")
    if dt_to is not None:
        # Inclusive end: bump to end-of-day if the caller passed just a date.
        if dt_to.time() == time(0, 0):
            dt_to = dt_to.replace(hour=23, minute=59, second=59, microsecond=999_999)
        stmt = stmt.where(QueryRun.created_at <= dt_to)
        count_stmt = count_stmt.where(QueryRun.created_at <= dt_to)

    statuses = _parse_status_filter(status_filter)
    if statuses:
        stmt = stmt.where(QueryRun.status.in_(statuses))
        count_stmt = count_stmt.where(QueryRun.status.in_(statuses))

    if favorite is not None:
        stmt = stmt.where(QueryRun.is_favorite.is_(favorite))
        count_stmt = count_stmt.where(QueryRun.is_favorite.is_(favorite))

    if q:
        pattern = f"%{q.strip()}%"
        stmt = stmt.where(QueryRun.question.ilike(pattern))
        count_stmt = count_stmt.where(QueryRun.question.ilike(pattern))

    total = session.execute(count_stmt).scalar_one()

    stmt = (
        stmt.order_by(QueryRun.created_at.desc())
        .offset((page - 1) * limit)
        .limit(limit)
    )
    rows = session.execute(stmt).scalars().all()

    items = [
        HistoryItem(
            run_id=r.id,
            question=_truncate(r.question),
            status=r.status,  # type: ignore[arg-type]
            guard_status=r.guard_status,
            row_count=r.row_count or 0,
            latency_ms=r.latency_ms,
            is_favorite=bool(r.is_favorite),
            has_summary=bool(r.summary_text),
            created_at=r.created_at,
        )
        for r in rows
    ]

    return HistoryPage(
        items=items,
        total=int(total),
        page=page,
        limit=limit,
        has_next=(page * limit) < int(total),
    )


# ---------------------------------------------------------------------------
# GET /history/{run_id}
# ---------------------------------------------------------------------------


def _to_ask_response(run: QueryRun) -> AskResponse:
    key_numbers = [
        AskKeyNumber(**kn) for kn in (run.key_numbers or []) if isinstance(kn, dict)
    ]
    chart_suggestion = (
        ChartSuggestion(**run.chart_suggestion)
        if isinstance(run.chart_suggestion, dict)
        else None
    )
    return AskResponse(
        run_id=run.id,
        question=run.question,
        generated_sql=run.generated_sql,
        guard_status=run.guard_status,
        columns=list(run.columns or []),
        rows=list(run.result_json or []),
        row_count=run.row_count or 0,
        error=run.error,
        latency_ms=run.latency_ms,
        summary=run.summary_text,
        key_numbers=key_numbers,
        chart_suggestion=chart_suggestion,
        status=run.status,  # type: ignore[arg-type]
        created_at=run.created_at,
    )


def _get_owned_run(session: Session, run_id: UUID, user: User) -> QueryRun:
    run = session.get(QueryRun, run_id)
    if run is None or run.user_id != user.id:
        raise HTTPException(status_code=404, detail="Run not found.")
    return run


@router.get("/{run_id}", response_model=AskResponse)
def get_run(
    run_id: UUID,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> AskResponse:
    run = _get_owned_run(session, run_id, user)
    return _to_ask_response(run)


# ---------------------------------------------------------------------------
# Favorite toggle
# ---------------------------------------------------------------------------


@router.post("/{run_id}/favorite", response_model=FavoriteResponse)
def favorite_run(
    run_id: UUID,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> FavoriteResponse:
    run = _get_owned_run(session, run_id, user)
    if not run.is_favorite:
        run.is_favorite = True
        session.add(run)
        session.commit()
        log.info("history.favorite.add", run_id=str(run.id), user_id=str(user.id))
    return FavoriteResponse(run_id=run.id, is_favorite=True)


@router.delete("/{run_id}/favorite", response_model=FavoriteResponse)
def unfavorite_run(
    run_id: UUID,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> FavoriteResponse:
    run = _get_owned_run(session, run_id, user)
    if run.is_favorite:
        run.is_favorite = False
        session.add(run)
        session.commit()
        log.info("history.favorite.remove", run_id=str(run.id), user_id=str(user.id))
    return FavoriteResponse(run_id=run.id, is_favorite=False)
