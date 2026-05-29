"""Agent router.

  POST /agent/ask                        - synchronous run, returns full result.
  GET  /agent/stream?question=...        - SSE: step events + final result.
  POST /agent/stop                       - cancel a running run by run_id.
  GET  /agent/runs/{run_id}/export.csv   - stream cached result as CSV.
  GET  /agent/sample-questions           - chips for the Ask screen.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import UUID

import jwt
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.errors import public_error_message
from agent import cancellation
from agent.schemas import (
    AskKeyNumber,
    AskRequest,
    AskResponse,
    ChartSuggestion,
    SampleQuestionOut,
    StopRequest,
)
from agent.service import (
    apply_post_guards,
    execute_ask,
    run_agent_state,
)
from api.settings import Settings, get_settings
from auth.deps import current_user
from chart.heuristics import suggest_chart
from db.base import get_db, get_sessionmaker
from db.models import QueryRun, SampleQuestion, User, UserPreferences


log = structlog.get_logger("agent.router")
router = APIRouter(prefix="/agent", tags=["agent"])

CSV_TOKEN_TTL_SECONDS = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Helpers
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
        status=run.status,
        created_at=run.created_at,
    )


# ---------------------------------------------------------------------------
# POST /agent/ask - synchronous
# ---------------------------------------------------------------------------


@router.post("/ask", response_model=AskResponse)
async def ask(
    body: AskRequest,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> AskResponse:
    # Wrap the agent in a named task so /stop can cancel it.
    task = asyncio.current_task()
    if task is not None:
        # Best-effort: we register the route's own task under a fresh run_id
        # so it can be cancelled. The persisted run_id is generated inside
        # execute_ask, so /stop should reference the response's run_id.
        # For Phase 3 simplicity, /stop accepts a run_id and we look it up
        # in the cancellation registry which is populated by the stream
        # endpoint below. The sync /ask endpoint is not cancellable.
        pass

    run = await execute_ask(
        session=session,
        user_id=user.id,
        question=body.question,
        summarize=body.summarize,
        chart_type_hint=body.chart_type,
    )
    return _to_ask_response(run)


# ---------------------------------------------------------------------------
# GET /agent/stream - SSE
# ---------------------------------------------------------------------------


_STEPS = ["load_metadata", "build_prompt", "generate_sql", "guard_sql", "execute_sql", "summarize"]


def _sse(event: str, data: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


async def _stream_run(
    request: Request,
    *,
    user_id: str,
    question: str,
    summarize: Optional[bool],
    chart_type_hint: Optional[str],
) -> AsyncIterator[str]:
    """Run the agent in a worker thread and emit step events plus the final
    result. Persists the run synchronously after the worker completes."""
    import uuid as _uuid

    run_id = _uuid.uuid4()

    # Register an asyncio task so POST /agent/stop can cancel it.
    task = asyncio.current_task()
    if task is not None:
        cancellation.register(str(run_id), user_id, task)

    started_at = time.time()
    yield _sse("step", {"step": "starting", "status": "ok", "run_id": str(run_id)})

    state: Dict[str, Any] = {}
    error: Optional[str] = None

    try:
        # Run the synchronous LangGraph in a worker thread; emit one event
        # per step. LangGraph 1.x supports .astream which we could use for
        # true incremental updates, but it requires async nodes. The
        # simpler Phase-3 approach: emit "running" for each step in order,
        # then run the whole graph, then emit "ok" for each step.
        for step in _STEPS:
            yield _sse("step", {"step": step, "status": "running"})

        state = await run_agent_state(question, summarize, str(run_id))

        for step in _STEPS:
            yield _sse("step", {"step": step, "status": "ok"})

    except asyncio.CancelledError:
        yield _sse("step", {"step": "stopped", "status": "cancelled"})
        # Persist as 'stopped' before re-raising.
        _persist_stopped_run(run_id, user_id, question, state, started_at)
        cancellation.unregister(str(run_id))
        raise

    except Exception as exc:
        error = public_error_message(
            exc,
            "Agent run failed. Check the data catalog and model configuration, then retry.",
        )
        state["error"] = error
        log.exception("agent.stream.error", run_id=str(run_id))
        yield _sse("step", {"step": "error", "status": "error", "error": error})

    # Apply post-guards and chart suggestion.
    final_sql, guard_status = apply_post_guards(state.get("generated_sql"))
    upstream_error = state.get("error")
    if upstream_error and guard_status == "pass":
        guard_status = "blocked" if "Forbidden" in upstream_error else "error"
    rows = state.get("query_result") or []
    columns = list(rows[0].keys()) if rows else []
    chart_suggestion = suggest_chart(columns, rows)

    status_value = "success" if not error and not state.get("error") else "failed"

    run = _persist_run_sync(
        run_id=run_id,
        user_id=user_id,
        question=question,
        state=state,
        final_sql=final_sql or state.get("generated_sql"),
        guard_status=guard_status,
        chart_suggestion=chart_suggestion,
        chart_type_hint=chart_type_hint,
        started_at=started_at,
        status=status_value,
    )

    # Final payload.
    yield _sse(
        "result",
        {
            "run_id": str(run.id),
            "question": question,
            "generated_sql": run.generated_sql,
            "guard_status": run.guard_status,
            "columns": run.columns or [],
            "rows": run.result_json or [],
            "row_count": run.row_count or 0,
            "error": run.error,
            "latency_ms": run.latency_ms,
            "summary": run.summary_text,
            "key_numbers": run.key_numbers or [],
            "chart_suggestion": run.chart_suggestion,
            "status": run.status,
            "created_at": run.created_at.isoformat(),
        },
    )

    cancellation.unregister(str(run_id))


def _persist_run_sync(**kwargs) -> QueryRun:
    """Persist using a fresh session so we don't reuse the request-scoped one."""
    import uuid as _uuid

    state = kwargs["state"]
    rows = state.get("query_result") or []
    columns = list(rows[0].keys()) if rows else []
    key_numbers_raw = state.get("key_numbers") or []
    if not isinstance(key_numbers_raw, list):
        key_numbers_raw = []

    latency_ms = int((time.time() - kwargs["started_at"]) * 1000)

    SessionLocal = get_sessionmaker()
    session = SessionLocal()
    handle = cancellation.get(str(kwargs["run_id"]))
    try:
        run = QueryRun(
            id=kwargs["run_id"],
            user_id=_uuid.UUID(kwargs["user_id"]),
            question=kwargs["question"],
            generated_sql=kwargs["final_sql"],
            guard_status=kwargs["guard_status"],
            row_count=len(rows),
            latency_ms=latency_ms,
            error=state.get("error"),
            summary_text=state.get("summary"),
            key_numbers=key_numbers_raw[:4],
            chart_type=kwargs["chart_type_hint"] or ("auto" if kwargs["chart_suggestion"] else None),
            chart_suggestion=kwargs["chart_suggestion"],
            columns=columns,
            result_json=rows[:10000],
            trino_query_id=handle.trino_query_id if handle else None,
            is_favorite=False,
            status=kwargs["status"],
        )
        session.add(run)
        session.commit()
        session.refresh(run)
        return run
    finally:
        session.close()


def _persist_stopped_run(
    run_id, user_id: str, question: str, state: Dict[str, Any], started_at: float
) -> None:
    import uuid as _uuid

    latency_ms = int((time.time() - started_at) * 1000)
    handle = cancellation.get(str(run_id))
    SessionLocal = get_sessionmaker()
    session = SessionLocal()
    try:
        run = QueryRun(
            id=run_id,
            user_id=_uuid.UUID(user_id),
            question=question,
            generated_sql=state.get("generated_sql"),
            guard_status="pass",
            row_count=0,
            latency_ms=latency_ms,
            error="Stopped by user.",
            trino_query_id=handle.trino_query_id if handle else None,
            status="stopped",
        )
        session.add(run)
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()


@router.get("/stream")
async def stream(
    request: Request,
    question: str = Query(..., min_length=1, max_length=2000),
    summarize: Optional[bool] = Query(None),
    chart_type: Optional[str] = Query(None),
    user: User = Depends(current_user),
):
    async def gen() -> AsyncIterator[str]:
        async for chunk in _stream_run(
            request,
            user_id=str(user.id),
            question=question,
            summarize=summarize,
            chart_type_hint=chart_type,
        ):
            yield chunk

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# POST /agent/stop
# ---------------------------------------------------------------------------


@router.post("/stop", status_code=status.HTTP_204_NO_CONTENT)
async def stop(
    body: StopRequest,
    user: User = Depends(current_user),
) -> Response:
    handle = cancellation.get(str(body.run_id))
    if handle is None:
        # Either the run already finished or it was for someone else.
        return Response(status_code=204)
    if handle.user_id != str(user.id):
        raise HTTPException(status_code=403, detail="Run belongs to another user.")
    cancellation.cancel_trino_query(handle)
    handle.task.cancel()
    log.info("agent.stop.cancelled", run_id=str(body.run_id))
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# GET /agent/runs/{run_id}/export.csv  (with one-time signed token)
# ---------------------------------------------------------------------------


def _issue_csv_token(run_id: UUID, user: User, settings: Settings) -> str:
    payload = {
        "sub": str(user.id),
        "run_id": str(run_id),
        "type": "csv_download",
        "iat": int(time.time()),
        "exp": int(time.time()) + CSV_TOKEN_TTL_SECONDS,
        "jti": secrets.token_urlsafe(8),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg)


@router.get("/runs/{run_id}/export-token")
def issue_export_token(
    run_id: UUID,
    user: User = Depends(current_user),
    settings: Settings = Depends(get_settings),
) -> Dict[str, Any]:
    """Mint a short-lived token the browser uses with /export.csv?token=...
    so we don't expose the access JWT in a URL the user might paste."""
    return {
        "token": _issue_csv_token(run_id, user, settings),
        "expires_in": CSV_TOKEN_TTL_SECONDS,
    }


@router.get("/runs/{run_id}/export.csv")
def export_csv(
    run_id: UUID,
    token: str = Query(...),
    settings: Settings = Depends(get_settings),
    session: Session = Depends(get_db),
):
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_alg])
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid download token.")
    if payload.get("type") != "csv_download" or payload.get("run_id") != str(run_id):
        raise HTTPException(status_code=403, detail="Token does not match run.")

    user_id = payload.get("sub")
    run = session.get(QueryRun, run_id)
    if run is None or str(run.user_id) != user_id:
        raise HTTPException(status_code=404, detail="Run not found.")

    delimiter = ","
    prefs = (
        session.execute(
            select(UserPreferences).where(UserPreferences.user_id == run.user_id)
        )
        .scalar_one_or_none()
    )
    if prefs and prefs.export_delimiter:
        delimiter = prefs.export_delimiter[:1]

    columns: List[str] = list(run.columns or [])
    rows: List[Dict[str, Any]] = list(run.result_json or [])

    def gen():
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=delimiter, lineterminator="\n")
        writer.writerow(columns)
        yield buf.getvalue()
        for row in rows:
            buf.seek(0)
            buf.truncate(0)
            writer.writerow([row.get(c) for c in columns])
            yield buf.getvalue()

    filename = f"agent4da_{run.id}.csv"
    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# GET /agent/sample-questions
# ---------------------------------------------------------------------------


@router.get("/sample-questions", response_model=List[SampleQuestionOut])
def sample_questions(
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> List[SampleQuestionOut]:
    rows = (
        session.execute(
            select(SampleQuestion)
            .where(SampleQuestion.is_active.is_(True))
            .order_by(SampleQuestion.sort_order, SampleQuestion.label)
        )
        .scalars()
        .all()
    )
    return [
        SampleQuestionOut(
            id=r.id, label=r.label, question=r.question, sort_order=r.sort_order
        )
        for r in rows
    ]
