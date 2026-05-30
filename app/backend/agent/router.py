"""Agent router.

  POST   /agent/ask                       - synchronous run, returns full result.
  GET    /agent/stream?question=...       - SSE: step events + final result.
  POST   /agent/stop                      - cancel a running run by run_id.
  GET    /agent/sessions                  - list the caller's chat sessions.
  POST   /agent/sessions                  - create a new empty chat session.
  GET    /agent/sessions/current          - most-recent (or new) session.
  GET    /agent/sessions/{id}/runs        - that session's turns, oldest first.
  DELETE /agent/sessions/{id}             - delete a chat (runs kept in History).
  GET    /agent/runs/{run_id}/export.csv  - stream cached result as CSV.
  GET    /agent/sample-questions          - chips for the Ask screen.
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import io
import json
import secrets
import time
from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import UUID

import jwt
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.errors import public_error_message
from agent import cancellation
from agent.contextual_learning import rank_contextual_suggestions
from agent.schemas import (
    AskKeyNumber,
    AskRequest,
    AskResponse,
    ChartSuggestion,
    AgentFeedbackRequest,
    AgentFeedbackResponse,
    ClarificationSuggestion,
    SampleQuestionOut,
    SessionOut,
    SessionSummaryOut,
    SessionUpdateRequest,
    StopRequest,
)
from agent.service import (
    apply_post_guards,
    build_effective_question,
    compute_insights,
    create_chat_session,
    delete_chat_session,
    execute_ask,
    get_or_create_current_session,
    next_turn_index,
    recent_session_questions,
    resolve_chat_session,
    run_agent_state,
    selected_agent_engine,
    update_chat_session,
    update_session_after_run,
)
from api.settings import Settings, get_settings
from auth.deps import current_user
from chart.heuristics import suggest_chart
from db.base import get_db, get_sessionmaker
from db.models import AgentFeedback, AgentSuggestionEvent, ChatSession, QueryRun, SampleQuestion, User, UserPreferences


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
    trace = run.agent_trace if isinstance(run.agent_trace, dict) else {}
    clarification_suggestions = []
    for item in trace.get("clarification_suggestions") or []:
        if not isinstance(item, dict):
            continue
        try:
            clarification_suggestions.append(ClarificationSuggestion(**item))
        except Exception:
            continue
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
        insights=list(run.insights or []),
        key_numbers=key_numbers,
        chart_suggestion=chart_suggestion,
        agent_engine=run.agent_engine or "legacy",
        status=run.status,
        session_id=run.session_id,
        turn_index=run.turn_index,
        created_at=run.created_at,
        # v2 additive fields (None/empty for legacy runs).
        answer=run.summary_text,
        chart_type=run.chart_type,
        chart=run.chart_payload if isinstance(run.chart_payload, dict) else None,
        chart_data=list(run.chart_data or []),
        retry_count=run.retry_count,
        model_used=run.model_used,
        intent=trace.get("intent"),
        used_tables=list(trace.get("used_tables") or []),
        warnings=list(trace.get("warnings") or []),
        validation_notes=list(trace.get("validation_notes") or []),
        confidence=trace.get("confidence"),
        context_used=bool(trace.get("context_used")),
        resolved_question=trace.get("resolved_question"),
        agent_trace=run.agent_trace if isinstance(run.agent_trace, dict) else None,
        answer_type=trace.get("answer_type") or "answer",
        needs_clarification=bool(trace.get("needs_clarification")),
        clarification_suggestions=clarification_suggestions,
        assumptions=list(trace.get("assumptions") or []),
    )


def _session_summary(session: Session, chat: ChatSession) -> SessionSummaryOut:
    run_count = session.execute(
        select(func.count())
        .select_from(QueryRun)
        .where(QueryRun.session_id == chat.id, QueryRun.user_id == chat.user_id)
    ).scalar_one()
    last_run = (
        session.execute(
            select(QueryRun)
            .where(QueryRun.session_id == chat.id, QueryRun.user_id == chat.user_id)
            .order_by(QueryRun.created_at.desc(), QueryRun.id.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )
    return SessionSummaryOut(
        id=chat.id,
        title=chat.title,
        is_pinned=bool(chat.is_pinned),
        pinned_at=chat.pinned_at,
        created_at=chat.created_at,
        last_used_at=chat.last_used_at,
        run_count=int(run_count),
        last_question=last_run.question if last_run else None,
        last_status=last_run.status if last_run else None,
    )


def _session_out(chat: ChatSession) -> SessionOut:
    return SessionOut(
        id=chat.id,
        title=chat.title,
        is_pinned=bool(chat.is_pinned),
        pinned_at=chat.pinned_at,
        created_at=chat.created_at,
        last_used_at=chat.last_used_at,
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
    # The sync /ask endpoint is not cancellable; /stop targets the stream's
    # run_id via the cancellation registry.
    try:
        chat = resolve_chat_session(session, user.id, body.session_id)
    except PermissionError:
        raise HTTPException(status_code=404, detail="Chat session not found.")

    run = await execute_ask(
        session=session,
        user_id=user.id,
        question=body.question,
        summarize=body.summarize,
        chart_type_hint=body.chart_type,
        session_id=chat.id,
    )
    return _to_ask_response(run)


# ---------------------------------------------------------------------------
# /agent/sessions - manage conversations
# ---------------------------------------------------------------------------


@router.get("/sessions", response_model=List[SessionSummaryOut])
def list_sessions(
    limit: int = Query(default=50, ge=1, le=100),
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> List[SessionSummaryOut]:
    chats = (
        session.execute(
            select(ChatSession)
            .where(ChatSession.user_id == user.id)
            .order_by(
                ChatSession.is_pinned.desc(),
                ChatSession.pinned_at.desc().nullslast(),
                ChatSession.last_used_at.desc(),
                ChatSession.created_at.desc(),
            )
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [_session_summary(session, chat) for chat in chats]


@router.post("/sessions", response_model=SessionOut, status_code=status.HTTP_201_CREATED)
def create_session(
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> SessionOut:
    chat = create_chat_session(session, user.id)
    return _session_out(chat)


@router.get("/sessions/current", response_model=SessionOut)
def current_session(
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> SessionOut:
    chat = get_or_create_current_session(session, user.id)
    return _session_out(chat)


@router.patch("/sessions/{session_id}", response_model=SessionOut)
def update_session(
    session_id: UUID,
    body: SessionUpdateRequest,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> SessionOut:
    chat = update_chat_session(
        session,
        user.id,
        session_id,
        title_provided="title" in body.model_fields_set,
        title=body.title,
        is_pinned=body.is_pinned,
    )
    if chat is None:
        raise HTTPException(status_code=404, detail="Chat session not found.")
    log.info("agent.session.updated", session_id=str(session_id))
    return _session_out(chat)


@router.get("/sessions/{session_id}/runs", response_model=List[AskResponse])
def session_runs(
    session_id: UUID,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> List[AskResponse]:
    try:
        chat = resolve_chat_session(session, user.id, session_id)
    except PermissionError:
        raise HTTPException(status_code=404, detail="Chat session not found.")

    runs = (
        session.execute(
            select(QueryRun)
            .where(QueryRun.session_id == chat.id, QueryRun.user_id == user.id)
            .order_by(QueryRun.turn_index.asc().nullslast(), QueryRun.created_at.asc())
        )
        .scalars()
        .all()
    )
    return [_to_ask_response(run) for run in runs]


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_session(
    session_id: UUID,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> Response:
    # Deletes the chat thread only. The session_id FK on query_runs is
    # ON DELETE SET NULL, so the underlying runs stay available in History.
    deleted = delete_chat_session(session, user.id, session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Chat session not found.")
    log.info("agent.session.deleted", session_id=str(session_id))
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# GET /agent/stream - SSE
# ---------------------------------------------------------------------------


# Canonical UI step names (the frontend AgentStepper keys off exactly these).
_STEPS = ["load_metadata", "build_prompt", "generate_sql", "guard_sql", "execute_sql", "summarize"]

# Real v2 node name behind each canonical UI step, surfaced as an extra `node`
# field so clients can show the true v2 progression while the stepper keeps
# working against the canonical names.
_V2_STEP_NODES = {
    "load_metadata": "load_context+metadata",
    "build_prompt": "safety+nlu",
    "generate_sql": "text_to_sql",
    "guard_sql": "guard_sql",
    "execute_sql": "execute_sql+correct_sql",
    "summarize": "validate_result+visualization+insight",
}


def _sse(event: str, data: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


def _stream_context(session_id: Optional[UUID], question: str, user_id: str, engine: str = "legacy"):
    """Build follow-up context + turn index using a short-lived DB session,
    since the SSE generator runs outside the request-scoped session.

    The v2 engine resolves follow-ups internally, so it receives the raw
    question; only the legacy engine gets a context-prepended question."""
    if session_id is None:
        return question, None
    import uuid as _uuid

    SessionLocal = get_sessionmaker()
    db = SessionLocal()
    try:
        if engine == "v2":
            effective_question = question
        else:
            prior = recent_session_questions(db, session_id, _uuid.UUID(user_id))
            effective_question, _ctx = build_effective_question(question, prior)
        turn_index = next_turn_index(db, session_id)
        return effective_question, turn_index
    finally:
        db.close()


async def _stream_run(
    request: Request,
    *,
    user_id: str,
    question: str,
    summarize: Optional[bool],
    chart_type_hint: Optional[str],
    session_id: Optional[UUID] = None,
) -> AsyncIterator[str]:
    """Run the agent in a worker thread and emit step events plus the final
    result. Persists the run synchronously after the worker completes."""
    import uuid as _uuid

    engine = selected_agent_engine()
    run_id = _uuid.uuid4()
    effective_question, turn_index = _stream_context(session_id, question, user_id, engine)

    # Register an asyncio task so POST /agent/stop can cancel it.
    task = asyncio.current_task()
    if task is not None:
        cancellation.register(str(run_id), user_id, task)

    started_at = time.time()
    yield _sse("step", {"step": "starting", "status": "ok", "run_id": str(run_id), "agent_engine": engine})

    state: Dict[str, Any] = {}
    error: Optional[str] = None

    def _step_event(step: str, status: str) -> str:
        data: Dict[str, Any] = {"step": step, "status": status}
        if engine == "v2":
            data["node"] = _V2_STEP_NODES.get(step, step)
        return _sse("step", data)

    agent_task: Optional[asyncio.Task[Dict[str, Any]]] = None
    try:
        yield _step_event(_STEPS[0], "running")
        agent_task = asyncio.create_task(
            run_agent_state(
                effective_question,
                summarize,
                str(run_id),
                engine=engine,
                session_id=session_id,
                user_id=_uuid.UUID(user_id),
            )
        )

        active_step_index = 0
        heartbeat_count = 0
        while not agent_task.done():
            if await request.is_disconnected():
                raise asyncio.CancelledError()
            try:
                await asyncio.wait_for(asyncio.shield(agent_task), timeout=2.0)
                break
            except asyncio.TimeoutError:
                heartbeat_count += 1
                if active_step_index < len(_STEPS) - 1:
                    yield _step_event(_STEPS[active_step_index], "ok")
                    active_step_index += 1
                    yield _step_event(_STEPS[active_step_index], "running")
                yield _sse(
                    "heartbeat",
                    {
                        "run_id": str(run_id),
                        "step": _STEPS[active_step_index],
                        "count": heartbeat_count,
                        "elapsed_ms": int((time.time() - started_at) * 1000),
                    },
                )

        state = await agent_task
        if engine == "v2":
            SessionLocal = get_sessionmaker()
            db = SessionLocal()
            try:
                rank_contextual_suggestions(db, _uuid.UUID(user_id), state)
            finally:
                db.close()

        for step in _STEPS:
            yield _step_event(step, "ok")

    except asyncio.CancelledError:
        if agent_task and not agent_task.done():
            agent_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await agent_task
        yield _sse("step", {"step": "stopped", "status": "cancelled"})
        _persist_stopped_run(run_id, user_id, question, state, started_at, session_id, turn_index)
        cancellation.unregister(str(run_id))
        raise

    except Exception as exc:
        if agent_task and not agent_task.done():
            agent_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await agent_task
        error = public_error_message(
            exc,
            "Agent run failed. Check the data catalog and model configuration, then retry.",
        )
        state["error"] = error
        log.exception("agent.stream.error", run_id=str(run_id))
        yield _sse("step", {"step": "error", "status": "error", "error": error})

    if engine == "v2" and not error:
        # The v2 engine already guarded the SQL and produced status/chart/insights.
        final_sql = state.get("generated_sql")
        guard_status = state.get("guard_status") or "pass"
        chart_suggestion = state.get("chart_suggestion")
        insights = list(state.get("insights") or [])
        status_value = state.get("status") or "success"
    else:
        # Legacy engine (or a hard failure): apply post-guards + heuristic chart.
        final_sql, guard_status = apply_post_guards(state.get("generated_sql"))
        upstream_error = state.get("error")
        if upstream_error and guard_status == "pass":
            guard_status = "blocked" if "Forbidden" in upstream_error else "error"
        rows = state.get("query_result") or []
        columns = list(rows[0].keys()) if rows else []
        chart_suggestion = state.get("chart_suggestion") if engine == "v2" else suggest_chart(columns, rows)
        insights = [] if (error or upstream_error) else compute_insights(columns, rows, len(rows))
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
        session_id=session_id,
        turn_index=turn_index,
        insights=insights,
    )

    trace = run.agent_trace if isinstance(run.agent_trace, dict) else {}

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
            "answer": run.summary_text,
            "insights": run.insights or [],
            "key_numbers": run.key_numbers or [],
            "chart_suggestion": run.chart_suggestion,
            "chart_type": run.chart_type,
            "chart": run.chart_payload,
            "chart_data": run.chart_data or [],
            "retry_count": run.retry_count,
            "model_used": run.model_used,
            "intent": trace.get("intent"),
            "used_tables": trace.get("used_tables") or [],
            "warnings": trace.get("warnings") or [],
            "validation_notes": trace.get("validation_notes") or [],
            "confidence": trace.get("confidence"),
            "context_used": bool(trace.get("context_used")),
            "resolved_question": trace.get("resolved_question"),
            "answer_type": trace.get("answer_type") or "answer",
            "needs_clarification": bool(trace.get("needs_clarification")),
            "clarification_suggestions": trace.get("clarification_suggestions") or [],
            "assumptions": trace.get("assumptions") or [],
            "agent_engine": run.agent_engine or "legacy",
            "status": run.status,
            "session_id": str(run.session_id) if run.session_id else None,
            "turn_index": run.turn_index,
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
    session_id = kwargs.get("session_id")
    turn_index = kwargs.get("turn_index")
    insights = kwargs.get("insights") or []

    SessionLocal = get_sessionmaker()
    session = SessionLocal()
    handle = cancellation.get(str(kwargs["run_id"]))
    try:
        run = QueryRun(
            id=kwargs["run_id"],
            user_id=_uuid.UUID(kwargs["user_id"]),
            session_id=session_id,
            turn_index=turn_index,
            question=kwargs["question"],
            generated_sql=kwargs["final_sql"],
            guard_status=kwargs["guard_status"],
            row_count=len(rows),
            latency_ms=latency_ms,
            error=state.get("error"),
            summary_text=state.get("summary"),
            insights=insights,
            key_numbers=key_numbers_raw[:4],
            chart_type=(
                kwargs["chart_type_hint"]
                or state.get("chart_type")
                or ("auto" if kwargs["chart_suggestion"] else None)
            ),
            chart_suggestion=kwargs["chart_suggestion"],
            columns=columns,
            result_json=rows[:10000],
            chart_payload=state.get("chart_payload"),
            chart_data=state.get("chart_data") or None,
            agent_trace=state.get("agent_trace"),
            retry_count=state.get("retry_count"),
            model_used=state.get("model_used"),
            trino_query_id=handle.trino_query_id if handle else None,
            agent_engine=str(state.get("agent_engine") or "legacy"),
            is_favorite=False,
            status=kwargs["status"],
        )
        session.add(run)
        session.flush()
        trace = state.get("agent_trace") if isinstance(state.get("agent_trace"), dict) else {}
        suggestions = (
            state.get("clarification_suggestions")
            or trace.get("clarification_suggestions")
            or []
        )
        if suggestions:
            session.add(
                AgentSuggestionEvent(
                    user_id=_uuid.UUID(kwargs["user_id"]),
                    session_id=session_id,
                    run_id=kwargs["run_id"],
                    input_question=kwargs["question"],
                    intent=trace.get("intent"),
                    suggestions_generated=suggestions,
                    result_status=kwargs["status"],
                )
            )
        update_session_after_run(session, session_id, kwargs["question"])
        session.commit()
        session.refresh(run)
        return run
    finally:
        session.close()


def _persist_stopped_run(
    run_id,
    user_id: str,
    question: str,
    state: Dict[str, Any],
    started_at: float,
    session_id: Optional[UUID] = None,
    turn_index: Optional[int] = None,
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
            session_id=session_id,
            turn_index=turn_index,
            question=question,
            generated_sql=state.get("generated_sql"),
            guard_status="pass",
            row_count=0,
            latency_ms=latency_ms,
            error="Stopped by user.",
            trino_query_id=handle.trino_query_id if handle else None,
            agent_engine=str(state.get("agent_engine") or "legacy"),
            status="stopped",
        )
        session.add(run)
        update_session_after_run(session, session_id, question)
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
    session_id: Optional[UUID] = Query(None),
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
):
    try:
        chat = resolve_chat_session(session, user.id, session_id)
    except PermissionError:
        raise HTTPException(status_code=404, detail="Chat session not found.")
    chat_id = chat.id

    async def gen() -> AsyncIterator[str]:
        async for chunk in _stream_run(
            request,
            user_id=str(user.id),
            question=question,
            summarize=summarize,
            chart_type_hint=chart_type,
            session_id=chat_id,
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
# POST /agent/feedback - contextual-learning signal
# ---------------------------------------------------------------------------


@router.post("/feedback", response_model=AgentFeedbackResponse, status_code=status.HTTP_201_CREATED)
def record_feedback(
    body: AgentFeedbackRequest,
    session: Session = Depends(get_db),
    user: User = Depends(current_user),
) -> AgentFeedbackResponse:
    if body.run_id is not None:
        run = session.get(QueryRun, body.run_id)
        if run is None or run.user_id != user.id:
            raise HTTPException(status_code=404, detail="Run not found.")
    if body.session_id is not None:
        try:
            resolve_chat_session(session, user.id, body.session_id)
        except PermissionError:
            raise HTTPException(status_code=404, detail="Chat session not found.")

    selected = body.selected_suggestion.model_dump() if body.selected_suggestion else None
    feedback = AgentFeedback(
        user_id=user.id,
        session_id=body.session_id,
        run_id=body.run_id,
        feedback_type=body.feedback_type,
        selected_suggestion=selected,
        free_text=body.free_text,
    )
    session.add(feedback)

    if body.feedback_type == "suggestion_click" and selected:
        event = (
            session.execute(
                select(AgentSuggestionEvent)
                .where(
                    AgentSuggestionEvent.user_id == user.id,
                    AgentSuggestionEvent.run_id == body.run_id,
                )
                .order_by(AgentSuggestionEvent.created_at.desc(), AgentSuggestionEvent.id.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if event is not None:
            event.selected_suggestion = selected

    session.commit()
    session.refresh(feedback)
    return AgentFeedbackResponse(id=feedback.id, created_at=feedback.created_at)


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
