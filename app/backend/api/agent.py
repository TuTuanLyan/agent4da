from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

import jwt
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from . import cache
from .auth import current_user
from .db import db_conn, json_param, json_ready
from .integrations import ensure_code_paths
from .obs_metrics import observe_ask
from .settings import bridge_agent_env, get_settings

router = APIRouter(prefix="/agent", tags=["agent"])

RUN_TASKS: dict[str, asyncio.Task] = {}
UI_STEPS = ["load_metadata", "build_prompt", "generate_sql", "guard_sql", "execute_sql", "summarize"]
ALLOWED_CHART_TYPES = {"auto", "bar", "line", "pie", "table", "scatter"}


class AskRequest(BaseModel):
    question: str = Field(min_length=1)
    summarize: Optional[bool] = True
    chart_type: Optional[str] = "auto"
    session_id: Optional[str] = None


class StopRequest(BaseModel):
    run_id: str


class SessionPatch(BaseModel):
    title: Optional[str] = None
    is_pinned: Optional[bool] = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def title_from_question(question: str) -> str:
    title = " ".join(question.strip().split())
    if not title:
        return "Cuoc tro chuyen moi"
    return title[:77] + "..." if len(title) > 80 else title


def create_session_row(conn, user_id: UUID) -> dict:
    session_id = uuid4()
    row = conn.execute(
        """
        INSERT INTO app.chat_sessions (id, user_id)
        VALUES (%s, %s)
        RETURNING *
        """,
        (session_id, user_id),
    ).fetchone()
    return row


def get_owned_session(conn, user_id: UUID, session_id: str | UUID) -> Optional[dict]:
    try:
        sid = UUID(str(session_id))
    except ValueError:
        return None
    return conn.execute(
        "SELECT * FROM app.chat_sessions WHERE id = %s AND user_id = %s",
        (sid, user_id),
    ).fetchone()


def resolve_session(conn, user_id: UUID, session_id: Optional[str]) -> dict:
    if session_id:
        existing = get_owned_session(conn, user_id, session_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Chat session not found.")
        return existing
    return create_session_row(conn, user_id)


def session_out(row: dict) -> dict:
    return {
        "id": str(row["id"]),
        "title": row.get("title"),
        "is_pinned": bool(row.get("is_pinned")),
        "pinned_at": json_ready(row.get("pinned_at")),
        "created_at": json_ready(row.get("created_at")),
        "last_used_at": json_ready(row.get("last_used_at")),
    }


def session_summary(conn, row: dict) -> dict:
    count = conn.execute(
        "SELECT count(*) AS n FROM app.query_runs WHERE session_id = %s AND user_id = %s",
        (row["id"], row["user_id"]),
    ).fetchone()["n"]
    last = conn.execute(
        """
        SELECT question, status
        FROM app.query_runs
        WHERE session_id = %s AND user_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (row["id"], row["user_id"]),
    ).fetchone()
    out = session_out(row)
    out.update(
        {
            "run_count": int(count),
            "last_question": last["question"] if last else None,
            "last_status": last["status"] if last else None,
        }
    )
    return out


def chart_suggestion(columns: list[str], rows: list[dict], source: Optional[dict] = None) -> Optional[dict]:
    source = source or {}
    chart_type = source.get("chart_type") or source.get("type")
    x_col = source.get("x")
    y_col = source.get("y")
    if chart_type in {"bar", "line", "pie", "scatter"} and x_col in columns and y_col in columns:
        return {"chart_type": chart_type, "x": x_col, "y": y_col, "series": [], "sort": None}

    if not rows or len(columns) < 2:
        return None
    numeric = []
    labels = []
    for col in columns:
        values = [row.get(col) for row in rows[:25]]
        if any(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
            numeric.append(col)
        else:
            labels.append(col)
    if labels and numeric:
        return {"chart_type": "bar", "x": labels[0], "y": numeric[0], "series": [], "sort": "desc"}
    return None


def key_numbers(columns: list[str], rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    out = []
    for col in columns:
        values = [row.get(col) for row in rows if isinstance(row.get(col), (int, float))]
        if values:
            out.append({"label": col, "value": sum(values), "delta": None})
        if len(out) >= 4:
            break
    return out


def normalize_chart_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = str(value).strip().lower()
    return normalized if normalized in ALLOWED_CHART_TYPES else None


def provider_for_model(model: Optional[str]) -> Optional[str]:
    if not model:
        return None
    normalized = model.strip().lower()
    if normalized.startswith("gemini-"):
        return "gemini"
    if normalized.startswith("llama-") or normalized.startswith("mixtral-") or normalized.startswith("gemma-"):
        return "groq"
    return None


def resolve_llm_preferences(user: dict) -> tuple[Optional[str], Optional[str]]:
    preferences = user.get("preferences") or {}
    model = (preferences.get("default_model") or "").strip() or None
    return provider_for_model(model), model


def build_failed_result(run_id: str, question: str, session_id: Optional[str], error: str, started: float) -> dict:
    return {
        "run_id": run_id,
        "question": question,
        "generated_sql": None,
        "guard_status": "error",
        "columns": [],
        "rows": [],
        "row_count": 0,
        "error": error,
        "latency_ms": round((time.perf_counter() - started) * 1000),
        "summary": error,
        "answer": error,
        "insights": [],
        "key_numbers": [],
        "chart_suggestion": None,
        "chart_type": None,
        "chart": None,
        "chart_data": [],
        "agent_engine": get_settings().normalized_agent_engine,
        "status": "failed",
        "session_id": session_id,
        "turn_index": None,
        "answer_type": "blocked",
        "needs_clarification": False,
        "clarification_suggestions": [],
        "assumptions": [],
        "retry_count": None,
        "model_used": None,
        "intent": None,
        "used_tables": [],
        "warnings": [],
        "validation_notes": [],
        "confidence": None,
        "context_used": False,
        "resolved_question": None,
        "created_at": utc_now().isoformat(),
    }


# Number of prior turns from the same session to feed back into the model as
# follow-up context. Keep small so the prompt stays focused and cheap.
MAX_CONTEXT_TURNS = int(os.getenv("AGENT_MAX_CONTEXT_TURNS", "4"))


def build_app_context_from_runs(conn, session_id: str | UUID, max_turns: int = MAX_CONTEXT_TURNS) -> dict:
    """Build the compact `app_context` the SQL graph expects from prior turns.

    The live app path persists every turn into app.query_runs, so we can rebuild
    same-session memory straight from there - no dependency on the agent
    service's separate Trino->Postgres context store. The shape returned here
    matches what code/agent/nodes/build_prompt_node.build_app_context consumes.
    """
    try:
        sid = UUID(str(session_id))
    except (ValueError, TypeError):
        return {}

    rows = conn.execute(
        """
        SELECT question, generated_sql, columns, rows, chart_suggestion,
               chart_type, status
        FROM app.query_runs
        WHERE session_id = %s AND status = 'success'
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (sid, max(1, int(max_turns))),
    ).fetchall()

    if not rows:
        return {}

    # rows came back newest-first; flip to chronological for a natural summary.
    turns = list(reversed(rows))
    latest = turns[-1]

    summary_parts: list[str] = []
    for turn in turns[-3:]:
        question = (turn.get("question") or "").strip()
        sql = (turn.get("generated_sql") or "").strip()
        columns = ", ".join(turn.get("columns") or [])
        if question:
            summary_parts.append(f"User asked: {question}")
        if columns:
            summary_parts.append(f"Returned columns: {columns}")
        if sql:
            summary_parts.append(f"Executed SQL: {sql}")

    sample_rows = json_ready((latest.get("rows") or [])[:3])
    chart = latest.get("chart_suggestion") or {}
    if not isinstance(chart, dict):
        chart = {}

    return {
        "conversation_summary": "\n".join(summary_parts),
        "last_question": latest.get("question") or "",
        "last_sql": latest.get("generated_sql") or "",
        "last_result_columns": latest.get("columns") or [],
        "last_result_sample": sample_rows,
        "last_chart_suggestion": chart,
        "last_answer_kind": "data" if (latest.get("columns") or sample_rows) else "",
    }


# Bumping the "v1" suffix invalidates a whole cache family at once.
SCHEMA_CACHE_KEY = cache.make_key("schema_ctx", "v1")


def load_session_context(conn, session_id: str | UUID) -> dict:
    """Same-session follow-up context, served from Redis when warm.

    On a cache miss we rebuild from app.query_runs and store the result. The
    entry is invalidated by invalidate_session_context() whenever a new turn is
    persisted, so it never goes stale within a session.
    """
    key = cache.make_key("sessctx", "v1", session_id)
    cached = cache.get_json(key)
    if cached is not None:
        return cached
    ctx = build_app_context_from_runs(conn, session_id)
    if ctx:
        cache.set_json(key, ctx, get_settings().cache_session_ttl)
    return ctx


def invalidate_session_context(session_id: str | UUID) -> None:
    cache.delete(cache.make_key("sessctx", "v1", session_id))


def answer_cache_key(
    user_id: str,
    session_id: str,
    question: str,
    chart_type: Optional[str],
    llm_model: Optional[str],
    app_context: Optional[dict],
) -> str:
    """A cached answer is only reused for the *same* user, session, question,
    chart type, model AND conversation context - so a follow-up like
    'what about February?' can never collide with the same words in another
    session or after the context has moved on.
    """
    fp = cache.fingerprint(
        (question or "").strip().lower(),
        chart_type or "auto",
        llm_model or "",
        app_context or {},
    )
    return cache.make_key("ans", "v1", user_id, session_id, fp)


def load_schema_context() -> Optional[dict]:
    """Cached {schema_context, source, warning} or None on miss."""
    return cache.get_json(SCHEMA_CACHE_KEY)


def store_schema_context(state: dict) -> None:
    schema_context = state.get("schema_context")
    if not schema_context:
        return
    cache.set_json(
        SCHEMA_CACHE_KEY,
        {
            "schema_context": schema_context,
            "source": state.get("metadata_source"),
            "warning": state.get("metadata_warning"),
        },
        get_settings().cache_schema_ttl,
    )


def run_graph_sync(
    question: str,
    run_id: str,
    session_id: str,
    user_id: str,
    chart_type: Optional[str],
    llm_provider: Optional[str],
    llm_model: Optional[str],
    app_context: Optional[dict] = None,
    schema_context: Optional[str] = None,
) -> dict:
    ensure_code_paths()
    bridge_agent_env()
    from graph.sql_graph import graph
    from services.llm_service import llm_runtime

    initial_state = {
        "user_question": question,
        "request_id": run_id,
        "session_id": session_id,
        "user_id": user_id,
        "app_context": app_context or {},
        "max_retries": 3,
        "max_requery_rounds": 1,
        "chart_type_requested": chart_type or "auto",
    }
    # Seeding schema_context lets load_metadata_node short-circuit its Trino
    # round-trip (see code/agent/nodes/load_metadata_node.py).
    if schema_context:
        initial_state["schema_context"] = schema_context

    with llm_runtime(provider=llm_provider, model=llm_model):
        return graph.invoke(initial_state)


def normalize_graph_result(
    state: dict,
    run_id: str,
    question: str,
    session_id: str,
    started: float,
    llm_provider: Optional[str],
    llm_model: Optional[str],
) -> dict:
    final = state.get("final_answer") or {}
    result = final.get("result") or {}
    rows = json_ready(result.get("rows") or state.get("query_result") or [])
    columns = list(result.get("columns") or (list(rows[0].keys()) if rows else []))
    row_count = int(result.get("row_count") or len(rows))
    sql = final.get("sql") or state.get("generated_sql")
    error = final.get("error") or state.get("error")
    summary = final.get("text_answer") or state.get("insight_summary") or error
    chart_src = final.get("chart_suggestion") or (final.get("visualization") or {}).get("chart_spec") or state.get("chart_spec")
    chart_type = normalize_chart_type((chart_src or {}).get("chart_type") if isinstance(chart_src, dict) else None)
    trace = {
        "metadata": final.get("metadata"),
        "safety": final.get("safety"),
        "sql_validation": final.get("sql_validation"),
        "result_validation": (final.get("analysis") or {}).get("result_validation"),
        "missing_info": (final.get("analysis") or {}).get("missing_info"),
        "llm_provider": llm_provider or os.getenv("AGENT_LLM_PROVIDER", "auto"),
        "model_used": llm_model,
    }
    missing = trace.get("missing_info") or {}
    created_at = utc_now().isoformat()
    return {
        "run_id": run_id,
        "question": question,
        "generated_sql": sql,
        "guard_status": "blocked" if error and not sql else "pass",
        "columns": columns,
        "rows": rows,
        "row_count": row_count,
        "error": error,
        "latency_ms": round((time.perf_counter() - started) * 1000),
        "summary": summary,
        "answer": summary,
        "insights": [summary] if summary and not error else [],
        "key_numbers": key_numbers(columns, rows),
        "chart_suggestion": chart_suggestion(columns, rows, chart_src),
        "chart_type": chart_type,
        "chart": chart_src if isinstance(chart_src, dict) else None,
        "chart_data": rows,
        "agent_engine": get_settings().normalized_agent_engine,
        "status": "failed" if error else "success",
        "session_id": session_id,
        "turn_index": None,
        "answer_type": "empty_result" if not rows and not error else ("blocked" if error else "answer"),
        "needs_clarification": bool(missing.get("has_missing_info")),
        "clarification_suggestions": [],
        "assumptions": missing.get("items") or [],
        "retry_count": state.get("retry_count"),
        "model_used": llm_model,
        "intent": None,
        "used_tables": [],
        "warnings": [state.get("metadata_warning")] if state.get("metadata_warning") else [],
        "validation_notes": [],
        "confidence": None,
        "context_used": False,
        "resolved_question": None,
        "created_at": created_at,
        "agent_trace": trace,
    }


def persist_run(conn, user_id: UUID, session: dict, payload: dict) -> dict:
    turn_index = conn.execute(
        "SELECT count(*) AS n FROM app.query_runs WHERE session_id = %s",
        (session["id"],),
    ).fetchone()["n"]
    payload["turn_index"] = int(turn_index) + 1
    if not session.get("title"):
        conn.execute(
            "UPDATE app.chat_sessions SET title = %s WHERE id = %s",
            (title_from_question(payload["question"]), session["id"]),
        )
    conn.execute("UPDATE app.chat_sessions SET last_used_at = now() WHERE id = %s", (session["id"],))
    conn.execute(
        """
        INSERT INTO app.query_runs (
          id, user_id, session_id, question, generated_sql, guard_status, columns,
          rows, row_count, error, latency_ms, summary, insights, key_numbers,
          chart_suggestion, chart_type, chart, chart_data, agent_engine, status,
          turn_index, agent_trace, created_at
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s
        )
        """,
        (
            UUID(payload["run_id"]),
            user_id,
            session["id"],
            payload["question"],
            payload.get("generated_sql"),
            payload.get("guard_status"),
            json_param(payload.get("columns") or []),
            json_param(payload.get("rows") or []),
            payload.get("row_count") or 0,
            payload.get("error"),
            payload.get("latency_ms"),
            payload.get("summary"),
            json_param(payload.get("insights") or []),
            json_param(payload.get("key_numbers") or []),
            json_param(payload.get("chart_suggestion")) if payload.get("chart_suggestion") else None,
            payload.get("chart_type"),
            json_param(payload.get("chart")) if payload.get("chart") else None,
            json_param(payload.get("chart_data") or []),
            payload.get("agent_engine") or "legacy",
            payload.get("status") or "failed",
            payload["turn_index"],
            json_param(payload.get("agent_trace") or {}),
            payload.get("created_at"),
        ),
    )
    payload["session_id"] = str(session["id"])
    return payload


async def execute_question(
    *,
    question: str,
    user: dict,
    session_id: Optional[str],
    run_id: Optional[str] = None,
    chart_type: Optional[str] = "auto",
) -> dict:
    """Thin wrapper that records Prometheus /ask metrics around the run.

    Both POST /agent/ask and GET /agent/stream call this, so instrumenting here
    covers every agent invocation in one place.
    """
    metric_started = time.perf_counter()
    payload = await _execute_question(
        question=question,
        user=user,
        session_id=session_id,
        run_id=run_id,
        chart_type=chart_type,
    )
    observe_ask(payload, time.perf_counter() - metric_started)
    return payload


async def _execute_question(
    *,
    question: str,
    user: dict,
    session_id: Optional[str],
    run_id: Optional[str] = None,
    chart_type: Optional[str] = "auto",
) -> dict:
    started = time.perf_counter()
    rid = run_id or str(uuid4())
    settings = get_settings()
    with db_conn() as conn:
        session = resolve_session(conn, user["id"], session_id)
        sid = str(session["id"])
        # Same-session memory: served from Redis when warm, otherwise rebuilt
        # from app.query_runs so the model can resolve follow-up questions that
        # omit the metric, dimension, filters, or time range.
        app_context = load_session_context(conn, session["id"])
    llm_provider, llm_model = resolve_llm_preferences(user)
    context_used = bool(app_context.get("last_question") or app_context.get("last_sql"))

    # 1) Answer cache - identical question, same session + same conversation
    #    context, same model => return the previous answer without re-running
    #    the LLM or Trino. (No-op miss if Redis is unavailable.)
    ans_key = answer_cache_key(str(user["id"]), sid, question, chart_type, llm_model, app_context)
    cached_answer = cache.get_json(ans_key)
    if cached_answer:
        payload = dict(cached_answer)
        payload.update({
            "run_id": rid,
            "session_id": sid,
            "created_at": utc_now().isoformat(),
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "turn_index": None,
            "cached": True,
            "context_used": context_used,
        })
    else:
        # 2) Schema/metadata cache - seed schema_context so the graph skips its
        #    per-request Trino metadata lookup.
        schema_cached = load_schema_context()
        schema_context = (schema_cached or {}).get("schema_context")
        try:
            state = await asyncio.to_thread(
                run_graph_sync,
                question,
                rid,
                sid,
                str(user["id"]),
                chart_type,
                llm_provider,
                llm_model,
                app_context,
                schema_context,
            )
            payload = normalize_graph_result(state, rid, question, sid, started, llm_provider, llm_model)
            payload["context_used"] = context_used
            payload["cached"] = False
            if context_used:
                payload["resolved_question"] = app_context.get("last_question") or None
            if not schema_cached:
                store_schema_context(state)
            # 3) Store the answer for reuse (successful + not-too-large only).
            if payload.get("status") == "success" and int(payload.get("row_count") or 0) <= settings.cache_answer_max_rows:
                cache.set_json(ans_key, payload, settings.cache_answer_ttl)
        except Exception as exc:  # noqa: BLE001
            payload = build_failed_result(rid, question, sid, f"{exc.__class__.__name__}: {exc}", started)
            payload["model_used"] = llm_model
            payload["context_used"] = context_used
            payload["cached"] = False
            payload["agent_trace"] = {"llm_provider": llm_provider or os.getenv("AGENT_LLM_PROVIDER", "auto"), "model_used": llm_model}

    with db_conn() as conn:
        session = get_owned_session(conn, user["id"], session["id"]) or session
        try:
            result = persist_run(conn, user["id"], session, payload)
            # A new turn changes this session's follow-up context.
            invalidate_session_context(session["id"])
            return result
        except Exception as exc:  # noqa: BLE001
            payload["status"] = "failed"
            payload["error"] = payload.get("error") or f"Persistence failed: {exc.__class__.__name__}: {exc}"
            payload["summary"] = payload["error"]
            payload["answer"] = payload["error"]
            payload["session_id"] = str(session["id"])
            return payload


def format_run(row: dict, is_favorite: bool = False) -> dict:
    trace = row.get("agent_trace") or {}
    return {
        "run_id": str(row["id"]),
        "question": row["question"],
        "generated_sql": row.get("generated_sql"),
        "guard_status": row.get("guard_status"),
        "columns": row.get("columns") or [],
        "rows": row.get("rows") or [],
        "row_count": row.get("row_count") or 0,
        "error": row.get("error"),
        "latency_ms": row.get("latency_ms"),
        "summary": row.get("summary"),
        "answer": row.get("summary"),
        "insights": row.get("insights") or [],
        "key_numbers": row.get("key_numbers") or [],
        "chart_suggestion": row.get("chart_suggestion"),
        "chart_type": row.get("chart_type"),
        "chart": row.get("chart"),
        "chart_data": row.get("chart_data") or [],
        "agent_engine": row.get("agent_engine") or "legacy",
        "status": row.get("status") or "failed",
        "session_id": str(row["session_id"]) if row.get("session_id") else None,
        "turn_index": row.get("turn_index"),
        "answer_type": trace.get("answer_type") or ("blocked" if row.get("error") else "answer"),
        "needs_clarification": bool(trace.get("needs_clarification")),
        "clarification_suggestions": trace.get("clarification_suggestions") or [],
        "assumptions": (trace.get("missing_info") or {}).get("items") or [],
        "retry_count": trace.get("retry_count"),
        "model_used": trace.get("model_used"),
        "intent": trace.get("intent"),
        "used_tables": trace.get("used_tables") or [],
        "warnings": trace.get("warnings") or [],
        "validation_notes": trace.get("validation_notes") or [],
        "confidence": trace.get("confidence"),
        "context_used": bool(trace.get("context_used")),
        "resolved_question": trace.get("resolved_question"),
        "created_at": json_ready(row["created_at"]),
        "is_favorite": is_favorite,
    }


def enforce_ask_rate_limit(user: dict) -> None:
    """Throttle agent questions per user. Fail-open if Redis is down."""
    settings = get_settings()
    if not settings.rate_limit_enabled:
        return
    verdict = cache.rate_limit_check(
        "ask", str(user["id"]), settings.rl_ask_limit, settings.rl_ask_window_s
    )
    if not verdict["allowed"]:
        raise HTTPException(
            status_code=429,
            detail="Too many questions in a short time. Please wait a moment and try again.",
            headers={"Retry-After": str(verdict["retry_after"])},
        )


@router.post("/ask")
async def ask(body: AskRequest, user: dict = Depends(current_user)) -> dict:
    enforce_ask_rate_limit(user)
    return await execute_question(
        question=body.question,
        user=user,
        session_id=body.session_id,
        chart_type=body.chart_type,
    )


def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(json_ready(data), ensure_ascii=False)}\n\n"


@router.get("/stream")
async def stream_agent(
    question: str = Query(min_length=1),
    summarize: Optional[bool] = True,
    chart_type: Optional[str] = "auto",
    session_id: Optional[str] = None,
    user: dict = Depends(current_user),
) -> StreamingResponse:
    enforce_ask_rate_limit(user)

    async def events():
        run_id = str(uuid4())
        yield sse("step", {"step": "starting", "status": "running", "run_id": run_id})
        for step in UI_STEPS[:4]:
            yield sse("step", {"step": step, "status": "running", "run_id": run_id})
            await asyncio.sleep(0.05)
            yield sse("step", {"step": step, "status": "ok", "run_id": run_id})
        yield sse("step", {"step": "execute_sql", "status": "running", "run_id": run_id})
        task = asyncio.create_task(
            execute_question(
                question=question,
                user=user,
                session_id=session_id,
                run_id=run_id,
                chart_type=chart_type,
            )
        )
        RUN_TASKS[run_id] = task
        try:
            while not task.done():
                yield sse("heartbeat", {"run_id": run_id, "ts": utc_now().isoformat()})
                await asyncio.sleep(5)
            result = await task
            yield sse("step", {"step": "execute_sql", "status": "error" if result.get("error") else "ok", "run_id": run_id})
            yield sse("step", {"step": "summarize", "status": "running", "run_id": run_id})
            await asyncio.sleep(0.05)
            yield sse("step", {"step": "summarize", "status": "ok", "run_id": run_id})
            yield sse("result", result)
        except Exception as exc:  # noqa: BLE001
            result = build_failed_result(
                run_id,
                question,
                session_id,
                f"{exc.__class__.__name__}: {exc}",
                time.perf_counter(),
            )
            yield sse("step", {"step": "execute_sql", "status": "error", "run_id": run_id})
            yield sse("result", result)
        except asyncio.CancelledError:
            yield sse("step", {"step": "stopped", "status": "cancelled", "run_id": run_id})
        finally:
            RUN_TASKS.pop(run_id, None)

    return StreamingResponse(events(), media_type="text/event-stream")


@router.post("/stop")
def stop(body: StopRequest, user: dict = Depends(current_user)) -> dict:
    task = RUN_TASKS.get(body.run_id)
    if task:
        task.cancel()
    with db_conn() as conn:
        conn.execute(
            "UPDATE app.query_runs SET status = 'stopped' WHERE id = %s AND user_id = %s",
            (body.run_id, user["id"]),
        )
    return {"run_id": body.run_id, "status": "stopped"}


@router.get("/sessions")
def list_sessions(limit: int = Query(default=50, ge=1, le=100), user: dict = Depends(current_user)) -> list[dict]:
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM app.chat_sessions
            WHERE user_id = %s
            ORDER BY is_pinned DESC, pinned_at DESC NULLS LAST, last_used_at DESC, created_at DESC
            LIMIT %s
            """,
            (user["id"], limit),
        ).fetchall()
        return [session_summary(conn, row) for row in rows]


@router.post("/sessions", status_code=201)
def create_session(user: dict = Depends(current_user)) -> dict:
    with db_conn() as conn:
        return session_out(create_session_row(conn, user["id"]))


@router.get("/sessions/{session_id}/runs")
def session_runs(session_id: str, user: dict = Depends(current_user)) -> list[dict]:
    with db_conn() as conn:
        session = get_owned_session(conn, user["id"], session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Chat session not found.")
        rows = conn.execute(
            """
            SELECT r.*, (f.run_id IS NOT NULL) AS is_favorite
            FROM app.query_runs r
            LEFT JOIN app.favorite_runs f ON f.run_id = r.id AND f.user_id = r.user_id
            WHERE r.session_id = %s AND r.user_id = %s
            ORDER BY r.turn_index ASC NULLS LAST, r.created_at ASC
            """,
            (session["id"], user["id"]),
        ).fetchall()
        return [format_run(row, row.get("is_favorite")) for row in rows]


@router.patch("/sessions/{session_id}")
def update_session(session_id: str, body: SessionPatch, user: dict = Depends(current_user)) -> dict:
    with db_conn() as conn:
        session = get_owned_session(conn, user["id"], session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Chat session not found.")
        updates = []
        values = []
        fields_set = body.model_fields_set
        if "title" in fields_set:
            updates.append("title = %s")
            values.append(" ".join((body.title or "").split()) or None)
        if body.is_pinned is not None:
            updates.append("is_pinned = %s")
            values.append(body.is_pinned)
            updates.append("pinned_at = CASE WHEN %s THEN now() ELSE NULL END")
            values.append(body.is_pinned)
        if updates:
            values.extend([session["id"], user["id"]])
            session = conn.execute(
                f"UPDATE app.chat_sessions SET {', '.join(updates)} WHERE id = %s AND user_id = %s RETURNING *",
                values,
            ).fetchone()
        return session_out(session)


@router.delete("/sessions/{session_id}", status_code=204)
def delete_session(session_id: str, user: dict = Depends(current_user)) -> Response:
    with db_conn() as conn:
        deleted = conn.execute(
            "DELETE FROM app.chat_sessions WHERE id = %s AND user_id = %s",
            (session_id, user["id"]),
        ).rowcount
        if not deleted:
            raise HTTPException(status_code=404, detail="Chat session not found.")
    return Response(status_code=204)


@router.get("/sample-questions")
def sample_questions(_user: dict = Depends(current_user)) -> list[dict]:
    samples = [
        ("top-brand", "Top brand", "Top 10 brand theo doanh thu trong thang gan nhat"),
        ("daily-revenue", "Daily revenue", "Doanh thu theo ngay trong thang gan nhat"),
        ("category-cr", "Category conversion", "Danh muc nao co ty le chuyen doi cao nhat?"),
        ("product-views", "Top viewed products", "San pham nao duoc xem nhieu nhat?"),
    ]
    return [
        {"id": item[0], "label": item[1], "question": item[2], "sort_order": index}
        for index, item in enumerate(samples, start=1)
    ]


@router.post("/feedback")
def feedback(body: dict, user: dict = Depends(current_user)) -> dict:
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO app.agent_feedback (id, user_id, run_id, session_id, feedback_type, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                uuid4(),
                user["id"],
                body.get("run_id"),
                body.get("session_id"),
                body.get("feedback_type") or "feedback",
                json_param(body),
            ),
        )
    return {"status": "ok"}


@router.get("/runs/{run_id}/export-token")
def export_token(run_id: str, user: dict = Depends(current_user)) -> dict:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT id FROM app.query_runs WHERE id = %s AND user_id = %s",
            (run_id, user["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found.")
    settings = get_settings()
    payload = {
        "type": "export",
        "run_id": run_id,
        "sub": str(user["id"]),
        "exp": int((utc_now() + timedelta(minutes=5)).timestamp()),
    }
    return {"token": jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_alg)}


@router.get("/runs/{run_id}/export.csv")
def export_csv(run_id: str, token: str) -> Response:
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_alg])
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid export token.") from exc
    if payload.get("type") != "export" or payload.get("run_id") != run_id:
        raise HTTPException(status_code=401, detail="Wrong export token.")
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM app.query_runs WHERE id = %s AND user_id = %s",
            (run_id, payload["sub"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found.")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=row.get("columns") or [])
    writer.writeheader()
    for item in row.get("rows") or []:
        writer.writerow({key: item.get(key) for key in row.get("columns") or []})
    headers = {"Content-Disposition": f'attachment; filename="agent4da_{run_id}.csv"'}
    return Response(content=output.getvalue(), media_type="text/csv; charset=utf-8", headers=headers)
