"""Web-facing wrapper around the existing LangGraph agent.

Responsibilities:
- Bridge APP_* envs to the legacy TRINO_*/AGENT_* envs the CLI agent reads.
- Run `graph.invoke` in a worker thread so cancellation lands cleanly on the
  asyncio side.
- Apply lightweight post-LangGraph guard tweaks (auto-LIMIT on raw fact
  tables) that are too web-specific to live in code/agent.
- Manage chat sessions (create / resolve / list-context / delete) and persist
  one `app.query_runs` row per invocation, tied to its session.
- Compute a chart suggestion and a couple of heuristic insights from the result.

Phase 3 limitation noted in the plan: we do not yet capture the real Trino
query id, so `POST /agent/stop` calls `task.cancel()` and relies on the
session-level `query_max_execution_time=30s` to free the query upstream.
"""

from __future__ import annotations

import asyncio
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.errors import public_error_message
from api.settings import Settings, get_settings
from agent.contextual_learning import rank_contextual_suggestions
from chart.heuristics import suggest_chart
from db.models import AgentSuggestionEvent, ChatSession, QueryRun


log = structlog.get_logger("agent.service")

# How many prior turns to feed back to the agent as follow-up context.
MAX_CONTEXT_TURNS = 3
SESSION_TITLE_MAX_LEN = 80
CUSTOM_SESSION_TITLE_MAX_LEN = 200


# ---------------------------------------------------------------------------
# Env bridging - run once before importing the LangGraph agent
# ---------------------------------------------------------------------------

_GRAPH_IMPORTED = False


def _bridge_env(settings: Settings) -> None:
    """Map APP_* envs to the legacy variable names the CLI agent reads."""
    os.environ.setdefault("TRINO_HOST", settings.trino_host)
    os.environ.setdefault("TRINO_PORT", str(settings.trino_port))
    os.environ.setdefault("TRINO_USER", settings.trino_user)
    # GROQ_API_KEY is already in the container env via envs/groq.env.


def _get_graph():
    """Lazy import + compile of the LangGraph. Cached after first call."""
    global _GRAPH_IMPORTED
    settings = get_settings()
    _bridge_env(settings)

    # PYTHONPATH already includes /opt/project/code/agent (see Dockerfile),
    # so `from graph.sql_graph import graph` works.
    from graph.sql_graph import graph as compiled_graph  # type: ignore

    _GRAPH_IMPORTED = True
    return compiled_graph


def selected_agent_engine(settings: Optional[Settings] = None) -> str:
    """Engine name surfaced to clients.

    `legacy` wraps code/agent; `v2` runs app/backend/agent/engine_v2 with
    context-aware clarification and stricter Gold-only contracts.
    """
    value = (settings or get_settings()).agent_engine.strip().lower()
    return "v2" if value == "v2" else "legacy"


# ---------------------------------------------------------------------------
# Post-graph guards
# ---------------------------------------------------------------------------

_FACT_TABLE_PATTERN = re.compile(r"\bfact_[a-z_]+\b", flags=re.IGNORECASE)
_LIMIT_PATTERN = re.compile(r"\blimit\s+\d+\b", flags=re.IGNORECASE)


def apply_post_guards(sql: Optional[str]) -> Tuple[Optional[str], str]:
    """Returns (possibly-modified-sql, guard_status).

    guard_status: 'pass' | 'blocked' | 'auto_limited' | 'error'
    """
    if not sql:
        return sql, "error"
    stripped = sql.strip().rstrip(";").strip()
    if not stripped.lower().startswith("select"):
        # The existing guard_sql_node should have caught this. Defense in depth.
        return sql, "blocked"

    # Auto-append LIMIT 10000 when the SQL hits a raw fact table without LIMIT.
    if _FACT_TABLE_PATTERN.search(stripped) and not _LIMIT_PATTERN.search(stripped):
        return stripped + "\nLIMIT 10000", "auto_limited"

    return stripped, "pass"


# ---------------------------------------------------------------------------
# Run the agent in a worker thread
# ---------------------------------------------------------------------------


def _run_graph_sync(question: str, summarize: Optional[bool], run_id: str) -> Dict[str, Any]:
    graph = _get_graph()
    initial: Dict[str, Any] = {"user_question": question, "run_id": run_id}
    if summarize is not None:
        initial["summarize"] = summarize
    state = graph.invoke(initial)
    state["agent_engine"] = "legacy"
    return state


def _run_v2_sync(
    question: str,
    run_id: str,
    session_id: Optional[uuid.UUID],
    user_id: Optional[uuid.UUID],
) -> Dict[str, Any]:
    # Bridge APP_* -> TRINO_* so the shared Trino client connects correctly.
    _bridge_env(get_settings())
    from agent.engine_v2.runner import run_agent_state_v2

    return run_agent_state_v2(
        question=question,
        session_id=str(session_id) if session_id else "",
        user_id=str(user_id) if user_id else None,
        run_id=run_id,
    )


async def run_agent_state(
    question: str,
    summarize: Optional[bool],
    run_id: str,
    *,
    engine: str = "legacy",
    session_id: Optional[uuid.UUID] = None,
    user_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """Async wrapper. Cancellation propagates via asyncio.CancelledError.

    For the v2 engine the raw question is passed straight through (the v2 graph
    resolves follow-ups itself); the legacy engine receives whatever question the
    caller built (typically with a prepended context preamble).
    """
    if engine == "v2":
        return await asyncio.to_thread(_run_v2_sync, question, run_id, session_id, user_id)
    return await asyncio.to_thread(_run_graph_sync, question, summarize, run_id)


# ---------------------------------------------------------------------------
# Result formatting helpers
# ---------------------------------------------------------------------------


def _columns_from_rows(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows:
        return []
    # Preserve dict ordering of the first row.
    return list(rows[0].keys())


def _normalize_key_numbers(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        if "label" not in item or "value" not in item:
            continue
        out.append(
            {
                "label": str(item.get("label") or "")[:64],
                "value": item.get("value"),
                "delta": item.get("delta"),
            }
        )
    return out[:4]


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def compute_insights(
    columns: List[str],
    rows: List[Dict[str, Any]],
    row_count: int,
) -> List[str]:
    """Best-effort heuristic observations shown as the "main insight" line(s).

    Pure-Python, never raises, and never blocks the run. Vietnamese-friendly
    audience, but kept ASCII so the strings render anywhere.
    """
    insights: List[str] = []
    try:
        if not rows or not columns:
            return insights

        numeric_cols = [
            c for c in columns if any(_is_number(r.get(c)) for r in rows[:50])
        ]
        label_cols = [c for c in columns if c not in numeric_cols]

        insights.append(f"Tra ve {row_count:,} dong, {len(columns)} cot.")

        # Top contributor when there's a label + a numeric measure.
        if label_cols and numeric_cols:
            label_col = label_cols[0]
            value_col = numeric_cols[0]
            ranked = [
                r for r in rows if _is_number(r.get(value_col))
            ]
            if ranked:
                top = max(ranked, key=lambda r: float(r[value_col]))
                total = sum(float(r[value_col]) for r in ranked)
                top_val = float(top[value_col])
                share = (top_val / total * 100) if total else 0.0
                label = str(top.get(label_col, ""))[:48]
                if total and share >= 0:
                    insights.append(
                        f"Dan dau theo {value_col}: '{label}' "
                        f"({top_val:,.0f}, ~{share:.0f}% tong)."
                    )
        elif numeric_cols:
            value_col = numeric_cols[0]
            nums = [float(r[value_col]) for r in rows if _is_number(r.get(value_col))]
            if nums:
                insights.append(
                    f"{value_col}: tong {sum(nums):,.0f}, cao nhat {max(nums):,.0f}."
                )
    except Exception:  # noqa: BLE001 - insights are decorative, never fatal
        log.warning("agent.insights_failed")
        return insights[:3]
    return insights[:3]


# ---------------------------------------------------------------------------
# Chat sessions
# ---------------------------------------------------------------------------


def session_title_from_question(question: str) -> str:
    """A readable session title derived from its first question."""
    title = " ".join(question.strip().split())
    if not title:
        return "Cuoc tro chuyen moi"
    if len(title) <= SESSION_TITLE_MAX_LEN:
        return title
    return title[: SESSION_TITLE_MAX_LEN - 3].rstrip() + "..."


def normalize_session_title(title: Optional[str]) -> Optional[str]:
    """Normalize user-edited conversation titles; empty clears the custom title."""
    if title is None:
        return None
    normalized = " ".join(title.strip().split())
    if not normalized:
        return None
    return normalized[:CUSTOM_SESSION_TITLE_MAX_LEN]


def create_chat_session(session: Session, user_id: uuid.UUID) -> ChatSession:
    created = ChatSession(user_id=user_id)
    session.add(created)
    session.commit()
    session.refresh(created)
    return created


def owned_chat_session(
    session: Session,
    user_id: uuid.UUID,
    session_id: uuid.UUID,
) -> Optional[ChatSession]:
    chat = session.get(ChatSession, session_id)
    if chat is None or chat.user_id != user_id:
        return None
    return chat


def resolve_chat_session(
    session: Session,
    user_id: uuid.UUID,
    session_id: Optional[uuid.UUID] = None,
) -> ChatSession:
    """Return the requested session (404-on-foreign via PermissionError), or the
    user's current session when none is supplied so context is never lost."""
    if session_id is None:
        return get_or_create_current_session(session, user_id)
    chat = owned_chat_session(session, user_id, session_id)
    if chat is None:
        raise PermissionError("Chat session not found.")
    return chat


def delete_chat_session(
    session: Session,
    user_id: uuid.UUID,
    session_id: uuid.UUID,
) -> bool:
    """Delete an owned session. Its runs survive (FK ON DELETE SET NULL), so they
    remain visible in History. Returns False when the session is not owned."""
    chat = owned_chat_session(session, user_id, session_id)
    if chat is None:
        return False
    session.delete(chat)
    session.commit()
    return True


def update_chat_session(
    session: Session,
    user_id: uuid.UUID,
    session_id: uuid.UUID,
    *,
    title_provided: bool = False,
    title: Optional[str] = None,
    is_pinned: Optional[bool] = None,
) -> Optional[ChatSession]:
    chat = owned_chat_session(session, user_id, session_id)
    if chat is None:
        return None
    if title_provided:
        chat.title = normalize_session_title(title)
    if is_pinned is not None and bool(chat.is_pinned) != bool(is_pinned):
        chat.is_pinned = bool(is_pinned)
        chat.pinned_at = datetime.now(timezone.utc) if is_pinned else None
    session.commit()
    session.refresh(chat)
    return chat


def update_session_after_run(
    session: Session,
    session_id: Optional[uuid.UUID],
    question: str,
) -> None:
    if session_id is None:
        return
    chat = session.get(ChatSession, session_id)
    if chat is None:
        return
    if not chat.title:
        chat.title = session_title_from_question(question)
    chat.last_used_at = datetime.now(timezone.utc)


def get_or_create_current_session(session: Session, user_id: uuid.UUID) -> ChatSession:
    """Return the user's most-recently-used session, creating one if none."""
    existing = (
        session.execute(
            select(ChatSession)
            .where(ChatSession.user_id == user_id)
            .order_by(
                ChatSession.is_pinned.desc(),
                ChatSession.pinned_at.desc().nullslast(),
                ChatSession.last_used_at.desc(),
            )
            .limit(1)
        )
        .scalars()
        .first()
    )
    if existing is not None:
        return existing
    return create_chat_session(session, user_id)


def recent_session_questions(
    session: Session,
    session_id: Optional[uuid.UUID],
    user_id: Optional[uuid.UUID] = None,
    limit: int = MAX_CONTEXT_TURNS,
) -> List[str]:
    """Original (un-augmented) questions of the last successful turns, oldest
    first. Scoped to user_id when provided so a forged session_id cannot pull
    another user's questions into the prompt context."""
    if session_id is None:
        return []
    stmt = select(QueryRun.question).where(
        QueryRun.session_id == session_id, QueryRun.status == "success"
    )
    if user_id is not None:
        stmt = stmt.where(QueryRun.user_id == user_id)
    stmt = stmt.order_by(QueryRun.created_at.desc()).limit(limit)
    rows = session.execute(stmt).scalars().all()
    return list(reversed(list(rows)))


def build_effective_question(question: str, prior_questions: List[str]) -> Tuple[str, bool]:
    """Prepend a compact context preamble so follow-ups resolve naturally.

    The persisted `question` stays the user's original text; only the text handed
    to the LangGraph is augmented. Returns (effective_question, context_used).
    """
    if not prior_questions:
        return question, False
    lines = "\n".join(f"{i + 1}. {q}" for i, q in enumerate(prior_questions))
    preamble = (
        "Earlier questions in this conversation (use as context only if the "
        "new question is a follow-up; otherwise ignore):\n"
        f"{lines}\n\nCurrent question: "
    )
    return preamble + question, True


def next_turn_index(session: Session, session_id: Optional[uuid.UUID]) -> Optional[int]:
    if session_id is None:
        return None
    count = session.execute(
        select(func.count()).select_from(QueryRun).where(QueryRun.session_id == session_id)
    ).scalar_one()
    return int(count) + 1


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _persist_run(
    session: Session,
    *,
    run_id: uuid.UUID,
    user_id: uuid.UUID,
    question: str,
    state: Dict[str, Any],
    guard_status: str,
    final_sql: Optional[str],
    chart_suggestion: Optional[Dict[str, Any]],
    chart_type_hint: Optional[str],
    started_at: float,
    status: str,
    session_id: Optional[uuid.UUID] = None,
    turn_index: Optional[int] = None,
    insights: Optional[List[str]] = None,
) -> QueryRun:
    latency_ms = int((time.time() - started_at) * 1000)
    rows = state.get("query_result") or []
    columns = _columns_from_rows(rows)
    key_numbers = _normalize_key_numbers(state.get("key_numbers"))
    error_text = state.get("error")
    row_count = len(rows) if rows is not None else 0

    # v2 engine supplies an explicit chart_type (bar/line/pie/None) and extra
    # trace columns; the legacy engine leaves these unset.
    chart_type_value = (
        chart_type_hint
        or state.get("chart_type")
        or ("auto" if chart_suggestion else None)
    )

    run = QueryRun(
        id=run_id,
        user_id=user_id,
        session_id=session_id,
        turn_index=turn_index,
        question=question,
        generated_sql=final_sql,
        guard_status=guard_status,
        row_count=row_count,
        latency_ms=latency_ms,
        error=error_text,
        summary_text=state.get("summary"),
        insights=insights or [],
        key_numbers=key_numbers,
        chart_type=chart_type_value,
        chart_suggestion=chart_suggestion,
        columns=columns,
        result_json=rows[:10000] if rows else [],
        chart_payload=state.get("chart_payload"),
        chart_data=state.get("chart_data") or None,
        agent_trace=state.get("agent_trace"),
        retry_count=state.get("retry_count"),
        model_used=state.get("model_used"),
        agent_engine=str(state.get("agent_engine") or "legacy"),
        is_favorite=False,
        status=status,
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
                user_id=user_id,
                session_id=session_id,
                run_id=run_id,
                input_question=question,
                intent=trace.get("intent"),
                suggestions_generated=suggestions,
                result_status=status,
            )
        )
    update_session_after_run(session, session_id, question)
    session.commit()
    session.refresh(run)
    return run


# ---------------------------------------------------------------------------
# Public façade
# ---------------------------------------------------------------------------


async def execute_ask(
    session: Session,
    *,
    user_id: uuid.UUID,
    question: str,
    summarize: Optional[bool],
    chart_type_hint: Optional[str],
    session_id: Optional[uuid.UUID] = None,
) -> QueryRun:
    """Orchestrates: build context -> run agent -> post-guard -> persist."""
    if session_id is None:
        session_id = get_or_create_current_session(session, user_id).id

    engine = selected_agent_engine()
    run_id = uuid.uuid4()
    started_at = time.time()
    status = "success"
    final_sql: Optional[str] = None
    guard_status = "pass"
    state: Dict[str, Any] = {}
    chart_suggestion: Optional[Dict[str, Any]] = None
    insights: List[str] = []

    # The v2 graph resolves follow-ups itself, so it gets the raw question; the
    # legacy graph receives a context-prepended question built from prior turns.
    if engine == "v2":
        effective_question = question
    else:
        prior = recent_session_questions(session, session_id, user_id)
        effective_question, _ctx = build_effective_question(question, prior)
    turn_index = next_turn_index(session, session_id)

    try:
        state = await run_agent_state(
            effective_question,
            summarize,
            str(run_id),
            engine=engine,
            session_id=session_id,
            user_id=user_id,
        )

        if engine == "v2":
            # The v2 engine already guarded + limited the SQL and produced its
            # own status/guard_status/chart/insights.
            rank_contextual_suggestions(session, user_id, state)
            final_sql = state.get("generated_sql")
            status = state.get("status") or "success"
            guard_status = state.get("guard_status") or "pass"
            chart_suggestion = state.get("chart_suggestion")
            insights = list(state.get("insights") or [])
        else:
            # Apply web-only guard tweaks on top of the legacy LangGraph guard.
            final_sql, post_status = apply_post_guards(state.get("generated_sql"))
            upstream_error = state.get("error")
            if upstream_error:
                status = "blocked" if "Forbidden" in upstream_error or "SELECT" in upstream_error else "failed"
                guard_status = "blocked" if status == "blocked" else "error"
            else:
                guard_status = post_status

            rows = state.get("query_result") or []
            columns = _columns_from_rows(rows)
            chart_suggestion = suggest_chart(columns, rows)
            if not upstream_error:
                insights = compute_insights(columns, rows, len(rows))

    except asyncio.CancelledError:
        status = "stopped"
        guard_status = guard_status or "pass"
        state["agent_engine"] = state.get("agent_engine") or engine
        log.info("agent.cancelled", run_id=str(run_id))
        _persist_run(
            session,
            run_id=run_id,
            user_id=user_id,
            question=question,
            state=state,
            guard_status=guard_status,
            final_sql=final_sql or state.get("generated_sql"),
            chart_suggestion=chart_suggestion,
            chart_type_hint=chart_type_hint,
            started_at=started_at,
            status=status,
            session_id=session_id,
            turn_index=turn_index,
            insights=insights,
        )
        raise

    except Exception as exc:
        status = "failed"
        guard_status = "error"
        state["agent_engine"] = state.get("agent_engine") or engine
        state["error"] = state.get("error") or public_error_message(
            exc,
            "Agent run failed. Check the data catalog and model configuration, then retry.",
        )
        log.exception("agent.unexpected_error", run_id=str(run_id))

    return _persist_run(
        session,
        run_id=run_id,
        user_id=user_id,
        question=question,
        state=state,
        guard_status=guard_status,
        final_sql=final_sql or state.get("generated_sql"),
        chart_suggestion=chart_suggestion,
        chart_type_hint=chart_type_hint,
        started_at=started_at,
        status=status,
        session_id=session_id,
        turn_index=turn_index,
        insights=insights,
    )
