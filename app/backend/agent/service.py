"""Web-facing wrapper around the existing LangGraph agent.

Responsibilities:
- Bridge APP_* envs to the legacy TRINO_*/AGENT_* envs the CLI agent reads.
- Run `graph.invoke` in a worker thread so cancellation lands cleanly on the
  asyncio side.
- Apply lightweight post-LangGraph guard tweaks (auto-LIMIT on raw fact
  tables) that are too web-specific to live in code/agent.
- Persist one `app.query_runs` row per invocation.
- Compute a chart suggestion from the result.

Phase 3 limitation noted in the plan: we do not yet capture the real Trino
query id, so `POST /agent/stop` calls `task.cancel()` and relies on the
session-level `query_max_execution_time=30s` to free the query upstream.
A future patch will register the cursor.query_id for true cancellation.
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
from sqlalchemy.orm import Session

from api.errors import public_error_message
from api.settings import Settings, get_settings
from chart.heuristics import suggest_chart
from db.models import QueryRun


log = structlog.get_logger("agent.service")


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
    return graph.invoke(initial)


async def run_agent_state(
    question: str,
    summarize: Optional[bool],
    run_id: str,
) -> Dict[str, Any]:
    """Async wrapper. Cancellation propagates via asyncio.CancelledError."""
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
) -> QueryRun:
    latency_ms = int((time.time() - started_at) * 1000)
    rows = state.get("query_result") or []
    columns = _columns_from_rows(rows)
    key_numbers = _normalize_key_numbers(state.get("key_numbers"))
    error_text = state.get("error")

    run = QueryRun(
        id=run_id,
        user_id=user_id,
        question=question,
        generated_sql=final_sql,
        guard_status=guard_status,
        row_count=len(rows) if rows is not None else 0,
        latency_ms=latency_ms,
        error=error_text,
        summary_text=state.get("summary"),
        key_numbers=key_numbers,
        chart_type=chart_type_hint or ("auto" if chart_suggestion else None),
        chart_suggestion=chart_suggestion,
        columns=columns,
        result_json=rows[:10000] if rows else [],
        is_favorite=False,
        status=status,
    )
    session.add(run)
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
) -> QueryRun:
    """Orchestrates: run agent -> post-guard -> persist -> return QueryRun."""
    run_id = uuid.uuid4()
    started_at = time.time()
    status = "success"
    final_sql: Optional[str] = None
    guard_status = "pass"
    state: Dict[str, Any] = {}
    chart_suggestion: Optional[Dict[str, Any]] = None

    try:
        state = await run_agent_state(question, summarize, str(run_id))

        # Apply web-only guard tweaks on top of the LangGraph guard.
        final_sql, post_status = apply_post_guards(state.get("generated_sql"))
        # The LangGraph guard_sql_node may have already produced an error.
        upstream_error = state.get("error")
        if upstream_error:
            status = "blocked" if "Forbidden" in upstream_error or "SELECT" in upstream_error else "failed"
            guard_status = "blocked" if status == "blocked" else "error"
        else:
            guard_status = post_status

        rows = state.get("query_result") or []
        columns = _columns_from_rows(rows)
        chart_suggestion = suggest_chart(columns, rows)

    except asyncio.CancelledError:
        status = "stopped"
        guard_status = guard_status or "pass"
        log.info("agent.cancelled", run_id=str(run_id))
        run = _persist_run(
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
        )
        raise

    except Exception as exc:
        status = "failed"
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
    )
