"""Contextual ranking for clarification suggestions.

This is intentionally retrieval/ranking only. It never changes SQL guardrails,
allowed tables, or generated SQL; it only reorders/adds clarification chips using
explicit feedback, suggestion-click events, and recent successful query runs.
"""

from __future__ import annotations

import uuid
from collections import Counter
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import AgentFeedback, AgentSuggestionEvent, QueryRun


_CONFIDENCE_SCORE = {"high": 30, "medium": 20, "low": 10}


def _trace(state: dict[str, Any]) -> dict[str, Any]:
    trace = state.get("agent_trace")
    return trace if isinstance(trace, dict) else {}


def _raw_suggestions(state: dict[str, Any]) -> list[dict[str, Any]]:
    trace = _trace(state)
    raw = state.get("clarification_suggestions") or trace.get("clarification_suggestions") or []
    return [dict(item) for item in raw if isinstance(item, dict) and item.get("question")]


def _suggestion_key(value: Any) -> str:
    if isinstance(value, dict):
        text = value.get("question") or value.get("label") or ""
    else:
        text = str(value or "")
    return " ".join(str(text).lower().split())


def _dedupe(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        key = _suggestion_key(item)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _history(session: Session, user_id: uuid.UUID) -> tuple[list[AgentFeedback], list[AgentSuggestionEvent], list[QueryRun]]:
    feedback_rows = (
        session.execute(
            select(AgentFeedback)
            .where(AgentFeedback.user_id == user_id, AgentFeedback.selected_suggestion.is_not(None))
            .order_by(AgentFeedback.created_at.desc())
            .limit(50)
        )
        .scalars()
        .all()
    )
    event_rows = (
        session.execute(
            select(AgentSuggestionEvent)
            .where(AgentSuggestionEvent.user_id == user_id)
            .order_by(AgentSuggestionEvent.created_at.desc())
            .limit(100)
        )
        .scalars()
        .all()
    )
    run_rows = (
        session.execute(
            select(QueryRun)
            .where(QueryRun.user_id == user_id)
            .order_by(QueryRun.created_at.desc())
            .limit(100)
        )
        .scalars()
        .all()
    )
    return feedback_rows, event_rows, run_rows


def _set_ranked_suggestions(state: dict[str, Any], suggestions: list[dict[str, Any]], note: str | None) -> None:
    state["clarification_suggestions"] = suggestions
    trace = _trace(state)
    if trace:
        trace["clarification_suggestions"] = suggestions
        state["agent_trace"] = trace
    if note:
        notes = list(state.get("validation_notes") or trace.get("validation_notes") or [])
        if note not in notes:
            notes.append(note)
        state["validation_notes"] = notes
        if trace:
            trace["validation_notes"] = notes


def rank_contextual_suggestions(session: Session, user_id: uuid.UUID, state: dict[str, Any]) -> None:
    """Reorder clarification suggestions using persisted learning signals."""
    suggestions = _raw_suggestions(state)
    if not suggestions:
        return

    trace = _trace(state)
    current_intent = state.get("intent") or trace.get("intent")
    feedback_rows, event_rows, run_rows = _history(session, user_id)

    selected_counts: Counter[str] = Counter()
    selected_intent_counts: Counter[tuple[str | None, str]] = Counter()
    history_candidates: list[dict[str, Any]] = []
    bad_query_counts: Counter[str] = Counter()
    good_query_counts: Counter[str] = Counter()
    good_intent_counts: Counter[str] = Counter()

    for row in feedback_rows:
        selected = row.selected_suggestion if isinstance(row.selected_suggestion, dict) else None
        key = _suggestion_key(selected)
        if not key:
            continue
        selected_counts[key] += 1
        selected_intent_counts[(selected.get("intent"), key)] += 1
        history_candidates.append(selected)

    for row in event_rows:
        selected = row.selected_suggestion if isinstance(row.selected_suggestion, dict) else None
        key = _suggestion_key(selected)
        if selected and key:
            selected_counts[key] += 1
            selected_intent_counts[(selected.get("intent") or row.intent, key)] += 1
            history_candidates.append(selected)
            if row.result_status in {"failed", "blocked", "stopped"}:
                bad_query_counts[key] += 1

    for run in run_rows:
        run_key = _suggestion_key(run.question)
        trace_data = run.agent_trace if isinstance(run.agent_trace, dict) else {}
        run_intent = trace_data.get("intent")
        if run.status == "success" and (run.row_count or 0) > 0:
            good_query_counts[run_key] += 1
            if run_intent:
                good_intent_counts[str(run_intent)] += 1
        elif run.status in {"failed", "blocked"} or trace_data.get("answer_type") == "empty_result" or (run.row_count or 0) == 0:
            bad_query_counts[run_key] += 1

    candidates = _dedupe(
        [
            *suggestions,
            *[
                item
                for item in history_candidates
                if not current_intent or item.get("intent") == current_intent or _suggestion_key(item) in {_suggestion_key(s) for s in suggestions}
            ],
        ]
    )

    original_order = {_suggestion_key(item): index for index, item in enumerate(candidates)}

    def score(item: dict[str, Any]) -> tuple[int, int]:
        key = _suggestion_key(item)
        intent = item.get("intent")
        value = _CONFIDENCE_SCORE.get(str(item.get("confidence") or "medium"), 20)
        if intent == current_intent:
            value += 12
        value += selected_counts[key] * 20
        value += selected_intent_counts[(intent, key)] * 12
        value += good_query_counts[key] * 16
        value += good_intent_counts[str(intent)] * 3 if intent else 0
        value -= bad_query_counts[key] * 18
        return value, -original_order.get(key, 0)

    ranked = sorted(candidates, key=score, reverse=True)[:5]
    if [_suggestion_key(item) for item in ranked] == [_suggestion_key(item) for item in suggestions[:5]]:
        return

    _set_ranked_suggestions(
        state,
        ranked,
        "Contextual suggestion ranking used prior feedback and successful query history.",
    )
