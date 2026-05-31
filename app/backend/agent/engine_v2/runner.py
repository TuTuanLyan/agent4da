"""Public entrypoint + response mapping for the v2 engine.

`run_agent_state_v2` runs the graph and returns a normalized `state` dict whose
keys overlap the legacy engine's (`generated_sql`, `query_result`, `summary`,
`error`, `agent_engine`) so the agent service can persist both engines through
the same path, plus v2-only keys (`status`, `guard_status`, chart payloads,
`agent_trace`, `retry_count`, `model_used`).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .config import MAX_RESULT_ROWS

# v2 status -> persisted query_runs.status
_STATUS_MAP = {"success": "success", "blocked": "blocked", "error": "failed"}
# v2 status -> guard_status
_GUARD_MAP = {"success": "pass", "blocked": "blocked", "error": "error"}
_CHART_SUGGESTION_TYPES = {"bar", "line", "pie", "scatter"}


def _map_status(v2_status: Optional[str]) -> str:
    return _STATUS_MAP.get((v2_status or "error").lower(), "failed")


def _map_guard_status(v2_status: Optional[str]) -> str:
    return _GUARD_MAP.get((v2_status or "error").lower(), "error")


def _chart_suggestion(chart: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Adapt the v2 chart dict to the existing ChartSuggestion schema, when it fits."""
    if not chart or not chart.get("recommended"):
        return None
    chart_type = chart.get("type")
    columns = chart.get("columns") or {}
    x = chart.get("x") or columns.get("x") or columns.get("label")
    y = chart.get("y") or columns.get("y") or columns.get("value")
    if chart_type not in _CHART_SUGGESTION_TYPES or not x or not y:
        return None
    series = chart.get("series")
    return {
        "chart_type": chart_type,
        "x": str(x),
        "y": str(y),
        "series": [str(series)] if series else None,
        "sort": None,
    }


def _agent_trace(response: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "intent",
        "dimension",
        "metric",
        "analysis_type",
        "time_range",
        "time_grain",
        "applied_time_filter",
        "limit",
        "filters",
        "comparison_entities",
        "sort_direction",
        "extracted_entities",
        "nlu_confidence",
        "table_candidates",
        "used_tables",
        "metadata_used",
        "warnings",
        "validation_notes",
        "confidence",
        "insight_source",
        "llm_insight_used",
        "retry_attempted",
        "retry_success",
        "retry_count",
        "original_sql",
        "correction_reason",
        "correction_history",
        "context_used",
        "resolved_question",
        "previous_question",
        "context_notes",
        "answer_type",
        "needs_clarification",
        "clarification_suggestions",
        "assumptions",
        "model_used",
    )
    return {key: response.get(key) for key in keys}


def response_to_state(response: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a v2 response dict into the state shape used for persistence."""
    rows: List[dict] = list(response.get("rows") or [])[:MAX_RESULT_ROWS]
    chart = response.get("chart") or {}
    v2_status = response.get("status")
    error_message = response.get("error_message")

    return {
        # Shared with the legacy engine's state contract.
        "agent_engine": "v2",
        "generated_sql": response.get("generated_sql") or None,
        "query_result": rows,
        "summary": response.get("answer") or None,
        "error": error_message,
        "key_numbers": [],
        # v2 extras consumed by the service when persisting.
        "status": _map_status(v2_status),
        "guard_status": _map_guard_status(v2_status),
        "insights": list(response.get("insights") or []),
        "chart_type": response.get("chart_type"),
        "chart_suggestion": _chart_suggestion(chart),
        "chart_payload": chart or None,
        "chart_data": list(response.get("chart_data") or []),
        "retry_count": int(response.get("retry_count") or 0),
        "model_used": response.get("model_used"),
        "answer_type": response.get("answer_type"),
        "needs_clarification": bool(response.get("needs_clarification")),
        "clarification_suggestions": list(response.get("clarification_suggestions") or []),
        "assumptions": list(response.get("assumptions") or []),
        "agent_trace": _agent_trace(response),
    }


def run_agent_state_v2(
    *,
    question: str,
    session_id: str,
    user_id: Optional[str] = None,
    run_id: Optional[str] = None,
    recent_context: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Run the v2 graph and return the normalized persistence state.

    `recent_context` is the durable, newest-first prior-turn history (from
    `query_runs`); when provided it is the source of follow-up context instead of
    the process-local store.
    """
    from .graph import run_agent_graph

    response = run_agent_graph(
        session_id=session_id,
        question=question,
        user_id=user_id,
        run_id=run_id,
        recent_context=recent_context,
    )
    return response_to_state(response)
