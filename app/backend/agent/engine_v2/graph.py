"""The v2 LangGraph (ported from code/api/agent_graph.py).

Flow: initialize -> load_context -> safety -> resolve_followup -> intent_router
-> {metadata_answer | assistive_clarification | (metadata -> ambiguity_check
-> text_to_sql -> guard -> execute_sql -> validate_result -> chart -> insight)}
-> suggestion_generation -> save_context -> output.

Differences vs the standalone original:
- Catalog is `iceberg` (via engine_v2.config).
- No db_context: session existence + run persistence are owned by the backend
  agent service. save_context only records the in-process conversation turn.
- No langgraph Postgres saver: a compact snapshot is written to
  `app.agent_checkpoint_snapshots` after each run.
"""

from __future__ import annotations

import re
import time
from typing import Any, TypedDict

try:  # NotRequired is in typing on 3.11+; fall back for older interpreters.
    from typing import NotRequired
except ImportError:  # pragma: no cover - backend runtime is 3.12
    from typing_extensions import NotRequired

from langgraph.graph import END, StateGraph

from . import llm
from .charts import recommend_chart
from .checkpoint import new_checkpoint_id, save_checkpoint_snapshot
from .config import GOLD_CATALOG, GOLD_PREFIX, GOLD_SCHEMA, MAX_SQL_RETRY_ATTEMPTS
from .context import (
    classify_followup,
    get_recent_context,
    llm_extract_patch,
    llm_rewrite_followup,
    save_turn,
)
from .spec import merge_spec, render_spec_question
from .corrector import correct_sql
from .insights import generate_insight, generate_llm_insight
from .metadata import get_gold_tables, get_table_columns, select_metadata
from .nlu import route_intent
from .sql_generator import generate_sql, resolve_applied_time_filter
from .guard import validate_sql
from .suggestions import answer_type_for, build_assumptions, build_suggestions, needs_clarification
from .trino_exec import execute_query
from .validator import validate_result


class AgentState(TypedDict):
    session_id: str
    user_id: NotRequired[str | None]
    run_id: NotRequired[str | None]
    question: str
    effective_question: NotRequired[str]
    resolved_question: NotRequired[str | None]
    context_used: NotRequired[bool]
    context_sql_followup: NotRequired[bool]
    spec_ready: NotRequired[bool]
    previous_turn_id: NotRequired[int | None]
    previous_question: NotRequired[str | None]
    previous_sql: NotRequired[str | None]
    context_notes: NotRequired[list[str]]
    recent_context: NotRequired[list[dict[str, Any]]]
    intent: NotRequired[str | None]
    dimension: NotRequired[str | None]
    metric: NotRequired[str | None]
    analysis_type: NotRequired[str | None]
    time_range: NotRequired[dict[str, Any] | str | None]
    time_grain: NotRequired[str | None]
    applied_time_filter: NotRequired[dict[str, Any] | None]
    filters: NotRequired[list[dict[str, Any]]]
    comparison_entities: NotRequired[list[str]]
    sort_direction: NotRequired[str | None]
    extracted_entities: NotRequired[dict[str, Any]]
    nlu_confidence: NotRequired[str]
    table_candidates: NotRequired[list[str]]
    needs_metadata: NotRequired[bool]
    limit: NotRequired[int]
    intent_result: NotRequired[dict[str, Any]]
    metadata_used: NotRequired[dict[str, Any]]
    metadata_context: NotRequired[dict[str, Any]]
    generated_sql: NotRequired[str]
    original_sql: NotRequired[str | None]
    failed_sql: NotRequired[str]
    used_tables: NotRequired[list[str]]
    rows: NotRequired[list[dict[str, Any]]]
    row_count: NotRequired[int]
    warnings: NotRequired[list[str]]
    validation_notes: NotRequired[list[str]]
    confidence: NotRequired[str]
    insights: NotRequired[list[str]]
    insight_source: NotRequired[str]
    llm_insight_used: NotRequired[bool]
    chart: NotRequired[dict[str, Any]]
    chart_type: NotRequired[str | None]
    chart_data: NotRequired[list[dict[str, Any]]]
    answer: NotRequired[str]
    answer_type: NotRequired[str]
    conversational_answer: NotRequired[bool]
    conversational_suggestions: NotRequired[list[dict[str, Any]]]
    needs_clarification: NotRequired[bool]
    clarification_suggestions: NotRequired[list[dict[str, Any]]]
    assumptions: NotRequired[list[str]]
    status: NotRequired[str]
    error_message: NotRequired[str | None]
    retry_attempted: NotRequired[bool]
    retry_success: NotRequired[bool]
    correction_reason: NotRequired[str | None]
    retry_count: NotRequired[int]
    correction_history: NotRequired[list[dict[str, Any]]]
    start_time: NotRequired[float]
    execution_time_ms: NotRequired[int]
    model_used: NotRequired[str | None]
    guard_ok: NotRequired[bool]
    execute_ok: NotRequired[bool]
    can_retry: NotRequired[bool]
    terminal_action: NotRequired[str | None]
    response: NotRequired[dict[str, Any]]


def _empty_chart(reason: str) -> dict[str, Any]:
    return {
        "recommended": False,
        "type": None,
        "title": None,
        "x": None,
        "y": None,
        "series": None,
        "reason": reason,
        "limit": None,
        "chart_data_mode": None,
        "data": [],
        "columns": {},
        "alternative_types": [],
    }


def _chart_type(chart: dict[str, Any]) -> str | None:
    return chart.get("type") if chart.get("recommended") else None


def _chart_data(chart: dict[str, Any]) -> list[dict[str, Any]]:
    data = chart.get("data") if chart.get("recommended") else []
    return data if isinstance(data, list) else []


def _elapsed_ms(state: AgentState) -> int:
    return int((time.perf_counter() - float(state.get("start_time", time.perf_counter()))) * 1000)


def _model_used() -> str | None:
    return llm.default_model() if llm.llm_available() else None


def _used_tables(sql: str) -> list[str]:
    if not sql:
        return []

    refs = []
    show_match = re.fullmatch(
        r"\s*SHOW\s+TABLES\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*",
        sql,
        flags=re.IGNORECASE,
    )
    if show_match:
        refs.append(show_match.group(1).lower())

    describe_match = re.fullmatch(
        r"\s*(?:DESCRIBE|DESC)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})\s*",
        sql,
        flags=re.IGNORECASE,
    )
    if describe_match:
        refs.append(describe_match.group(1).lower())

    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})\b",
        sql,
        flags=re.IGNORECASE,
    ):
        refs.append(match.group(1).lower())

    unique_refs = []
    for ref in refs:
        if ref not in unique_refs:
            unique_refs.append(ref)
    return unique_refs


SEMANTIC_TABLE_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_table_catalog"
SEMANTIC_COLUMN_CATALOG = f"{GOLD_CATALOG}.metadata.semantic_column_catalog"


def _metadata_tables_sql() -> str:
    table_expr = (
        f"CASE WHEN starts_with(table_name, '{GOLD_SCHEMA}.') "
        f"THEN substr(table_name, {len(GOLD_SCHEMA) + 2}) "
        "ELSE table_name END"
    )
    return (
        f"SELECT {table_expr} AS table_name "
        f"FROM {SEMANTIC_TABLE_CATALOG} "
        "WHERE is_agent_visible = true "
        "ORDER BY table_name"
    )


def _metadata_columns_sql(table_name: str) -> str:
    qualified_table_name = f"{GOLD_SCHEMA}.{table_name}"
    return (
        "SELECT DISTINCT column_name, data_type "
        f"FROM {SEMANTIC_COLUMN_CATALOG} "
        "WHERE is_agent_visible = true "
        f"AND table_name IN ('{table_name}', '{qualified_table_name}') "
        "ORDER BY column_name"
    )


def _is_dangerous_request(question: str) -> bool:
    text = question.strip().lower()
    sql_keywords = (
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "truncate",
        "create",
        "merge",
        "call",
        "grant",
        "revoke",
    )
    if any(re.search(rf"\b{keyword}\b", text) for keyword in sql_keywords):
        return True
    return ("xóa" in text or "xoá" in text or "xoa" in text) and ("bảng" in text or "bang" in text)


def _is_non_retryable_execution_error(error_message: str) -> bool:
    normalized = error_message.lower()
    return any(
        token in normalized
        for token in (
            "catalog_not_found",
            f"catalog '{GOLD_CATALOG}' not found",
            f"catalog '{GOLD_CATALOG}' does not exist",
            "connection refused",
            "failed to establish",
            "name or service not known",
            "timed out",
            "timeout",
            "permission denied",
            "access denied",
            "authentication",
            "unauthorized",
        )
    )


def _is_non_retryable_guard_error(error_message: str, failed_sql: str) -> bool:
    normalized_error = error_message.lower()
    normalized_sql = failed_sql.lower()
    return "disallowed sql keyword" in normalized_error or any(
        re.search(rf"\b{keyword}\b", normalized_sql, flags=re.IGNORECASE)
        for keyword in (
            "insert",
            "update",
            "delete",
            "drop",
            "alter",
            "truncate",
            "create",
            "merge",
            "call",
            "grant",
            "revoke",
        )
    )


def _intent_dict(state: AgentState) -> dict[str, Any]:
    return {
        "intent": state.get("intent"),
        "dimension": state.get("dimension"),
        "metric": state.get("metric"),
        "analysis_type": state.get("analysis_type"),
        "time_range": state.get("time_range"),
        "time_grain": state.get("time_grain"),
        "applied_time_filter": state.get("applied_time_filter"),
        "filters": state.get("filters", []),
        "comparison_entities": state.get("comparison_entities", []),
        "sort_direction": state.get("sort_direction"),
        "extracted_entities": state.get("extracted_entities", {}),
        "nlu_confidence": state.get("nlu_confidence", "low"),
        "table_candidates": state.get("table_candidates", []),
        "limit": state.get("limit", 10),
        "needs_metadata": state.get("needs_metadata", False),
        "table_name": state.get("intent_result", {}).get("table_name"),
    }


def _build_response(state: AgentState) -> dict[str, Any]:
    chart = state.get("chart") or _empty_chart("Không có chart recommendation.")
    chart_type = state.get("chart_type", _chart_type(chart))
    chart_data = state.get("chart_data", _chart_data(chart))
    return {
        "session_id": state["session_id"],
        "question": state["question"],
        "answer": state.get("answer", ""),
        "generated_sql": state.get("generated_sql", ""),
        "used_tables": state.get("used_tables", []),
        "row_count": state.get("row_count", 0),
        "rows": state.get("rows", []),
        "warnings": state.get("warnings", []),
        "insights": state.get("insights", []),
        "insight_source": state.get("insight_source", "rule_based"),
        "llm_insight_used": state.get("llm_insight_used", False),
        "confidence": state.get("confidence", "low"),
        "validation_notes": state.get("validation_notes", []),
        "chart": chart,
        "chart_type": chart_type,
        "chart_data": chart_data,
        "answer_type": state.get("answer_type", "answer"),
        "needs_clarification": state.get("needs_clarification", False),
        "clarification_suggestions": state.get("clarification_suggestions", []),
        "assumptions": state.get("assumptions", []),
        "status": state.get("status", "error"),
        "error_message": state.get("error_message"),
        "intent": state.get("intent"),
        "dimension": state.get("dimension"),
        "metric": state.get("metric"),
        "analysis_type": state.get("analysis_type"),
        "time_range": state.get("time_range"),
        "time_grain": state.get("time_grain"),
        "applied_time_filter": state.get("applied_time_filter"),
        "filters": state.get("filters", []),
        "comparison_entities": state.get("comparison_entities", []),
        "sort_direction": state.get("sort_direction"),
        "extracted_entities": state.get("extracted_entities", {}),
        "nlu_confidence": state.get("nlu_confidence", "low"),
        "table_candidates": state.get("table_candidates", []),
        "limit": state.get("limit", 10),
        "needs_metadata": state.get("needs_metadata", False),
        "metadata_used": state.get("metadata_used") or {"tables": [], "columns": {}},
        "retry_attempted": state.get("retry_attempted", False),
        "retry_success": state.get("retry_success", False),
        "retry_count": state.get("retry_count", 0),
        "original_sql": state.get("original_sql"),
        "correction_reason": state.get("correction_reason"),
        "correction_history": state.get("correction_history", []),
        "context_used": state.get("context_used", False),
        "resolved_question": state.get("resolved_question"),
        "previous_turn_id": state.get("previous_turn_id"),
        "previous_question": state.get("previous_question"),
        "context_notes": state.get("context_notes", []),
        "model_used": state.get("model_used"),
        "agent_engine": "v2",
    }


def initialize_node(state: AgentState) -> AgentState:
    return {
        **state,
        "effective_question": state["question"],
        # Preserve any caller-provided context (durable history from query_runs);
        # load_context_node falls back to the process store only when it is empty.
        "recent_context": state.get("recent_context") or [],
        "resolved_question": None,
        "context_used": False,
        "context_sql_followup": False,
        "spec_ready": False,
        "previous_turn_id": None,
        "previous_question": None,
        "previous_sql": None,
        "context_notes": [],
        "intent": None,
        "dimension": None,
        "metric": None,
        "analysis_type": None,
        "time_range": None,
        "time_grain": None,
        "applied_time_filter": None,
        "filters": [],
        "comparison_entities": [],
        "sort_direction": None,
        "extracted_entities": {},
        "nlu_confidence": "low",
        "table_candidates": [],
        "needs_metadata": False,
        "limit": 10,
        "intent_result": {},
        "metadata_used": {"tables": [], "columns": {}},
        "metadata_context": {"tables": [], "columns": {}},
        "generated_sql": "",
        "original_sql": None,
        "failed_sql": "",
        "used_tables": [],
        "rows": [],
        "row_count": 0,
        "warnings": [],
        "validation_notes": [],
        "confidence": "low",
        "insights": [],
        "insight_source": "rule_based",
        "llm_insight_used": False,
        "chart": _empty_chart("Không có chart recommendation."),
        "chart_type": None,
        "chart_data": [],
        "answer": "",
        "answer_type": "answer",
        "conversational_answer": False,
        "conversational_suggestions": [],
        "needs_clarification": False,
        "clarification_suggestions": [],
        "assumptions": [],
        "status": "error",
        "error_message": None,
        "retry_attempted": False,
        "retry_success": False,
        "correction_reason": None,
        "retry_count": 0,
        "correction_history": [],
        "model_used": _model_used(),
        "terminal_action": None,
        "guard_ok": False,
        "execute_ok": False,
        "can_retry": False,
        "response": {},
    }


def load_context_node(state: AgentState) -> AgentState:
    # Durable history passed by the service (rehydrated from query_runs) wins so
    # follow-ups survive restarts/multi-worker; otherwise use the process store.
    if state.get("recent_context"):
        return state
    return {**state, "recent_context": get_recent_context(state["session_id"], limit=5)}


def safety_node(state: AgentState) -> AgentState:
    if not _is_dangerous_request(state["question"]):
        return state

    intent_result = route_intent(state["question"])
    error_message = "Yêu cầu bị chặn vì chứa thao tác DDL/DML hoặc thay đổi dữ liệu không an toàn."
    return {
        **state,
        "intent_result": intent_result,
        "intent": intent_result["intent"],
        "dimension": intent_result["dimension"],
        "metric": intent_result["metric"],
        "analysis_type": intent_result.get("analysis_type"),
        "time_range": intent_result.get("time_range"),
        "time_grain": intent_result.get("time_grain"),
        "filters": intent_result.get("filters", []),
        "comparison_entities": intent_result.get("comparison_entities", []),
        "sort_direction": intent_result.get("sort_direction"),
        "extracted_entities": intent_result.get("extracted_entities", {}),
        "nlu_confidence": intent_result.get("nlu_confidence", "low"),
        "table_candidates": intent_result["table_candidates"],
        "needs_metadata": intent_result["needs_metadata"],
        "limit": intent_result["limit"],
        "answer": error_message,
        "answer_type": "blocked",
        "needs_clarification": True,
        "warnings": [error_message],
        "validation_notes": ["Dangerous user request was blocked before SQL generation."],
        "chart": _empty_chart("Yêu cầu bị chặn nên không có chart recommendation."),
        "status": "blocked",
        "error_message": error_message,
        "terminal_action": "blocked",
    }


def _state_from_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Mirror a merged Active Query Spec onto the individual state fields the
    downstream nodes read (so we can skip a fresh NLU parse for refinements)."""
    return {
        "intent_result": dict(spec),
        "intent": spec.get("intent"),
        "dimension": spec.get("dimension"),
        "metric": spec.get("metric"),
        "analysis_type": spec.get("analysis_type"),
        "time_range": spec.get("time_range"),
        "time_grain": spec.get("time_grain"),
        "filters": spec.get("filters", []),
        "comparison_entities": spec.get("comparison_entities", []),
        "sort_direction": spec.get("sort_direction"),
        "extracted_entities": spec.get("extracted_entities", {}),
        "nlu_confidence": spec.get("nlu_confidence", "high"),
        "table_candidates": spec.get("table_candidates", []),
        "needs_metadata": spec.get("needs_metadata", True),
        "limit": spec.get("limit", 10),
    }


def _apply_reuse_chart(updates: AgentState, followup: dict[str, Any]) -> AgentState:
    chart = followup.get("chart") or _empty_chart("Không có chart recommendation ở lượt trước.")
    updates.update(
        {
            "generated_sql": followup.get("generated_sql") or "",
            "used_tables": followup.get("used_tables") or _used_tables(followup.get("generated_sql") or ""),
            "row_count": int(followup.get("row_count") or 0),
            "rows": [],
            "answer": followup.get("answer") or "Dưới đây là gợi ý biểu đồ cho kết quả trước.",
            "chart": chart,
            "chart_type": _chart_type(chart),
            "chart_data": _chart_data(chart),
            "confidence": "high",
            "validation_notes": ["Reused previous chart recommendation without executing a new query."],
            "insight_source": "rule_based",
            "llm_insight_used": False,
            "intent": followup.get("intent") or "chart_previous",
            "table_candidates": followup.get("table_candidates") or [],
            "status": "success",
            "error_message": None,
            "terminal_action": "direct_response",
        }
    )
    return updates


def _apply_explain_sql(updates: AgentState, followup: dict[str, Any]) -> AgentState:
    updates.update(
        {
            "generated_sql": followup.get("generated_sql") or "",
            "used_tables": followup.get("used_tables") or _used_tables(followup.get("generated_sql") or ""),
            "row_count": 0,
            "rows": [],
            "answer": followup.get("answer") or "Không có SQL trước đó để giải thích trong session này.",
            "chart": _empty_chart("Câu hỏi giải thích SQL nên hiển thị dạng text."),
            "chart_type": None,
            "chart_data": [],
            "confidence": "high",
            "validation_notes": ["Explained previous SQL without executing a new query."],
            "insight_source": "rule_based",
            "llm_insight_used": False,
            "intent": "explain_sql",
            "table_candidates": followup.get("table_candidates") or [],
            "status": "success",
            "error_message": None,
            "terminal_action": "direct_response",
        }
    )
    return updates


def resolve_followup_node(state: AgentState) -> AgentState:
    """Classify the turn into an operation on the session's Active Query Spec.

    refine/entity_ref produce a merged spec the SQL pipeline runs directly
    (spec_ready, skipping a fresh NLU parse); presentation reuses the prior
    result; meta routes to the conversational assistant; reset/new_query run
    normally; ambiguous falls back to the LLM rewriter.
    """
    recent_context = state.get("recent_context", [])
    cls = classify_followup(state["question"], recent_context)
    op = cls.get("op")
    updates: AgentState = {
        **state,
        "context_used": bool(cls.get("context_used")),
        "context_notes": list(cls.get("context_notes") or []),
        "previous_turn_id": cls.get("previous_turn_id"),
        "previous_question": cls.get("previous_question"),
    }

    if op == "presentation":
        followup = cls["followup"]
        updates["context_used"] = True
        updates["context_notes"] = list(followup.get("context_notes") or [])
        updates["previous_turn_id"] = followup.get("previous_turn_id")
        updates["previous_question"] = followup.get("previous_question")
        if followup.get("action") == "reuse_chart":
            return _apply_reuse_chart(updates, followup)
        return _apply_explain_sql(updates, followup)

    if op == "meta":
        updates["terminal_action"] = "conversational"
        return updates

    if op == "reset":
        return {**updates, "context_used": False, "context_sql_followup": False, "effective_question": state["question"]}

    if op == "clarification_answer":
        resolved = cls.get("resolved_question") or state["question"]
        updates["effective_question"] = resolved
        updates["resolved_question"] = resolved
        updates["context_used"] = True
        updates["context_notes"] = ["Resolved a clarification answer from the prior suggestions."]
        return updates

    if op in ("refine", "entity_ref"):
        spec = cls["merged_spec"]
        updates.update(_state_from_spec(spec))
        updates["spec_ready"] = True
        updates["context_used"] = True
        updates["effective_question"] = render_spec_question(spec)
        updates["resolved_question"] = render_spec_question(spec)
        updates["previous_sql"] = cls.get("previous_sql") or ""
        return updates

    if op == "ambiguous":
        # First try a typed LLM patch on the prior spec (precise, validated);
        # fall back to the string rewriter only if that yields nothing.
        prev_spec = cls.get("prev_spec")
        patch = llm_extract_patch(state["question"], recent_context) if prev_spec else None
        if patch:
            merged = merge_spec(prev_spec, patch)
            updates.update(_state_from_spec(merged))
            updates["spec_ready"] = True
            updates["context_used"] = True
            updates["effective_question"] = render_spec_question(merged)
            updates["resolved_question"] = render_spec_question(merged)
            updates["context_notes"] = ["Resolved an ambiguous follow-up via an LLM patch on the prior spec."]
            updates["previous_question"] = cls.get("previous_question")
            updates["previous_turn_id"] = cls.get("previous_turn_id")
            return updates

        rewrite = llm_rewrite_followup(state["question"], recent_context)
        if rewrite.get("is_followup"):
            standalone = rewrite["standalone_question"]
            notes = list(updates.get("context_notes") or [])
            notes.append(rewrite.get("reason") or "Resolved a contextual follow-up via LLM rewrite.")
            updates.update(
                {
                    "effective_question": standalone,
                    "resolved_question": standalone,
                    "context_used": True,
                    "context_sql_followup": True,
                    "previous_sql": rewrite.get("previous_sql") or "",
                    "previous_turn_id": rewrite.get("previous_turn_id"),
                    "previous_question": rewrite.get("previous_question"),
                    "context_notes": notes,
                }
            )
        return updates

    # new_query
    updates["context_used"] = False
    return updates


def intent_router_node(state: AgentState) -> AgentState:
    intent_result = route_intent(state.get("effective_question") or state["question"])
    return {
        **state,
        "intent_result": intent_result,
        "intent": intent_result["intent"],
        "dimension": intent_result["dimension"],
        "metric": intent_result["metric"],
        "analysis_type": intent_result.get("analysis_type"),
        "time_range": intent_result.get("time_range"),
        "time_grain": intent_result.get("time_grain"),
        "filters": intent_result.get("filters", []),
        "comparison_entities": intent_result.get("comparison_entities", []),
        "sort_direction": intent_result.get("sort_direction"),
        "extracted_entities": intent_result.get("extracted_entities", {}),
        "nlu_confidence": intent_result.get("nlu_confidence", "low"),
        "table_candidates": intent_result["table_candidates"],
        "needs_metadata": intent_result["needs_metadata"],
        "limit": intent_result["limit"],
    }


def metadata_node(state: AgentState) -> AgentState:
    metadata_context = select_metadata(state.get("table_candidates", []))
    return {**state, "metadata_context": metadata_context, "metadata_used": metadata_context}


def metadata_answer_node(state: AgentState) -> AgentState:
    intent = state.get("intent")
    intent_result = state.get("intent_result", {})
    effective_question = state.get("effective_question") or state["question"]

    if intent == "metadata_tables":
        generated_sql = _metadata_tables_sql()
        tables = get_gold_tables()
        rows = [{"Table": table} for table in tables]
        metadata_used = {"tables": tables, "columns": {}}
    else:
        table_name = intent_result.get("table_name")
        if not table_name:
            raise ValueError("Cannot determine Gold table for metadata_columns question")
        generated_sql = _metadata_columns_sql(table_name)
        columns = get_table_columns(table_name)
        rows = [{"column_name": column["name"], "data_type": column["type"]} for column in columns]
        metadata_used = {"tables": [table_name], "columns": {table_name: columns}}

    used_tables = _used_tables(generated_sql)
    validation = validate_result(
        question=effective_question,
        intent=state.get("intent") or "",
        generated_sql=generated_sql,
        rows=rows,
        row_count=len(rows),
        table_candidates=state.get("table_candidates", []),
        used_tables=used_tables,
    )
    insight = generate_insight(
        question=effective_question,
        intent=state.get("intent") or "",
        rows=rows,
        warnings=validation["warnings"],
        generated_sql=generated_sql,
        table_candidates=state.get("table_candidates", []),
        used_tables=used_tables,
    )
    chart = recommend_chart(
        question=effective_question,
        intent=state.get("intent") or "",
        rows=rows,
        row_count=len(rows),
        generated_sql=generated_sql,
        table_candidates=state.get("table_candidates", []),
        used_tables=used_tables,
        warnings=validation["warnings"],
    )
    return {
        **state,
        "generated_sql": generated_sql,
        "used_tables": used_tables,
        "rows": rows,
        "row_count": len(rows),
        "warnings": validation["warnings"],
        "confidence": validation["confidence"],
        "validation_notes": validation["validation_notes"],
        "answer": insight["answer"],
        "insights": insight["insights"],
        "insight_source": insight.get("insight_source", "rule_based"),
        "llm_insight_used": insight.get("llm_insight_used", False),
        "chart": chart,
        "chart_type": _chart_type(chart),
        "chart_data": _chart_data(chart),
        "metadata_used": metadata_used,
        "status": "success",
        "error_message": None,
    }


def _fallback_clarification_answer(question: str) -> str:
    if any(
        token in question.lower()
        for token in ("thời tiết", "thoi tiet", "weather", "crypto", "bitcoin", "tin tức", "tin tuc", "news")
    ):
        return (
            "Mình chưa có nguồn dữ liệu đó trong hệ thống hiện tại. "
            "Mình có thể hỗ trợ phân tích hành vi e-commerce từ Gold data; hãy chọn một hướng bên dưới."
        )
    return (
        "Mình chưa đủ thông tin để tạo một truy vấn phân tích an toàn. "
        "Bạn có thể chọn metric, thời gian hoặc chiều phân tích phù hợp bên dưới."
    )


def assistive_clarification_node(state: AgentState) -> AgentState:
    """Answer free-form prompts the deterministic SQL pipeline cannot classify.

    Uses the Groq conversational assistant grounded in the semantic metadata and
    the current chat context. Falls back to the deterministic clarification
    message whenever the LLM is unavailable or fails, so behaviour degrades
    gracefully and stays guardrail-neutral (no SQL is generated here).
    """
    question = (state.get("effective_question") or state["question"]).strip()
    recent_context = state.get("recent_context") or []

    conversation: dict[str, Any] = {"llm_used": False}
    try:
        from .conversation import answer_conversational

        # overview is built lazily inside (only when a Groq key is configured),
        # so there is no Trino round-trip when the LLM is unavailable.
        conversation = answer_conversational(question, recent_context)
    except Exception:  # noqa: BLE001 - conversational answer is best-effort
        conversation = {"llm_used": False}

    answered = bool(conversation.get("llm_used")) and bool(conversation.get("answer"))
    if answered:
        answer = conversation["answer"]
        validation_note = (
            "Answered a free-form question with the Groq conversational assistant "
            "grounded in Gold semantic metadata and chat context."
        )
        warnings: list[str] = []
    else:
        answer = _fallback_clarification_answer(question)
        validation_note = (
            "The request was routed to assistive clarification instead of returning "
            "a hard unsupported answer."
        )
        warnings = ["Cần bổ sung ngữ cảnh trước khi sinh SQL."]

    follow_ups = conversation.get("follow_ups") or []
    conversational_suggestions = [
        {
            "label": follow_up[:64],
            "question": follow_up,
            "reason": "Gợi ý tiếp theo dựa trên ngữ cảnh cuộc trò chuyện.",
            "intent": "clarification",
            "confidence": "medium",
        }
        for follow_up in follow_ups
        if isinstance(follow_up, str) and follow_up.strip()
    ]

    return {
        **state,
        "answer": answer,
        "answer_type": "answer" if answered else "clarification",
        "conversational_answer": answered,
        "conversational_suggestions": conversational_suggestions,
        "needs_clarification": not answered,
        "generated_sql": "",
        "used_tables": [],
        "row_count": 0,
        "rows": [],
        "warnings": warnings,
        "insights": [],
        "insight_source": "rule_based",
        "llm_insight_used": False,
        "confidence": "high" if answered else "low",
        "validation_notes": [validation_note],
        "chart": _empty_chart("Câu trả lời dạng hội thoại nên hiển thị dạng text và suggestion."),
        "chart_type": None,
        "chart_data": [],
        "metadata_used": {"tables": [], "columns": {}},
        "status": "success",
        "error_message": None,
    }


def ambiguity_check_node(state: AgentState) -> AgentState:
    assumptions = build_assumptions(state)
    suggestions = build_suggestions({**state, "assumptions": assumptions})
    needs = needs_clarification(state, assumptions, suggestions)
    validation_notes = list(state.get("validation_notes", []))
    if assumptions:
        validation_notes.extend(f"Assumption: {item}" for item in assumptions)
    return {
        **state,
        "assumptions": assumptions,
        "clarification_suggestions": suggestions,
        "needs_clarification": needs,
        "validation_notes": validation_notes,
    }


def text_to_sql_node(state: AgentState) -> AgentState:
    context_followup = bool(state.get("context_sql_followup"))
    generated_sql = generate_sql(
        state.get("effective_question") or state["question"],
        intent_result=state.get("intent_result"),
        metadata_context=state.get("metadata_context"),
        prefer_llm=context_followup,
        previous_sql=state.get("previous_sql") if context_followup else None,
    )
    applied_time_filter = resolve_applied_time_filter(
        state.get("intent_result"),
        state.get("metadata_context"),
        generated_sql,
    )
    validation_notes = list(state.get("validation_notes", []))
    if state.get("time_range") and not applied_time_filter:
        validation_notes.append("Time range was extracted, but no safe date filter was applied from available metadata.")
    return {
        **state,
        "generated_sql": generated_sql,
        "failed_sql": generated_sql,
        "applied_time_filter": applied_time_filter,
        "validation_notes": validation_notes,
    }


def guard_node(state: AgentState) -> AgentState:
    try:
        validated_sql = validate_sql(state.get("generated_sql", ""))
        return {
            **state,
            "generated_sql": validated_sql,
            "failed_sql": validated_sql,
            "guard_ok": True,
            "can_retry": False,
            "error_message": None,
        }
    except Exception as exc:
        failed_sql = state.get("generated_sql", "")
        error_message = str(exc)
        retry_count = state.get("retry_count", 0)
        non_retryable = _is_non_retryable_guard_error(error_message, failed_sql)
        can_retry = retry_count < MAX_SQL_RETRY_ATTEMPTS and not non_retryable
        updates: AgentState = {
            **state,
            "failed_sql": failed_sql,
            "guard_ok": False,
            "can_retry": can_retry,
            "status": "error",
            "error_message": error_message,
        }
        if not can_retry and retry_count >= MAX_SQL_RETRY_ATTEMPTS and not non_retryable:
            answer = f"Đã thử sửa SQL tối đa {MAX_SQL_RETRY_ATTEMPTS} lần nhưng vẫn thất bại."
            updates["answer"] = answer
            updates["error_message"] = f"{answer} Lỗi cuối từ SQL Guard: {error_message}"
        return updates


def execute_sql_node(state: AgentState) -> AgentState:
    try:
        _columns, rows = execute_query(state.get("generated_sql", ""), run_id=state.get("run_id"))
        return {
            **state,
            "rows": rows,
            "row_count": len(rows),
            "used_tables": _used_tables(state.get("generated_sql", "")),
            "execute_ok": True,
            "can_retry": False,
            "retry_success": bool(state.get("retry_attempted", False)),
            "error_message": None,
        }
    except Exception as exc:
        error_message = str(exc)
        retry_count = state.get("retry_count", 0)
        non_retryable = _is_non_retryable_execution_error(error_message)
        can_retry = retry_count < MAX_SQL_RETRY_ATTEMPTS and not non_retryable
        updates: AgentState = {
            **state,
            "failed_sql": state.get("generated_sql", ""),
            "execute_ok": False,
            "can_retry": can_retry,
            "status": "error",
            "error_message": error_message,
        }
        if not can_retry and retry_count >= MAX_SQL_RETRY_ATTEMPTS and not non_retryable:
            answer = f"Đã thử sửa SQL tối đa {MAX_SQL_RETRY_ATTEMPTS} lần nhưng vẫn thất bại."
            updates["answer"] = answer
            updates["error_message"] = f"{answer} Lỗi cuối từ Trino: {error_message}"
        return updates


def correct_sql_node(state: AgentState) -> AgentState:
    failed_sql = state.get("failed_sql") or state.get("generated_sql", "")
    attempt_number = state.get("retry_count", 0) + 1
    correction = correct_sql(
        question=state.get("effective_question") or state["question"],
        intent_result=_intent_dict(state),
        failed_sql=failed_sql,
        error_message=state.get("error_message") or "",
        table_candidates=state.get("table_candidates", []),
        metadata_context=state.get("metadata_context") or {"tables": [], "columns": {}},
        attempt_number=attempt_number,
    )
    correction_history = list(state.get("correction_history", []))
    correction_history.append(
        {
            "attempt": attempt_number,
            "failed_sql": failed_sql,
            "error_message": state.get("error_message") or "",
            "corrected_sql": correction["corrected_sql"],
            "correction_reason": correction["correction_reason"],
        }
    )
    if not correction["can_retry"]:
        return {
            **state,
            "retry_attempted": True,
            "correction_reason": correction["correction_reason"],
            "correction_history": correction_history,
            "can_retry": False,
            "status": "error",
            "error_message": (
                f"SQL correction skipped: {correction['correction_reason']}. "
                f"Original error: {state.get('error_message')}"
            ),
        }

    return {
        **state,
        "generated_sql": correction["corrected_sql"],
        "failed_sql": correction["corrected_sql"],
        "original_sql": state.get("original_sql") or failed_sql,
        "retry_attempted": True,
        "correction_reason": correction["correction_reason"],
        "correction_history": correction_history,
        "retry_count": attempt_number,
        "guard_ok": False,
        "execute_ok": False,
        "can_retry": False,
        "error_message": None,
    }


def validate_result_node(state: AgentState) -> AgentState:
    validation = validate_result(
        question=state.get("effective_question") or state["question"],
        intent=state.get("intent") or "",
        generated_sql=state.get("generated_sql", ""),
        rows=state.get("rows", []),
        row_count=state.get("row_count", 0),
        table_candidates=state.get("table_candidates", []),
        used_tables=state.get("used_tables", []),
    )
    validation_notes = list(state.get("validation_notes", []))
    validation_notes.extend(validation["validation_notes"])
    return {
        **state,
        "warnings": validation["warnings"],
        "confidence": validation["confidence"],
        "validation_notes": validation_notes,
    }


def chart_node(state: AgentState) -> AgentState:
    chart = recommend_chart(
        question=state["question"],
        intent=state.get("intent") or "",
        rows=state.get("rows", []),
        row_count=state.get("row_count", 0),
        generated_sql=state.get("generated_sql", ""),
        table_candidates=state.get("table_candidates", []),
        used_tables=state.get("used_tables", []),
        warnings=state.get("warnings", []),
    )
    return {
        **state,
        "chart": chart,
        "chart_type": _chart_type(chart),
        "chart_data": _chart_data(chart),
        "status": "success",
        "error_message": None,
    }


def insight_node(state: AgentState) -> AgentState:
    insight = generate_llm_insight(
        question=state.get("effective_question") or state["question"],
        intent=state.get("intent") or "",
        rows=state.get("rows", []),
        row_count=state.get("row_count", 0),
        warnings=state.get("warnings", []),
        generated_sql=state.get("generated_sql", ""),
        chart=state.get("chart") or _empty_chart("Không có chart recommendation."),
        confidence=state.get("confidence", "low"),
        table_candidates=state.get("table_candidates", []),
        used_tables=state.get("used_tables", []),
    )
    validation_notes = list(state.get("validation_notes", []))
    if insight.get("llm_insight_error"):
        validation_notes.append("LLM insight failed; used rule-based insight fallback.")
    return {
        **state,
        "answer": insight["answer"],
        "insights": insight["insights"],
        "insight_source": insight.get("insight_source", "rule_based"),
        "llm_insight_used": insight.get("llm_insight_used", False),
        "validation_notes": validation_notes,
    }


def _merge_suggestions(
    primary: list[dict[str, Any]],
    secondary: list[dict[str, Any]],
    limit: int = 5,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in [*primary, *secondary]:
        if not isinstance(item, dict):
            continue
        key = " ".join(str(item.get("question", "")).lower().split())
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged[:limit]


def suggestion_generation_node(state: AgentState) -> AgentState:
    assumptions = build_assumptions(state)
    suggestions = build_suggestions({**state, "assumptions": assumptions})
    # For conversational answers, lead with the LLM's context-aware follow-ups
    # and keep the deterministic chips as a backstop.
    conversational_suggestions = state.get("conversational_suggestions") or []
    if conversational_suggestions:
        suggestions = _merge_suggestions(conversational_suggestions, suggestions)
    needs = needs_clarification(state, assumptions, suggestions)
    enriched: AgentState = {
        **state,
        "assumptions": assumptions,
        "clarification_suggestions": suggestions,
        "needs_clarification": needs,
    }
    enriched["answer_type"] = answer_type_for(enriched)

    if enriched["answer_type"] == "empty_result" and not enriched.get("error_message"):
        enriched["answer"] = (
            "Không tìm thấy dữ liệu phù hợp với điều kiện hiện tại. "
            "Bạn có thể nới khoảng thời gian, đổi chiều phân tích hoặc dùng bảng summary qua các gợi ý bên dưới."
        )
    elif enriched["answer_type"] == "blocked":
        enriched["answer"] = enriched.get("answer") or "Yêu cầu này bị chặn để bảo vệ dữ liệu."

    return enriched


def save_context_node(state: AgentState) -> AgentState:
    updated = {**state, "execution_time_ms": _elapsed_ms(state)}
    response = _build_response(updated)
    try:
        save_turn(updated["session_id"], updated["question"], response)
    except Exception as exc:  # noqa: BLE001 - context save is never fatal
        warnings = list(response.get("warnings") or [])
        warnings.append(f"Không lưu được conversation context: {exc}")
        updated["warnings"] = warnings
        response = _build_response(updated)
    return {**updated, "response": response}


def output_node(state: AgentState) -> AgentState:
    return {**state, "response": state.get("response") or _build_response(state)}


def route_after_safety(state: AgentState) -> str:
    if state.get("terminal_action") == "blocked":
        return "suggestion_generation"
    return "resolve_followup"


def route_after_followup(state: AgentState) -> str:
    terminal = state.get("terminal_action")
    if terminal == "direct_response":
        return "suggestion_generation"
    if terminal == "conversational":
        return "assistive_clarification"
    if state.get("spec_ready"):
        # refine/entity_ref already produced a complete intent_result; skip the
        # fresh NLU parse and go straight to metadata + SQL.
        return "metadata"
    return "intent_router"


def route_after_intent(state: AgentState) -> str:
    intent = state.get("intent")
    if intent in {"metadata_tables", "metadata_columns"}:
        return "metadata_answer"
    if intent == "unsupported":
        # A follow-up refinement (e.g. "bỏ qua nhãn hàng unknown") may not classify
        # on its own; the LLM already rewrote it into a standalone question, so run
        # the SQL pipeline instead of returning a clarification.
        if state.get("context_sql_followup"):
            return "metadata"
        return "assistive_clarification"
    return "metadata"


def route_after_guard(state: AgentState) -> str:
    if state.get("guard_ok"):
        return "execute_sql"
    if state.get("can_retry"):
        return "correct_sql"
    return "suggestion_generation"


def route_after_execute(state: AgentState) -> str:
    if state.get("execute_ok"):
        return "validate_result"
    if state.get("can_retry"):
        return "correct_sql"
    return "suggestion_generation"


def route_after_correction(state: AgentState) -> str:
    if state.get("can_retry"):
        return "guard"
    if (
        state.get("generated_sql")
        and state.get("retry_count", 0) <= MAX_SQL_RETRY_ATTEMPTS
        and state.get("error_message") is None
    ):
        return "guard"
    return "suggestion_generation"


def _build_graph():
    builder = StateGraph(AgentState)
    builder.add_node("initialize", initialize_node)
    builder.add_node("load_context", load_context_node)
    builder.add_node("safety", safety_node)
    builder.add_node("resolve_followup", resolve_followup_node)
    builder.add_node("intent_router", intent_router_node)
    builder.add_node("metadata", metadata_node)
    builder.add_node("metadata_answer", metadata_answer_node)
    builder.add_node("assistive_clarification", assistive_clarification_node)
    builder.add_node("ambiguity_check", ambiguity_check_node)
    builder.add_node("text_to_sql", text_to_sql_node)
    builder.add_node("guard", guard_node)
    builder.add_node("execute_sql", execute_sql_node)
    builder.add_node("correct_sql", correct_sql_node)
    builder.add_node("validate_result", validate_result_node)
    builder.add_node("insight", insight_node)
    builder.add_node("chart", chart_node)
    builder.add_node("suggestion_generation", suggestion_generation_node)
    builder.add_node("save_context", save_context_node)
    builder.add_node("output", output_node)

    builder.set_entry_point("initialize")
    builder.add_edge("initialize", "load_context")
    builder.add_edge("load_context", "safety")
    builder.add_conditional_edges(
        "safety",
        route_after_safety,
        {"suggestion_generation": "suggestion_generation", "resolve_followup": "resolve_followup"},
    )
    builder.add_conditional_edges(
        "resolve_followup",
        route_after_followup,
        {
            "suggestion_generation": "suggestion_generation",
            "intent_router": "intent_router",
            "assistive_clarification": "assistive_clarification",
            "metadata": "metadata",
        },
    )
    builder.add_conditional_edges(
        "intent_router",
        route_after_intent,
        {"metadata_answer": "metadata_answer", "assistive_clarification": "assistive_clarification", "metadata": "metadata"},
    )
    builder.add_edge("metadata_answer", "suggestion_generation")
    builder.add_edge("assistive_clarification", "suggestion_generation")
    builder.add_edge("metadata", "ambiguity_check")
    builder.add_edge("ambiguity_check", "text_to_sql")
    builder.add_edge("text_to_sql", "guard")
    builder.add_conditional_edges(
        "guard",
        route_after_guard,
        {"execute_sql": "execute_sql", "correct_sql": "correct_sql", "suggestion_generation": "suggestion_generation"},
    )
    builder.add_conditional_edges(
        "execute_sql",
        route_after_execute,
        {"validate_result": "validate_result", "correct_sql": "correct_sql", "suggestion_generation": "suggestion_generation"},
    )
    builder.add_conditional_edges(
        "correct_sql",
        route_after_correction,
        {"guard": "guard", "suggestion_generation": "suggestion_generation"},
    )
    builder.add_edge("validate_result", "chart")
    builder.add_edge("chart", "insight")
    builder.add_edge("insight", "suggestion_generation")
    builder.add_edge("suggestion_generation", "save_context")
    builder.add_edge("save_context", "output")
    builder.add_edge("output", END)
    return builder.compile()


_GRAPH = None


def get_graph():
    """Compile the v2 graph once and cache it."""
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = _build_graph()
    return _GRAPH


def run_agent_graph(
    session_id: str,
    question: str,
    user_id: str | None = None,
    run_id: str | None = None,
    recent_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    initial_state: AgentState = {
        "session_id": session_id,
        "user_id": user_id,
        "run_id": run_id,
        "question": question,
        "recent_context": recent_context or [],
        "start_time": time.perf_counter(),
    }
    try:
        final_state = get_graph().invoke(initial_state)
        save_checkpoint_snapshot(session_id, new_checkpoint_id(session_id), final_state)
        return final_state["response"]
    except Exception as exc:
        error_state: AgentState = {
            **initial_state,
            "generated_sql": "",
            "used_tables": [],
            "rows": [],
            "row_count": 0,
            "warnings": [],
            "insights": [],
            "insight_source": "rule_based",
            "llm_insight_used": False,
            "confidence": "low",
            "validation_notes": [],
            "chart": _empty_chart("Không đề xuất chart khi graph lỗi."),
            "chart_type": None,
            "chart_data": [],
            "answer": "",
            "status": "error",
            "error_message": str(exc),
            "intent": None,
            "analysis_type": None,
            "time_range": None,
            "time_grain": None,
            "applied_time_filter": None,
            "filters": [],
            "comparison_entities": [],
            "sort_direction": None,
            "extracted_entities": {},
            "nlu_confidence": "low",
            "table_candidates": [],
            "metadata_used": {"tables": [], "columns": {}},
            "retry_attempted": False,
            "retry_success": False,
            "retry_count": 0,
            "original_sql": None,
            "correction_reason": None,
            "correction_history": [],
            "context_used": False,
            "resolved_question": None,
            "previous_turn_id": None,
            "previous_question": None,
            "context_notes": [],
            "model_used": _model_used(),
        }
        return _build_response(error_state)
