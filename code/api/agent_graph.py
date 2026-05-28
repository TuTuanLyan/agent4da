import re
import time
from typing import Any, NotRequired, TypedDict

from langgraph.graph import END, StateGraph

from chart_recommender import recommend_chart
from checkpoint_context import create_postgres_checkpointer, latest_checkpoint_id, save_checkpoint_snapshot
from conversation_context import get_recent_context, resolve_followup, save_turn
from db_context import ensure_session, log_query
from insight_generator import generate_insight, generate_llm_insight
from intent_router import route_intent
from llm_sql import generate_sql, resolve_applied_time_filter
from metadata_service import get_gold_tables, get_table_columns, select_metadata
from result_validator import validate_result
from sql_corrector import MAX_SQL_RETRY_ATTEMPTS, correct_sql
from sql_guard import validate_sql
from trino_client import execute_query


class AgentState(TypedDict):
    session_id: str
    user_id: NotRequired[str | None]
    question: str
    effective_question: NotRequired[str]
    resolved_question: NotRequired[str | None]
    context_used: NotRequired[bool]
    previous_turn_id: NotRequired[int | None]
    previous_question: NotRequired[str | None]
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
    status: NotRequired[str]
    error_message: NotRequired[str | None]
    retry_attempted: NotRequired[bool]
    retry_success: NotRequired[bool]
    correction_reason: NotRequired[str | None]
    retry_count: NotRequired[int]
    correction_history: NotRequired[list[dict[str, Any]]]
    start_time: NotRequired[float]
    execution_time_ms: NotRequired[int]
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


def _metadata_columns_sql(table_name: str) -> str:
    return (
        "SELECT column_name, data_type "
        "FROM iceberg_catalog.information_schema.columns "
        "WHERE table_schema = 'gold' "
        f"AND table_name = '{table_name}' "
        "ORDER BY ordinal_position"
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
            "catalog 'iceberg_catalog' not found",
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
    }


def initialize_node(state: AgentState) -> AgentState:
    ensure_session(state["session_id"], state.get("user_id"))
    return {
        **state,
        "effective_question": state["question"],
        "resolved_question": None,
        "context_used": False,
        "previous_turn_id": None,
        "previous_question": None,
        "context_notes": [],
        "recent_context": [],
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
        "status": "error",
        "error_message": None,
        "retry_attempted": False,
        "retry_success": False,
        "correction_reason": None,
        "retry_count": 0,
        "correction_history": [],
        "terminal_action": None,
        "guard_ok": False,
        "execute_ok": False,
        "can_retry": False,
        "response": {},
    }


def load_context_node(state: AgentState) -> AgentState:
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
        "warnings": [error_message],
        "validation_notes": ["Dangerous user request was blocked before SQL generation."],
        "chart": _empty_chart("Yêu cầu bị chặn nên không có chart recommendation."),
        "status": "blocked",
        "error_message": error_message,
        "terminal_action": "blocked",
    }


def resolve_followup_node(state: AgentState) -> AgentState:
    followup = resolve_followup(state["question"], state.get("recent_context", []))
    resolved_question = followup.get("resolved_question")
    updates: AgentState = {
        **state,
        "context_used": bool(followup.get("context_used")),
        "resolved_question": resolved_question,
        "previous_turn_id": followup.get("previous_turn_id"),
        "previous_question": followup.get("previous_question"),
        "context_notes": list(followup.get("context_notes") or []),
    }

    if resolved_question:
        updates["effective_question"] = resolved_question

    if followup.get("action") == "reuse_chart":
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

    if followup.get("action") == "explain_sql":
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
        generated_sql = "SHOW TABLES FROM iceberg_catalog.gold"
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


def unsupported_node(state: AgentState) -> AgentState:
    return {
        **state,
        "answer": "Câu hỏi này nằm ngoài phạm vi dữ liệu e-commerce Gold hiện tại.",
        "generated_sql": "",
        "used_tables": [],
        "row_count": 0,
        "rows": [],
        "warnings": [],
        "insights": [],
        "insight_source": "rule_based",
        "llm_insight_used": False,
        "confidence": "low",
        "validation_notes": [],
        "chart": _empty_chart("Câu hỏi ngoài phạm vi không có chart recommendation."),
        "chart_type": None,
        "chart_data": [],
        "metadata_used": {"tables": [], "columns": {}},
        "status": "success",
        "error_message": None,
    }


def text_to_sql_node(state: AgentState) -> AgentState:
    generated_sql = generate_sql(
        state.get("effective_question") or state["question"],
        intent_result=state.get("intent_result"),
        metadata_context=state.get("metadata_context"),
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
        _columns, rows = execute_query(state.get("generated_sql", ""))
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


def save_context_node(state: AgentState) -> AgentState:
    updated = {**state, "execution_time_ms": _elapsed_ms(state)}
    status = updated.get("status", "error")
    try:
        log_query(
            updated["session_id"],
            updated["question"],
            updated.get("generated_sql", ""),
            status,
            int(updated["execution_time_ms"]),
            updated.get("error_message"),
        )
    except Exception as exc:
        warnings = list(updated.get("warnings", []))
        warnings.append(f"Không ghi được query log: {exc}")
        updated["warnings"] = warnings

    response = _build_response(updated)
    try:
        save_turn(updated["session_id"], updated["question"], response)
    except Exception as exc:
        warnings = list(response.get("warnings") or [])
        warnings.append(f"Không lưu được conversation context: {exc}")
        updated["warnings"] = warnings
        response = _build_response(updated)
    return {**updated, "response": response}


def output_node(state: AgentState) -> AgentState:
    return {**state, "response": state.get("response") or _build_response(state)}


def route_after_safety(state: AgentState) -> str:
    if state.get("terminal_action") == "blocked":
        return "save_context"
    return "resolve_followup"


def route_after_followup(state: AgentState) -> str:
    if state.get("terminal_action") == "direct_response":
        return "save_context"
    return "intent_router"


def route_after_intent(state: AgentState) -> str:
    intent = state.get("intent")
    if intent in {"metadata_tables", "metadata_columns"}:
        return "metadata_answer"
    if intent == "unsupported":
        return "unsupported"
    return "metadata"


def route_after_guard(state: AgentState) -> str:
    if state.get("guard_ok"):
        return "execute_sql"
    if state.get("can_retry"):
        return "correct_sql"
    return "save_context"


def route_after_execute(state: AgentState) -> str:
    if state.get("execute_ok"):
        return "validate_result"
    if state.get("can_retry"):
        return "correct_sql"
    return "save_context"


def route_after_correction(state: AgentState) -> str:
    if state.get("can_retry"):
        return "guard"
    if (
        state.get("generated_sql")
        and state.get("retry_count", 0) <= MAX_SQL_RETRY_ATTEMPTS
        and state.get("error_message") is None
    ):
        return "guard"
    return "save_context"


def _build_graph(checkpointer):
    builder = StateGraph(AgentState)
    builder.add_node("initialize", initialize_node)
    builder.add_node("load_context", load_context_node)
    builder.add_node("safety", safety_node)
    builder.add_node("resolve_followup", resolve_followup_node)
    builder.add_node("intent_router", intent_router_node)
    builder.add_node("metadata", metadata_node)
    builder.add_node("metadata_answer", metadata_answer_node)
    builder.add_node("unsupported", unsupported_node)
    builder.add_node("text_to_sql", text_to_sql_node)
    builder.add_node("guard", guard_node)
    builder.add_node("execute_sql", execute_sql_node)
    builder.add_node("correct_sql", correct_sql_node)
    builder.add_node("validate_result", validate_result_node)
    builder.add_node("insight", insight_node)
    builder.add_node("chart", chart_node)
    builder.add_node("save_context", save_context_node)
    builder.add_node("output", output_node)

    builder.set_entry_point("initialize")
    builder.add_edge("initialize", "load_context")
    builder.add_edge("load_context", "safety")
    builder.add_conditional_edges(
        "safety",
        route_after_safety,
        {"save_context": "save_context", "resolve_followup": "resolve_followup"},
    )
    builder.add_conditional_edges(
        "resolve_followup",
        route_after_followup,
        {"save_context": "save_context", "intent_router": "intent_router"},
    )
    builder.add_conditional_edges(
        "intent_router",
        route_after_intent,
        {
            "metadata_answer": "metadata_answer",
            "unsupported": "unsupported",
            "metadata": "metadata",
        },
    )
    builder.add_edge("metadata_answer", "save_context")
    builder.add_edge("unsupported", "save_context")
    builder.add_edge("metadata", "text_to_sql")
    builder.add_edge("text_to_sql", "guard")
    builder.add_conditional_edges(
        "guard",
        route_after_guard,
        {"execute_sql": "execute_sql", "correct_sql": "correct_sql", "save_context": "save_context"},
    )
    builder.add_conditional_edges(
        "execute_sql",
        route_after_execute,
        {"validate_result": "validate_result", "correct_sql": "correct_sql", "save_context": "save_context"},
    )
    builder.add_conditional_edges(
        "correct_sql",
        route_after_correction,
        {"guard": "guard", "save_context": "save_context"},
    )
    builder.add_edge("validate_result", "chart")
    builder.add_edge("chart", "insight")
    builder.add_edge("insight", "save_context")
    builder.add_edge("save_context", "output")
    builder.add_edge("output", END)
    return builder.compile(checkpointer=checkpointer)


CHECKPOINTER = create_postgres_checkpointer()
AGENT_GRAPH = _build_graph(CHECKPOINTER)


def run_agent_graph(session_id: str, question: str, user_id: str | None = None) -> dict[str, Any]:
    initial_state: AgentState = {
        "session_id": session_id,
        "user_id": user_id,
        "question": question,
        "start_time": time.perf_counter(),
    }
    try:
        graph_config = {"configurable": {"thread_id": session_id}}
        final_state = AGENT_GRAPH.invoke(initial_state, config=graph_config)
        checkpoint_id = latest_checkpoint_id(CHECKPOINTER, session_id)
        save_checkpoint_snapshot(session_id, checkpoint_id, final_state)
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
        }
        return _build_response(error_state)
