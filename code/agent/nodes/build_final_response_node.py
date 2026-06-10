from datetime import date, datetime
from decimal import Decimal
import os


def make_json_ready(value):
    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, list):
        return [make_json_ready(item) for item in value]

    if isinstance(value, dict):
        return {
            key: make_json_ready(item)
            for key, item in value.items()
        }

    return value


def default_chart_spec():
    return {
        "type": "none",
        "title": "Không có biểu đồ",
        "x": None,
        "y": None,
        "data": [],
        "reason": "Chưa có chart_spec."
    }


def build_result(rows, profile):
    max_rows = int(os.getenv("AGENT_MAX_RESPONSE_ROWS", "100"))
    is_truncated = len(rows) > max_rows
    visible_rows = rows[:max_rows]
    return {
        "row_count": profile.get("row_count", len(rows)),
        "columns": profile.get("columns", []),
        "rows": visible_rows,
        "is_truncated": is_truncated,
    }


def build_chart_suggestion(chart_spec, columns):
    chart_type = chart_spec.get("chart_type") or chart_spec.get("type") or "none"
    x_column = chart_spec.get("x")
    y_column = chart_spec.get("y")

    if x_column and x_column not in columns:
        x_column = None
    if y_column and y_column not in columns:
        y_column = None

    if chart_type not in ["bar", "line", "pie", "table", "scatter", "none"]:
        chart_type = "table"

    if chart_type in ["bar", "line", "pie", "scatter"] and (not x_column or not y_column):
        chart_type = "table" if columns else "none"

    return {
        "chart_type": chart_type,
        "x": x_column,
        "y": y_column,
        "title": chart_spec.get("title") or "",
        "reason": chart_spec.get("reason") or "",
    }


def build_blocks(status, error, insight_summary, chart_spec, result, sql):
    blocks = []

    if status == "error":
        blocks.append({
            "type": "error",
            "title": "Lỗi",
            "content": error
        })

    if insight_summary:
        blocks.append({
            "type": "insight",
            "title": "Nhận định",
            "content": insight_summary
        })

    missing_info = result.get("missing_info") or {}
    if missing_info.get("has_missing_info"):
        blocks.append({
            "type": "missing_info",
            "title": "Thông tin còn thiếu",
            "content": missing_info
        })

    if chart_spec.get("type") not in ["none", "table"]:
        blocks.append({
            "type": "chart",
            "title": chart_spec.get("title") or "Biểu đồ",
            "spec": chart_spec
        })

    if status == "success" or result["rows"]:
        blocks.append({
            "type": "table",
            "title": "Dữ liệu kết quả",
            "columns": result["columns"],
            "rows": result["rows"]
        })

    if sql:
        blocks.append({
            "type": "sql",
            "title": "SQL đã thực thi",
            "content": sql
        })

    return blocks


def build_final_response_node(state):
    rows = make_json_ready(state.get("query_result") or [])
    profile = make_json_ready(state.get("result_profile") or {})
    chart_spec = make_json_ready(state.get("chart_spec") or default_chart_spec())
    insight_summary = state.get("insight_summary")
    insight_error = state.get("insight_error")
    missing_info = make_json_ready(
        state.get("missing_info") or {
            "has_missing_info": False,
            "items": [],
            "can_requery": False,
            "notes": "",
        }
    )
    sql = state.get("generated_sql") or ""
    error = state.get("error")
    status = "error" if error else "success"
    result = build_result(rows, profile)
    result["missing_info"] = missing_info
    chart_suggestion = build_chart_suggestion(chart_spec, result["columns"])
    answer_kind = state.get("answer_kind")
    if not answer_kind:
        if error:
            answer_kind = "error"
        elif missing_info.get("has_missing_info") and not rows:
            answer_kind = "no_data"
        else:
            answer_kind = "data_answer"
    text_answer = state.get("text_answer") or insight_summary or error or ""

    final_answer = {
        "request_id": state.get("request_id"),
        "session_id": state.get("session_id"),
        "user_id": state.get("user_id"),
        "status": status,
        "answer_kind": answer_kind,
        "question": state.get("user_question") or "",
        "text_answer": text_answer,
        "sql": sql,
        "readonly": True,
        "safety": make_json_ready(state.get("safety") or {}),
        "sql_validation": make_json_ready(state.get("sql_validation") or {}),
        "sql_attempts": make_json_ready(state.get("sql_attempts") or []),
        "metadata": {
            "source": state.get("metadata_source"),
            "warning": state.get("metadata_warning"),
        },
        "result": result,
        "analysis": {
            "insight_summary": insight_summary,
            "insight_error": insight_error,
            "result_profile": profile,
            "result_validation": make_json_ready(state.get("result_validation") or {}),
            "entity_resolution": {
                "resolved_entities": make_json_ready(state.get("resolved_entities") or []),
                "warning": state.get("entity_resolution_warning"),
            },
            "missing_info": missing_info
        },
        "chart_suggestion": chart_suggestion,
        "visualization": {
            "chart_spec": chart_spec
        },
        "blocks": build_blocks(
            status,
            error,
            insight_summary,
            chart_spec,
            result,
            sql
        ),
        "context": {
            "warning": state.get("context_warning"),
            "stop_reason": state.get("stop_reason"),
        },
        "error": error
    }

    return {
        "final_answer": final_answer
    }
