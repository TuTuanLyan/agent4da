import json
import re
from typing import Any

from psycopg2.extras import Json

from db_context import _connect, _ensure_context_tables


def _ensure_turn_table(cursor) -> None:
    _ensure_context_tables(cursor)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_context.conversation_turns (
            turn_id BIGSERIAL PRIMARY KEY,
            session_id TEXT NOT NULL,
            question TEXT NOT NULL,
            intent TEXT,
            generated_sql TEXT,
            answer TEXT,
            used_tables JSONB,
            table_candidates JSONB,
            chart JSONB,
            row_count BIGINT,
            status TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
    )


def get_recent_context(session_id: str, limit: int = 5) -> list[dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cursor:
            _ensure_turn_table(cursor)
            cursor.execute(
                """
                SELECT
                    turn_id,
                    session_id,
                    question,
                    intent,
                    generated_sql,
                    answer,
                    used_tables,
                    table_candidates,
                    chart,
                    row_count,
                    status,
                    created_at
                FROM app_context.conversation_turns
                WHERE session_id = %s
                ORDER BY created_at DESC, turn_id DESC
                LIMIT %s
                """,
                (session_id, limit),
            )
            rows = cursor.fetchall()

    context = []
    for row in rows:
        context.append(
            {
                "turn_id": row[0],
                "session_id": row[1],
                "question": row[2],
                "intent": row[3],
                "generated_sql": row[4],
                "answer": row[5],
                "used_tables": row[6] or [],
                "table_candidates": row[7] or [],
                "chart": row[8] or {},
                "row_count": row[9] or 0,
                "status": row[10],
                "created_at": row[11].isoformat() if row[11] else None,
            }
        )
    return context


def save_turn(session_id: str, question: str, response: dict[str, Any]) -> None:
    with _connect() as conn:
        with conn.cursor() as cursor:
            _ensure_turn_table(cursor)
            cursor.execute(
                """
                INSERT INTO app_context.conversation_turns (
                    session_id,
                    question,
                    intent,
                    generated_sql,
                    answer,
                    used_tables,
                    table_candidates,
                    chart,
                    row_count,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    question,
                    response.get("intent"),
                    response.get("generated_sql") or "",
                    response.get("answer") or "",
                    Json(response.get("used_tables") or []),
                    Json(response.get("table_candidates") or []),
                    Json(response.get("chart") or {}),
                    int(response.get("row_count") or 0),
                    response.get("status"),
                ),
            )


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _extract_limit(text: str) -> int | None:
    match = re.search(r"\btop\s+(\d{1,3})\b", text)
    if match:
        return max(1, min(int(match.group(1)), 100))

    match = re.search(r"\b(\d{1,3})\s+thôi\b", text)
    if match:
        return max(1, min(int(match.group(1)), 100))
    return None


def _successful_previous(recent_context: list[dict[str, Any]]) -> dict[str, Any] | None:
    for turn in recent_context:
        if turn.get("status") == "success":
            return turn
    return recent_context[0] if recent_context else None


def _dimension_from_previous(previous: dict[str, Any]) -> str:
    candidates = previous.get("table_candidates") or []
    sql = (previous.get("generated_sql") or "").lower()
    question = (previous.get("question") or "").lower()
    if "daily_category_summary" in candidates or "daily_category_summary" in sql or "category" in question:
        return "category"
    if "daily_product_summary" in candidates or "daily_product_summary" in sql or "product" in question or "sản phẩm" in question:
        return "sản phẩm"
    return "brand"


def _metric_phrase_from_previous(previous: dict[str, Any]) -> str:
    sql = (previous.get("generated_sql") or "").lower()
    question = (previous.get("question") or "").lower()
    if "revenue" in sql or "gross_amount" in sql or "doanh thu" in question:
        return "doanh thu cao"
    if "view" in sql or "view" in question:
        return "view nhiều"
    if "cart" in sql or "cart" in question:
        return "cart nhiều"
    if "purchase" in sql or "purchase" in question:
        return "purchase nhiều"
    return "event nhiều"


def _ranking_question(dimension: str, metric_phrase: str, limit: int | None = None) -> str:
    prefix = f"Top {limit} " if limit else ""
    if metric_phrase == "doanh thu cao":
        return f"{prefix}{dimension} nào có doanh thu cao nhất? Trả về {dimension} và doanh thu."
    if metric_phrase == "event nhiều":
        return f"{prefix}{dimension} nào có nhiều event nhất? Trả về {dimension} và số event."
    if metric_phrase == "view nhiều":
        return f"{prefix}{dimension} nào có nhiều view nhất? Trả về {dimension} và số view."
    if metric_phrase == "cart nhiều":
        return f"{prefix}{dimension} nào có nhiều cart nhất? Trả về {dimension} và số cart."
    if metric_phrase == "purchase nhiều":
        return f"{prefix}{dimension} nào có nhiều purchase nhất? Trả về {dimension} và số purchase."
    return f"{prefix}{dimension} nào có {metric_phrase} nhất?"


def _explain_sql(sql: str) -> str:
    normalized = " ".join((sql or "").split())
    if not normalized:
        return "Không có SQL trước đó để giải thích trong session này."

    table_match = re.search(
        r"\bFROM\s+(iceberg_catalog\.gold\.[A-Za-z_][A-Za-z0-9_]*)",
        normalized,
        flags=re.IGNORECASE,
    )
    select_match = re.search(r"^\s*SELECT\s+(.*?)\s+FROM\s+", normalized, flags=re.IGNORECASE)
    order_match = re.search(r"\bORDER\s+BY\s+(.*?)(?:\s+LIMIT\b|$)", normalized, flags=re.IGNORECASE)
    limit_match = re.search(r"\bLIMIT\s+(\d+)", normalized, flags=re.IGNORECASE)

    parts = []
    if select_match:
        parts.append(f"SQL lấy các cột/chỉ số: {select_match.group(1)}.")
    if table_match:
        parts.append(f"Dữ liệu được đọc từ bảng Gold: {table_match.group(1)}.")
    if order_match:
        parts.append(f"Kết quả được sắp xếp theo: {order_match.group(1)}.")
    if limit_match:
        parts.append(f"Truy vấn chỉ lấy tối đa {limit_match.group(1)} dòng.")

    if not parts:
        return f"SQL vừa rồi là truy vấn đọc dữ liệu trong Gold: {normalized}"
    return " ".join(parts)


def resolve_followup(question: str, recent_context: list[dict[str, Any]]) -> dict[str, Any]:
    text = question.strip().lower()
    previous = _successful_previous(recent_context)
    if not previous:
        return {
            "context_used": False,
            "action": None,
            "resolved_question": None,
            "previous_turn_id": None,
            "previous_question": None,
            "context_notes": [],
        }

    base = {
        "context_used": True,
        "previous_turn_id": previous.get("turn_id"),
        "previous_question": previous.get("question"),
        "context_notes": [],
    }

    if _contains_any(text, ("vẽ biểu đồ", "ve bieu do", "chart", "biểu đồ câu trên", "bieu do cau tren")):
        return {
            **base,
            "action": "reuse_chart",
            "resolved_question": None,
            "chart": previous.get("chart") or {},
            "generated_sql": previous.get("generated_sql") or "",
            "used_tables": previous.get("used_tables") or [],
            "table_candidates": previous.get("table_candidates") or [],
            "intent": previous.get("intent"),
            "row_count": previous.get("row_count") or 0,
            "answer": "Dưới đây là gợi ý biểu đồ cho kết quả trước.",
            "context_notes": ["Reused chart recommendation from the previous successful turn."],
        }

    if _contains_any(text, ("giải thích sql", "giai thich sql", "sql vừa rồi", "sql vua roi", "câu query vừa rồi", "cau query vua roi")):
        return {
            **base,
            "action": "explain_sql",
            "resolved_question": None,
            "generated_sql": previous.get("generated_sql") or "",
            "used_tables": previous.get("used_tables") or [],
            "table_candidates": previous.get("table_candidates") or [],
            "intent": previous.get("intent"),
            "answer": _explain_sql(previous.get("generated_sql") or ""),
            "context_notes": ["Explained SQL from the previous successful turn without executing a new query."],
        }

    previous_is_ranking = previous.get("intent") == "ranking"
    if previous_is_ranking and _contains_any(text, ("category", "danh mục", "danh muc", "thế category", "the category", "còn category", "con category")):
        metric_phrase = _metric_phrase_from_previous(previous)
        resolved_question = _ranking_question("category", metric_phrase)
        return {
            **base,
            "action": "resolved_question",
            "resolved_question": resolved_question,
            "context_notes": ["Resolved category follow-up from previous ranking question."],
        }

    if previous_is_ranking and _contains_any(text, ("product", "sản phẩm", "san pham", "thế sản phẩm", "the san pham", "còn sản phẩm", "con san pham")):
        metric_phrase = _metric_phrase_from_previous(previous)
        resolved_question = _ranking_question("sản phẩm", metric_phrase)
        return {
            **base,
            "action": "resolved_question",
            "resolved_question": resolved_question,
            "context_notes": ["Resolved product follow-up from previous ranking question."],
        }

    limit = _extract_limit(text)
    if previous_is_ranking and limit and _contains_any(text, ("top", "thôi", "thoi")):
        dimension = _dimension_from_previous(previous)
        metric_phrase = _metric_phrase_from_previous(previous)
        resolved_question = _ranking_question(dimension, metric_phrase, limit=limit)
        return {
            **base,
            "action": "resolved_question",
            "resolved_question": resolved_question,
            "context_notes": [f"Reused previous ranking dimension and changed limit to {limit}."],
        }

    return {
        "context_used": False,
        "action": None,
        "resolved_question": None,
        "previous_turn_id": None,
        "previous_question": None,
        "context_notes": [],
    }
