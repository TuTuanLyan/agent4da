"""Conversation context for the v2 engine.

Replaces code/api's psycopg2-backed `conversation_context` + `db_context` with a
process-local store (single-replica MVP, same trade-off as `agent.cancellation`).
Holds the last few turns per session so the graph can resolve follow-ups
("vẽ biểu đồ", "giải thích SQL", "còn category thì sao", "top 5 thôi").

The durable record of every run still lives in `app.query_runs` (written by the
agent service); this store only powers in-conversation follow-ups.
"""

from __future__ import annotations

import itertools
import re
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List

from .config import GOLD_PREFIX

_MAX_TURNS_PER_SESSION = 10
_LOCK = threading.Lock()
_STORE: Dict[str, Deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=_MAX_TURNS_PER_SESSION))
_TURN_IDS = itertools.count(1)


def get_recent_context(session_id: str, limit: int = 5) -> List[dict[str, Any]]:
    """Most-recent-first list of prior turns for the session."""
    with _LOCK:
        turns = list(_STORE.get(session_id, ()))
    turns.reverse()  # deque holds oldest-first; callers expect newest-first
    return turns[:limit]


def save_turn(session_id: str, question: str, response: dict[str, Any]) -> None:
    turn = {
        "turn_id": next(_TURN_IDS),
        "session_id": session_id,
        "question": question,
        "intent": response.get("intent"),
        "generated_sql": response.get("generated_sql") or "",
        "answer": response.get("answer") or "",
        "used_tables": response.get("used_tables") or [],
        "table_candidates": response.get("table_candidates") or [],
        "chart": response.get("chart") or {},
        "clarification_suggestions": response.get("clarification_suggestions") or [],
        "assumptions": response.get("assumptions") or [],
        "row_count": int(response.get("row_count") or 0),
        "status": response.get("status"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    with _LOCK:
        _STORE[session_id].append(turn)


def clear_session(session_id: str) -> None:
    with _LOCK:
        _STORE.pop(session_id, None)


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
        rf"\bFROM\s+({GOLD_PREFIX}\.[A-Za-z_][A-Za-z0-9_]*)",
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
