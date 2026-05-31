"""Conversation context for the v2 engine.

Replaces code/api's psycopg2-backed `conversation_context` + `db_context` with a
process-local store (single-replica MVP, same trade-off as `agent.cancellation`).
Holds the last few turns per session so the graph can resolve follow-ups.

Two layers of follow-up resolution:

1. `resolve_followup` - fast, deterministic, keyword-driven. Handles a fixed set
   of canned follow-ups with no LLM call ("vẽ biểu đồ", "giải thích SQL",
   "còn category thì sao", "top 5 thôi").
2. `llm_rewrite_followup` - general fallback. When the deterministic resolver
   finds nothing, the Groq LLM rewrites an elliptical follow-up ("bỏ qua nhãn
   hàng unknown", "chỉ năm 2021", "thêm doanh thu") into a standalone analytics
   question by merging it with the recent turns, so it can run through the
   normal guarded read-only SQL pipeline. Degrades to a no-op (is_followup
   False) when no GROQ key is configured or any error occurs.

The durable record of every run still lives in `app.query_runs` (written by the
agent service); this store only powers in-conversation follow-ups.
"""

from __future__ import annotations

import itertools
import json
import os
import re
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List

from . import llm
from .config import GOLD_PREFIX
from .nlu import analyze_message, extract_exclusion_filters, extract_inclusion_filters, parse_nlu
from .spec import DIMENSIONAL_INTENTS, canonical_spec, merge_spec, render_spec_question

_MAX_TURNS_PER_SESSION = 10
_MAX_SAMPLE_ROWS = 20
_LOCK = threading.Lock()
_STORE: Dict[str, Deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=_MAX_TURNS_PER_SESSION))
_TURN_IDS = itertools.count(1)


def get_recent_context(session_id: str, limit: int = 5) -> List[dict[str, Any]]:
    """Most-recent-first list of prior turns for the session."""
    with _LOCK:
        turns = list(_STORE.get(session_id, ()))
    turns.reverse()  # deque holds oldest-first; callers expect newest-first
    return turns[:limit]


def _result_sample(rows: Any) -> tuple[list[str], list[dict[str, Any]]]:
    """Top-N rows + column names, used to resolve entity references next turn."""
    if not isinstance(rows, list) or not rows:
        return [], []
    sample = [row for row in rows[:_MAX_SAMPLE_ROWS] if isinstance(row, dict)]
    columns = list(sample[0].keys()) if sample else []
    return columns, sample


def save_turn(session_id: str, question: str, response: dict[str, Any]) -> None:
    result_columns, result_sample = _result_sample(response.get("rows"))
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
        # Active Query Spec + a small result sample so the next turn can resolve
        # refinements and entity references without re-deriving context.
        "effective_spec": canonical_spec(response),
        "result_columns": result_columns,
        "result_sample": result_sample,
        "clarification": {
            "needs_clarification": bool(response.get("needs_clarification")),
            "suggestions": response.get("clarification_suggestions") or [],
        },
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

    # Exclusion refinement ("bỏ qua nhãn hàng unknown") on a dimensional previous
    # turn: carry the previous question forward and re-run with the new NOT IN
    # filter. Deterministic, so it works without the LLM rewriter.
    exclusion_filters = extract_exclusion_filters(text)
    previous_question = (previous.get("question") or "").strip()
    dimensional_intents = {"ranking", "comparison", "breakdown", "revenue_sales", "trend", "metric_overview"}
    if exclusion_filters and previous_question and previous.get("intent") in dimensional_intents:
        excluded = ", ".join(value for spec in exclusion_filters for value in spec.get("values", []))
        merged_question = f"{previous_question} {question.strip()}".strip()
        return {
            **base,
            "action": "resolved_question",
            "resolved_question": merged_question,
            "context_notes": [f"Carried the previous question forward and excluded: {excluded}."],
        }

    return {
        "context_used": False,
        "action": None,
        "resolved_question": None,
        "previous_turn_id": None,
        "previous_question": None,
        "context_notes": [],
    }


# ---------------------------------------------------------------------------
# Turn classifier: map each message to an operation on the Active Query Spec
# ---------------------------------------------------------------------------
#
# This is the general successor to the canned `resolve_followup` rules. It looks
# at the message + the last successful turn's spec/result and returns one of:
#   new_query | refine | presentation | entity_ref | clarification_answer
#   | meta | reset | ambiguous
# plus a merged spec (for refine/entity_ref) the graph can run directly.

_RESET_CUES = (
    "quên đi", "quen di", "quên cái trên", "quen cai tren", "bỏ qua câu trên",
    "bo qua cau tren", "chuyển chủ đề", "chuyen chu de", "câu khác", "cau khac",
    "bắt đầu lại", "bat dau lai", "làm lại từ đầu", "lam lai tu dau",
    "reset", "start over", "new topic",
)
_META_CUES = (
    "bạn làm được gì", "ban lam duoc gi", "bạn là ai", "ban la ai", "giúp được gì",
    "giup duoc gi", "câu trước hỏi gì", "cau truoc hoi gi", "bạn giúp", "ban giup",
    "what can you do", "who are you", "help me with", "xin chào", "xin chao",
    "hello", "cảm ơn", "cam on", "thank",
)
_METRIC_SWITCH_CUES = ("đổi", "doi", "thay", "sang", "instead", "switch", "thay vì", "thay vi", "chuyển sang", "chuyen sang")
_METRIC_ADD_CUES = ("thêm", "them", "kèm", "kem", "cùng", "cung", "add", "plus", "và cả", "va ca")
_DIM_SWITCH_CUES = ("theo", "còn", "con", "chuyển", "chuyen", "đổi", "doi", "by ", "thế còn", "the con")
_SORT_FLIP_CUES = ("ngược lại", "nguoc lai", "reverse", "đảo", "dao")
_REMOVE_FILTER_CUES = ("bỏ lọc", "bo loc", "không lọc", "khong loc", "bỏ điều kiện", "bo dieu kien", "bỏ filter", "gỡ lọc", "go loc", "bỏ bộ lọc", "bo bo loc")
_INTERROGATIVES = (
    "nào", "nao", "bao nhiêu", "bao nhieu", "thế nào", "the nao", "là gì", "la gi",
    "liệt kê", "liet ke", "cho tôi", "cho toi", "hiển thị", "hien thi", "show ",
    "list ", "what ", "how many", "which ",
)


def _no_context_result() -> dict[str, Any]:
    return {"op": "new_query", "context_used": False, "merged_spec": None, "context_notes": []}


def _label_column(columns: list[str], sample: list[dict[str, Any]]) -> str | None:
    for col in columns:
        values = [row.get(col) for row in sample if isinstance(row, dict)]
        if values and all(
            (v is None) or (not isinstance(v, (int, float))) or isinstance(v, bool) for v in values
        ):
            return col
    return columns[0] if columns else None


def _label_values(previous: dict[str, Any]) -> tuple[str | None, list[str]]:
    sample = previous.get("result_sample") or []
    columns = previous.get("result_columns") or (list(sample[0].keys()) if sample else [])
    label_col = _label_column(columns, sample)
    if not label_col:
        return None, []
    values = [str(row.get(label_col)) for row in sample if isinstance(row, dict) and row.get(label_col) is not None]
    return label_col, values


def _is_reset(text: str) -> bool:
    return _contains_any(text, _RESET_CUES)


def _is_meta(text: str) -> bool:
    return _contains_any(text, _META_CUES)


def _detect_clarification_answer(text: str, question: str, previous: dict[str, Any]) -> dict[str, Any] | None:
    clar = previous.get("clarification") or {}
    if not clar.get("needs_clarification"):
        return None
    suggestions = clar.get("suggestions") or []
    if not suggestions or len(question.split()) > 8:
        return None
    tokens = {tok for tok in re.findall(r"[a-zà-ỹ0-9_]+", text) if len(tok) > 1}
    for suggestion in suggestions:
        haystack = f"{suggestion.get('label','')} {suggestion.get('question','')}".lower()
        if any(tok in haystack for tok in tokens):
            return {
                "op": "clarification_answer",
                "resolved_question": suggestion.get("question") or question,
                "previous_question": previous.get("question"),
                "previous_turn_id": previous.get("turn_id"),
            }
    return None


def _detect_entity_ref(text: str, previous: dict[str, Any], prev_spec: dict[str, Any]) -> dict[str, Any] | None:
    dimension = prev_spec.get("dimension")
    if not dimension:
        return None
    _label_col, values = _label_values(previous)
    if not values:
        return None

    patch: dict[str, Any] | None = None
    note = ""
    if _contains_any(text, ("đầu tiên", "dau tien", "top 1", "top1", "cái đầu", "cai dau", "first", "#1", "số 1", "so 1")):
        patch = {"add_filters": [{"field": dimension, "operator": "in", "values": [values[0]]}]}
        note = f"Referenced the top entity from the previous result: {values[0]}."
    elif _contains_any(text, ("cuối cùng", "cuoi cung", "cuối", "cuoi", "last", "thấp nhất trong số")):
        patch = {"add_filters": [{"field": dimension, "operator": "in", "values": [values[-1]]}]}
        note = f"Referenced the last entity from the previous result: {values[-1]}."
    elif _contains_any(text, ("còn lại", "con lai", "remaining", "the rest", "ngoài ra", "ngoai ra", "những cái khác", "nhung cai khac")):
        patch = {"add_filters": [{"field": dimension, "operator": "not_in", "values": [values[0]]}]}
        note = f"Excluded the previously-leading entity: {values[0]}."
    elif extract_exclusion_filters(text) or extract_inclusion_filters(text):
        # An explicit exclude/include cue ("bỏ qua unknown", "chỉ apple") must keep
        # its operator; let the refine path build the filter, not a named IN here.
        return None
    else:
        # Named reference: a value from the prior result mentioned by name.
        lowered = {v.lower(): v for v in values}
        matched = [original for low, original in lowered.items() if low and low in text]
        if matched:
            patch = {"add_filters": [{"field": dimension, "operator": "in", "values": matched}]}
            note = f"Focused on entities from the previous result: {', '.join(matched)}."
    if not patch:
        return None
    merged = merge_spec(prev_spec, patch)
    return {
        "op": "entity_ref",
        "context_used": True,
        "merged_spec": merged,
        "previous_question": previous.get("question"),
        "previous_turn_id": previous.get("turn_id"),
        "context_notes": [note],
    }


def _build_patch(text: str, prev_spec: dict[str, Any]) -> dict[str, Any]:
    signals = analyze_message(text, dimension_hint=prev_spec.get("dimension"))
    patch: dict[str, Any] = {"set": {}, "add_filters": [], "remove_filter_fields": [], "add_metrics": []}

    patch["add_filters"].extend(signals["exclusion_filters"])
    patch["add_filters"].extend(signals["inclusion_filters"])
    patch["add_filters"].extend(signals["threshold_filters"])

    if _contains_any(text, _REMOVE_FILTER_CUES):
        field = signals["dimension"]
        if field:
            patch["remove_filter_fields"].append(field)
        else:
            patch["remove_filter_fields"].extend(
                {f.get("field") for f in prev_spec.get("filters") or [] if f.get("field")}
            )

    if signals["time_range"]:
        patch["set"]["time_range"] = signals["time_range"]
    if signals["time_grain"]:
        patch["set"]["time_grain"] = signals["time_grain"]

    metric = signals["metric"]
    if metric:
        if _contains_any(text, _METRIC_ADD_CUES):
            patch["add_metrics"].append(metric)
        else:
            patch["set"]["metric"] = metric

    dim = signals["dimension"]
    if dim and dim != prev_spec.get("dimension"):
        if _contains_any(text, _DIM_SWITCH_CUES) or len(text.split()) <= 5:
            patch["set"]["dimension"] = dim

    if _contains_any(text, _SORT_FLIP_CUES):
        current = prev_spec.get("sort_direction") or "desc"
        patch["set"]["sort_direction"] = "asc" if current == "desc" else "desc"
    elif signals["sort_direction"]:
        patch["set"]["sort_direction"] = signals["sort_direction"]

    if signals["limit"]:
        patch["set"]["limit"] = signals["limit"]

    return patch


def _patch_is_empty(patch: dict[str, Any]) -> bool:
    return not (
        patch.get("set")
        or patch.get("add_filters")
        or patch.get("remove_filter_fields")
        or patch.get("add_metrics")
    )


def _is_standalone_new(question: str, text: str) -> bool:
    nlu = parse_nlu(question)
    intent = nlu.get("intent")
    if intent in ("metadata_tables", "metadata_columns"):
        return True
    if intent in (None, "unsupported"):
        return False
    interrogative = _contains_any(text, _INTERROGATIVES)
    has_dim = nlu.get("dimension") is not None
    has_metric = nlu.get("metric") is not None or bool((nlu.get("extracted_entities") or {}).get("metrics"))
    return bool(interrogative and has_dim and has_metric)


def classify_followup(question: str, recent_context: list[dict[str, Any]]) -> dict[str, Any]:
    """Classify a turn into an operation on the session's Active Query Spec."""
    text = question.strip().lower()
    previous = _successful_previous(recent_context)
    if not previous:
        return _no_context_result()

    prev_spec_raw = previous.get("effective_spec")
    prev_spec = canonical_spec(prev_spec_raw) if prev_spec_raw else None

    if _is_reset(text):
        return {"op": "reset", "context_used": False, "merged_spec": None, "context_notes": ["User reset the conversation context."]}

    # Presentation (chart reuse / explain) reuses the tested resolver.
    presentation = resolve_followup(question, recent_context)
    if presentation.get("action") in ("reuse_chart", "explain_sql"):
        return {"op": "presentation", "context_used": True, "followup": presentation, "merged_spec": None}

    clarification = _detect_clarification_answer(text, question, previous)
    if clarification:
        return clarification

    if prev_spec and prev_spec.get("intent") in DIMENSIONAL_INTENTS:
        entity = _detect_entity_ref(text, previous, prev_spec)
        if entity:
            return entity

        if not _is_standalone_new(question, text):
            patch = _build_patch(text, prev_spec)
            if not _patch_is_empty(patch):
                merged = merge_spec(prev_spec, patch)
                return {
                    "op": "refine",
                    "context_used": True,
                    "merged_spec": merged,
                    "previous_question": previous.get("question"),
                    "previous_turn_id": previous.get("turn_id"),
                    "previous_sql": previous.get("generated_sql") or "",
                    "context_notes": [f"Refined the previous question: {render_spec_question(merged)}."],
                }

    if _is_meta(text):
        return {"op": "meta", "context_used": True, "merged_spec": None, "context_notes": ["Answered a meta/conversational message."]}

    # Elliptical but no detectable structured delta -> let the LLM try (Phase 4).
    if prev_spec and not _is_standalone_new(question, text):
        return {
            "op": "ambiguous",
            "context_used": False,
            "merged_spec": None,
            "context_notes": [],
            "prev_spec": prev_spec,
            "previous_question": previous.get("question"),
            "previous_turn_id": previous.get("turn_id"),
        }

    return _no_context_result()


# ---------------------------------------------------------------------------
# General LLM-based follow-up rewriting
# ---------------------------------------------------------------------------
#
# The deterministic resolver above only covers a handful of canned phrasings.
# Real conversations refine the previous question in open-ended ways ("bỏ qua
# nhãn hàng unknown", "chỉ trong năm 2021", "thêm cột doanh thu", "đổi sang
# category"). For those we let the LLM rewrite the elliptical message into a
# standalone analytics question, grounded in the recent turns (including the
# SQL that answered them). The rewritten question then flows through the normal
# NLU -> guard -> execute pipeline, so the read-only guardrails are unchanged.

_MAX_REWRITE_TURNS = 4

_REWRITE_SYSTEM_PROMPT = """You rewrite a user's latest chat message into ONE \
self-contained data-analytics question for an e-commerce analytics agent.

You are given the recent conversation (each prior turn shows the user's question \
and the SQL that answered it) and the user's NEW message.

Decide whether the NEW message is a follow-up that only makes sense given the \
previous turns - e.g. it refines, filters, excludes, reorders, changes the time \
range, swaps the metric or dimension, or is a bare fragment / pronoun. Common \
follow-up cues (Vietnamese/English): "bỏ qua", "loại trừ", "không tính", "ngoại \
trừ", "chỉ", "thêm", "còn ... thì sao", "đổi", "thay vào đó", "trong năm ...".

Rules:
- If it IS a follow-up: produce "standalone_question" that fully restates the \
intent by merging the new message with the relevant previous turn, understandable \
with NO prior context. Keep the user's language. Preserve the previous metric, \
dimension and time filter unless the new message overrides them. Do NOT invent \
data values.
- If it is NOT a follow-up (already self-contained, or a brand-new topic): set \
is_followup=false and echo the new message unchanged.
- Never write SQL. Only a natural-language question.
- Return ONE JSON object only, no text outside it.

Output JSON:
{"is_followup": true|false, "standalone_question": "...", "reason": "short reason"}"""


def _extract_json_object(content: str) -> str:
    content = content.strip()
    fence = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", content, flags=re.IGNORECASE | re.DOTALL)
    if fence:
        content = fence.group(1).strip()
    if not content.startswith("{"):
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            content = content[start : end + 1]
    return content


def _rewrite_context_block(recent_context: list[dict[str, Any]]) -> str:
    """Render the recent turns (oldest-first) as compact, promptable text."""
    turns = [turn for turn in (recent_context or []) if isinstance(turn, dict)]
    turns = list(reversed(turns[:_MAX_REWRITE_TURNS]))  # store is newest-first
    lines: list[str] = []
    for index, turn in enumerate(turns, start=1):
        question = str(turn.get("question") or "").strip()
        if not question:
            continue
        lines.append(f"Turn {index} question: {question}")
        sql = " ".join((turn.get("generated_sql") or "").split())
        if sql:
            lines.append(f"Turn {index} SQL: {sql}")
    return "\n".join(lines)


_PATCH_SYSTEM_PROMPT = """You convert a user's follow-up message into a typed JSON \
patch on their current analytics query. You are given the CURRENT spec (JSON) and \
the user's NEW message.

Allowed spec fields you may set: dimension (brand|category|product|event_type|event_date), \
metric (revenue|total_views|total_carts|total_purchases|total_events|count), \
time_grain (day|month|hour), sort_direction (asc|desc), limit (integer 1-100).
Filters use {"field": <dimension>, "operator": "in"|"not_in", "values": [..]}.

Return ONE JSON object only:
{"is_followup": true|false,
 "patch": {"set": {..}, "add_filters": [..], "remove_filter_fields": [..], "add_metrics": [..]}}

Rules:
- is_followup=false if the message is a brand-new question unrelated to the spec.
- Only include fields the user actually asked to change. Do NOT invent values.
- Never output SQL."""

_ALLOWED_SET_FIELDS = {"dimension", "metric", "time_grain", "sort_direction", "limit"}
_ALLOWED_DIMENSIONS = {"brand", "category", "product", "event_type", "event_date"}
_ALLOWED_METRICS = {"revenue", "total_views", "total_carts", "total_purchases", "total_events", "count"}
_ALLOWED_FILTER_OPS = {"in", "not_in"}


def _sanitize_patch(raw: Any) -> dict[str, Any] | None:
    """Keep only well-formed, allow-listed patch entries (defence in depth; the
    SQL builder also drops columns that do not exist on the chosen table)."""
    if not isinstance(raw, dict):
        return None
    patch: dict[str, Any] = {"set": {}, "add_filters": [], "remove_filter_fields": [], "add_metrics": []}
    set_in = raw.get("set") or {}
    if isinstance(set_in, dict):
        for key, value in set_in.items():
            if key not in _ALLOWED_SET_FIELDS:
                continue
            if key == "dimension" and value not in _ALLOWED_DIMENSIONS:
                continue
            if key == "metric" and value not in _ALLOWED_METRICS:
                continue
            if key == "sort_direction" and value not in ("asc", "desc"):
                continue
            if key == "limit":
                try:
                    value = max(1, min(int(value), 100))
                except (TypeError, ValueError):
                    continue
            patch["set"][key] = value
    for spec_filter in raw.get("add_filters") or []:
        if not isinstance(spec_filter, dict):
            continue
        operator = (spec_filter.get("operator") or "").lower()
        values = [v for v in (spec_filter.get("values") or []) if str(v).strip()]
        if spec_filter.get("field") and operator in _ALLOWED_FILTER_OPS and values:
            patch["add_filters"].append({"field": spec_filter["field"], "operator": operator, "values": values})
    patch["remove_filter_fields"] = [f for f in (raw.get("remove_filter_fields") or []) if isinstance(f, str)]
    patch["add_metrics"] = [m for m in (raw.get("add_metrics") or []) if m in _ALLOWED_METRICS]
    if not (patch["set"] or patch["add_filters"] or patch["remove_filter_fields"] or patch["add_metrics"]):
        return None
    return patch


def llm_extract_patch(question: str, recent_context: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Ask the LLM for a typed patch on the current spec (Phase 4 fallback).

    Returns a sanitized patch dict or None (no key, no spec, not a follow-up, or
    nothing usable). Never raises.
    """
    if not llm.llm_available():
        return None
    previous = _successful_previous(recent_context)
    prev_spec = canonical_spec(previous.get("effective_spec")) if previous and previous.get("effective_spec") else None
    if not prev_spec:
        return None
    spec_view = {
        "dimension": prev_spec.get("dimension"),
        "metric": prev_spec.get("metric"),
        "time_range": prev_spec.get("time_range"),
        "filters": prev_spec.get("filters"),
        "sort_direction": prev_spec.get("sort_direction"),
        "limit": prev_spec.get("limit"),
    }
    messages = [
        {"role": "system", "content": _PATCH_SYSTEM_PROMPT},
        {"role": "user", "content": f"CURRENT spec: {json.dumps(spec_view, ensure_ascii=False)}\n\nNEW message: {question}"},
    ]
    try:
        content = llm.chat_completion(
            messages,
            temperature=0,
            max_tokens=220,
            response_format={"type": "json_object"},
            timeout=float(os.getenv("GROQ_TIMEOUT_SECONDS", "12")),
        )
        data = json.loads(_extract_json_object(content))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(data, dict) or not data.get("is_followup"):
        return None
    return _sanitize_patch(data.get("patch"))


def llm_rewrite_followup(
    question: str,
    recent_context: list[dict[str, Any]],
) -> dict[str, Any]:
    """Rewrite an elliptical follow-up into a standalone question via the LLM.

    Returns {"is_followup": bool, "standalone_question": str, "reason": str,
    "previous_sql": str, "previous_turn_id": int | None,
    "previous_question": str | None}. Never raises; when no Groq key is set, the
    context is empty, or the call fails, it returns is_followup=False and echoes
    the original question so the caller treats it as a fresh question.
    """
    result: dict[str, Any] = {
        "is_followup": False,
        "standalone_question": question,
        "reason": "",
        "previous_sql": "",
        "previous_turn_id": None,
        "previous_question": None,
    }
    if not recent_context or not llm.llm_available():
        return result

    context_block = _rewrite_context_block(recent_context)
    if not context_block:
        return result

    messages = [
        {"role": "system", "content": _REWRITE_SYSTEM_PROMPT},
        {"role": "user", "content": f"Recent conversation:\n{context_block}\n\nNEW message: {question}"},
    ]
    try:
        content = llm.chat_completion(
            messages,
            temperature=0,
            max_tokens=220,
            response_format={"type": "json_object"},
            timeout=float(os.getenv("GROQ_TIMEOUT_SECONDS", "12")),
        )
        data = json.loads(_extract_json_object(content))
        if not isinstance(data, dict):
            return result
        standalone = str(data.get("standalone_question") or "").strip() or question
        is_followup = bool(data.get("is_followup")) and (
            standalone.strip().lower() != question.strip().lower()
        )
        reason = str(data.get("reason") or "").strip()
    except Exception:  # noqa: BLE001 - any LLM/parse failure falls back cleanly
        return result

    previous = _successful_previous(recent_context)
    if previous:
        result["previous_sql"] = previous.get("generated_sql") or ""
        result["previous_turn_id"] = previous.get("turn_id")
        result["previous_question"] = previous.get("question")
    result["is_followup"] = is_followup
    result["standalone_question"] = standalone
    result["reason"] = reason or "Resolved a contextual follow-up into a standalone question via LLM rewrite."
    return result
