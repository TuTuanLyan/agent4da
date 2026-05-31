"""Groq-backed conversational assistant for the v2 engine.

The deterministic analytics pipeline (NLU -> metadata -> text-to-SQL -> guard ->
execute) only answers questions it can safely turn into read-only Trino SQL.
Everything it cannot classify is routed here so the agent can still answer
free-form / conversational prompts instead of returning a canned non-answer.

This module uses the same OpenAI-compatible Groq client and default model
(`llama-3.3-70b-versatile`) as the rest of the engine. It is grounded:

- It is told the data model it has "learned" (the semantic metadata overview).
- It is given the recent conversation turns so follow-ups make sense.
- It must NOT invent concrete data values; for real numbers it tells the user a
  query will be run and proposes the exact analytics question to ask.

Every failure path (no key, timeout, bad JSON) returns ``llm_used=False`` so the
caller can fall back to the existing deterministic clarification message.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from . import llm
from .config import GOLD_PREFIX
from .metadata import get_semantic_overview

# Keep the prompt compact: only the most recent turns and a column sample.
_MAX_CONTEXT_TURNS = 5
_MAX_COLUMNS_PER_TABLE = 12
_MAX_FOLLOW_UPS = 4

_SYSTEM_PROMPT = """Bạn là Agent4DA, trợ lý phân tích dữ liệu AI cho một data lakehouse \
thương mại điện tử (lớp Gold). Bạn đã học mô hình dữ liệu được mô tả bên dưới và \
trả lời người dùng dựa trên đó cùng với ngữ cảnh hội thoại.

Bạn CÓ THỂ:
- Giải thích các bảng, cột, chỉ số và ý nghĩa nghiệp vụ có trong mô hình dữ liệu.
- Làm rõ khái niệm, so sánh bảng/cột, và giúp người dùng diễn đạt câu hỏi phân tích.
- Đề xuất các bước phân tích tiếp theo dựa trên cuộc trò chuyện hiện tại.

Bạn KHÔNG ĐƯỢC:
- Bịa ra số liệu cụ thể. Ở chế độ trò chuyện này bạn không đọc giá trị dữ liệu thật;
  nếu người dùng cần con số thật, hãy nói rằng bạn sẽ chạy một truy vấn và đề xuất \
chính xác câu hỏi phân tích nên hỏi.
- Trả lời các yêu cầu ngoài phạm vi dữ liệu (thời tiết, tin tức, crypto, kiến thức \
chung...) bằng thông tin tự bịa. Hãy nói thẳng là việc đó nằm ngoài dữ liệu hiện có \
và hướng người dùng về điều bạn phân tích được.

Quy tắc trả lời:
- Trả lời bằng đúng ngôn ngữ của người dùng (tiếng Việt hoặc tiếng Anh).
- Ngắn gọn, rõ ràng, hữu ích. Không markdown rườm rà.
- Chỉ trả về một JSON object, không kèm văn bản ngoài JSON.

Định dạng JSON output:
{
  "answer": "Câu trả lời chính, ngắn gọn.",
  "follow_up_questions": [
    "Câu hỏi phân tích cụ thể người dùng có thể hỏi tiếp",
    "..."
  ]
}"""


def _overview_lines(overview: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for table in overview.get("tables", []):
        name = table.get("table_name") or ""
        header = f"- {GOLD_PREFIX}.{name}"
        display = table.get("display_name")
        if display:
            header += f" ({display})"
        lines.append(header)
        if table.get("purpose"):
            lines.append(f"    Mục đích: {table['purpose']}")
        if table.get("grain"):
            lines.append(f"    Hạt (grain): {table['grain']}")
        if table.get("use_for"):
            lines.append(f"    Dùng cho: {table['use_for']}")
        columns = table.get("columns") or []
        if columns:
            shown = columns[:_MAX_COLUMNS_PER_TABLE]
            parts = []
            for column in shown:
                label = column.get("name", "")
                meaning = column.get("meaning")
                parts.append(f"{label} ({meaning})" if meaning else label)
            suffix = ", ..." if len(columns) > len(shown) else ""
            lines.append("    Cột: " + ", ".join(parts) + suffix)
    return lines


def build_overview_text(overview: dict[str, Any] | None = None) -> str:
    """Render the learned data model as compact, promptable text."""
    overview = overview or get_semantic_overview()
    lines = _overview_lines(overview)
    if not lines:
        return "Hệ thống có dữ liệu Gold thương mại điện tử nhưng chưa tải được mô tả chi tiết."
    return "Mô hình dữ liệu Gold hiện có:\n" + "\n".join(lines)


def _context_messages(recent_context: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Turn stored turns (newest-first) into oldest-first chat history."""
    turns = [turn for turn in (recent_context or []) if isinstance(turn, dict)]
    turns = list(reversed(turns[:_MAX_CONTEXT_TURNS]))
    messages: list[dict[str, str]] = []
    for turn in turns:
        question = str(turn.get("question") or "").strip()
        answer = str(turn.get("answer") or "").strip()
        if question:
            messages.append({"role": "user", "content": question})
        if answer:
            messages.append({"role": "assistant", "content": answer[:600]})
    return messages


def _clean_json(content: str) -> str:
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


def _parse(content: str) -> dict[str, Any]:
    data = json.loads(_clean_json(content))
    if not isinstance(data, dict):
        raise ValueError("Conversational response is not a JSON object")
    answer = str(data.get("answer") or "").strip()
    if not answer:
        raise ValueError("Conversational answer is empty")
    raw_follow_ups = data.get("follow_up_questions") or []
    if not isinstance(raw_follow_ups, list):
        raw_follow_ups = []
    follow_ups = [str(item).strip() for item in raw_follow_ups if str(item).strip()][:_MAX_FOLLOW_UPS]
    return {"answer": answer, "follow_ups": follow_ups}


def answer_conversational(
    question: str,
    recent_context: list[dict[str, Any]] | None = None,
    *,
    overview_text: str | None = None,
) -> dict[str, Any]:
    """Answer a free-form prompt grounded in metadata + chat context.

    Returns {"answer": str, "follow_ups": [str], "llm_used": bool,
    "error": str | None}. Never raises.
    """
    if not llm.llm_available():
        return {
            "answer": "",
            "follow_ups": [],
            "llm_used": False,
            "error": "GROQ_API_KEY is not configured.",
        }

    if overview_text is None:
        try:
            overview_text = build_overview_text()
        except Exception as exc:  # noqa: BLE001
            return {"answer": "", "follow_ups": [], "llm_used": False, "error": str(exc)}

    messages: list[dict[str, str]] = [
        {"role": "system", "content": f"{_SYSTEM_PROMPT}\n\n{overview_text}"}
    ]
    messages.extend(_context_messages(recent_context or []))
    messages.append({"role": "user", "content": question})

    try:
        content = llm.chat_completion(
            messages,
            temperature=0.2,
            max_tokens=500,
            response_format={"type": "json_object"},
            timeout=float(os.getenv("GROQ_TIMEOUT_SECONDS", "12")),
        )
        parsed = _parse(content)
        return {
            "answer": parsed["answer"],
            "follow_ups": parsed["follow_ups"],
            "llm_used": True,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 - any LLM failure falls back cleanly
        return {"answer": "", "follow_ups": [], "llm_used": False, "error": str(exc)}
