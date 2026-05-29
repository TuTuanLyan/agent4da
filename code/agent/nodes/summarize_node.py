"""Summarize node: turns SQL + result rows into a short natural-language answer.

Toggle: AGENT_SUMMARIZE env (default true). Per-request: state["summarize"]=False.

Output is best-effort JSON:
    {"summary": "...", "key_numbers": [{"label": "...", "value": "..."}]}

Falls back to plain text if JSON parsing fails. Never raises - on any
error the state is left with `summary=None`, `key_numbers=[]`.
"""

from __future__ import annotations

import json
import os
import re

from services.llm_service import get_llm_client


SYSTEM_PROMPT = """
You are an analyst summarizing a SQL query result.

You will receive:
- The user's original question (Vietnamese or English).
- The Trino SQL that was executed.
- The first rows of the result table.

Produce a short summary the user can read in 10 seconds.

Output rules:
- Reply with one JSON object only. No markdown.
- Shape: {"summary": "<at most 4 sentences in the user's language>",
          "key_numbers": [{"label": "<short label>", "value": "<formatted value>"}]}
- Include at most 4 key_numbers. Each label/value must come from the result rows.
- If the result is empty, set summary to a one-sentence "no rows" message and
  key_numbers to an empty array.
- Do not invent numbers that are not in the rows.
""".strip()


def _truthy(value):
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _should_summarize(state):
    flag = state.get("summarize")
    if flag is False:
        return False
    if flag is True:
        return True
    return _truthy(os.getenv("AGENT_SUMMARIZE", "true"))


def _model_name():
    return os.getenv("AGENT_SUMMARIZE_MODEL") or os.getenv("AGENT_MODEL") or "llama-3.3-70b-versatile"


def _trim_rows(rows, limit=20):
    if not rows:
        return []
    return rows[:limit]


def _build_prompt(state):
    payload = {
        "user_question": state.get("user_question") or "",
        "sql": state.get("generated_sql") or "",
        "rows": _trim_rows(state.get("query_result") or [], limit=20),
    }
    return (
        SYSTEM_PROMPT
        + "\n\nINPUT JSON:\n"
        + json.dumps(payload, ensure_ascii=False, default=str)
    )


def _parse_json_lenient(text):
    if not text:
        return None
    # Strip code fences if the model still adds them.
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Last-resort: try to find the first {...} block.
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def summarize_node(state):
    # Skip cleanly when there is an upstream error or summarize is off.
    if state.get("error"):
        return {"summary": None, "key_numbers": []}
    if not _should_summarize(state):
        return {"summary": None, "key_numbers": []}

    try:
        client = get_llm_client()
    except Exception:
        # Groq not configured - leave the answer tab empty.
        return {"summary": None, "key_numbers": []}

    try:
        response = client.chat.completions.create(
            model=_model_name(),
            messages=[{"role": "user", "content": _build_prompt(state)}],
            temperature=0,
        )
        text = response.choices[0].message.content or ""
    except Exception:
        return {"summary": None, "key_numbers": []}

    parsed = _parse_json_lenient(text)
    if isinstance(parsed, dict):
        summary = parsed.get("summary") or ""
        key_numbers = parsed.get("key_numbers") or []
        if not isinstance(key_numbers, list):
            key_numbers = []
        return {
            "summary": str(summary).strip() or None,
            "key_numbers": key_numbers[:4],
        }

    # Plain-text fallback.
    return {"summary": text.strip()[:600] or None, "key_numbers": []}
