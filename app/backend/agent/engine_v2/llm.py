"""OpenAI-compatible Groq client wrapper.

The backend image ships the `openai` SDK, not the `groq` SDK, so every LLM call
in the v2 engine goes through Groq's OpenAI-compatible endpoint. Imports are
lazy so the rest of the engine (NLU, guard, deterministic SQL, rule-based
insights) imports cleanly even when `openai` or a key is absent.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict, List, Optional

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
_PLACEHOLDER_KEYS = {"", "your_groq_api_key_here"}


def _api_key() -> Optional[str]:
    return os.getenv("GROQ_API_KEY")


def llm_available() -> bool:
    """True when a usable Groq key is configured."""
    key = (_api_key() or "").strip()
    return key not in _PLACEHOLDER_KEYS


def default_model() -> str:
    """Model used for text-to-SQL, correction, and the insight summary.

    Honours AGENT_MODEL / GROQ_MODEL overrides; defaults to a model that is
    on the backend whitelist and currently supported by Groq.
    """
    return (
        os.getenv("AGENT_MODEL")
        or os.getenv("GROQ_MODEL")
        or "llama-3.3-70b-versatile"
    )


@lru_cache(maxsize=1)
def _client():
    from openai import OpenAI

    return OpenAI(api_key=_api_key(), base_url=GROQ_BASE_URL)


def chat_completion(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    temperature: float = 0,
    max_tokens: int = 300,
    response_format: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> str:
    """Run a chat completion and return the message content (may be empty)."""
    client = _client()
    if timeout is not None:
        client = client.with_options(timeout=timeout)
    kwargs: Dict[str, Any] = {
        "model": model or default_model(),
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if response_format is not None:
        kwargs["response_format"] = response_format
    completion = client.chat.completions.create(**kwargs)
    return completion.choices[0].message.content or ""
