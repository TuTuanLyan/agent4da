"""Checkpoint snapshots for the v2 engine.

The langgraph Postgres saver is not installed in the backend image, so instead
of langgraph's own checkpoint store we persist a compact final-state snapshot to
`app.agent_checkpoint_snapshots` via SQLAlchemy. This satisfies the plan's
durability requirement (thread_id + checkpoint_id + state_data) without the
extra dependency. Failures here never break a run.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import structlog

log = structlog.get_logger("agent.engine_v2.checkpoint")


def new_checkpoint_id(thread_id: str) -> str:
    return f"{thread_id}:{uuid.uuid4().hex}"


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


_SNAPSHOT_KEYS = (
    "session_id",
    "user_id",
    "question",
    "effective_question",
    "resolved_question",
    "context_used",
    "previous_turn_id",
    "previous_question",
    "context_notes",
    "intent",
    "dimension",
    "metric",
    "analysis_type",
    "time_range",
    "time_grain",
    "applied_time_filter",
    "filters",
    "comparison_entities",
    "sort_direction",
    "extracted_entities",
    "nlu_confidence",
    "table_candidates",
    "metadata_used",
    "generated_sql",
    "original_sql",
    "used_tables",
    "row_count",
    "warnings",
    "validation_notes",
    "confidence",
    "insights",
    "insight_source",
    "llm_insight_used",
    "chart_type",
    "chart_data",
    "answer",
    "status",
    "error_message",
    "retry_attempted",
    "retry_success",
    "retry_count",
    "correction_reason",
    "correction_history",
)


def state_snapshot(state: dict[str, Any]) -> dict[str, Any]:
    snapshot = {key: _json_safe(state.get(key)) for key in _SNAPSHOT_KEYS}
    if isinstance(snapshot.get("chart_data"), list):
        snapshot["chart_data"] = snapshot["chart_data"][:20]
    return snapshot


def save_checkpoint_snapshot(thread_id: str, checkpoint_id: str | None, state: dict[str, Any]) -> None:
    """Best-effort write of the final state snapshot. Swallows all errors."""
    checkpoint_key = checkpoint_id or new_checkpoint_id(thread_id)
    try:
        from db.base import get_sessionmaker
        from db.models import AgentCheckpointSnapshot

        SessionLocal = get_sessionmaker()
        session = SessionLocal()
        try:
            session.add(
                AgentCheckpointSnapshot(
                    thread_id=thread_id,
                    checkpoint_id=checkpoint_key,
                    state_data=state_snapshot(state),
                )
            )
            session.commit()
        finally:
            session.close()
    except Exception as exc:  # noqa: BLE001 - snapshotting is never fatal
        log.warning("agent.engine_v2.checkpoint_failed", error=str(exc), thread_id=thread_id)


def latest_checkpoint_id(thread_id: str) -> str:
    # No langgraph saver to query; mint a monotonic-ish id for this turn.
    return f"{thread_id}:{int(time.time() * 1000)}"
