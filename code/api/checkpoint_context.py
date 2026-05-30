import os
from typing import Any

from dotenv import load_dotenv
from psycopg2.extras import Json

from db_context import _connect, _ensure_context_tables

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required for LangGraph checkpointing")
    return value


def _ensure_checkpoint_schema() -> None:
    with _connect() as conn:
        with conn.cursor() as cursor:
            _ensure_context_tables(cursor)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS app_context.langgraph_checkpoints (
                    id BIGSERIAL PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    checkpoint_id TEXT NOT NULL,
                    state_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS langgraph_checkpoints_thread_id_idx
                ON app_context.langgraph_checkpoints (thread_id)
                """
            )


def _postgres_conninfo() -> str:
    from psycopg.conninfo import make_conninfo

    return make_conninfo(
        "",
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "agent4da"),
        user=os.getenv("POSTGRES_USER", "bigdata"),
        password=_require_env("POSTGRES_PASSWORD"),
        sslmode="disable",
        options="-c search_path=app_context,public",
    )


def create_postgres_checkpointer() -> Any:
    from langgraph.checkpoint.postgres import PostgresSaver
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    _ensure_checkpoint_schema()
    pool_size = int(os.getenv("LANGGRAPH_CHECKPOINT_POOL_SIZE", "4"))
    pool = ConnectionPool(
        conninfo=_postgres_conninfo(),
        min_size=1,
        max_size=max(1, pool_size),
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
    )
    pool.wait(timeout=10)
    checkpointer = PostgresSaver(pool)
    checkpointer.setup()
    return checkpointer


def latest_checkpoint_id(checkpointer: Any, thread_id: str) -> str | None:
    try:
        latest = next(checkpointer.list({"configurable": {"thread_id": thread_id}}, limit=1), None)
    except Exception:
        return None

    if not latest:
        return None

    config = getattr(latest, "config", {}) or {}
    checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
    if checkpoint_id:
        return str(checkpoint_id)

    checkpoint = getattr(latest, "checkpoint", {}) or {}
    checkpoint_id = checkpoint.get("id")
    return str(checkpoint_id) if checkpoint_id else None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _state_snapshot(state: dict[str, Any]) -> dict[str, Any]:
    snapshot_keys = (
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
        "metadata_source",
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
        "chart",
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
    snapshot = {key: _json_safe(state.get(key)) for key in snapshot_keys}
    if isinstance(snapshot.get("chart_data"), list):
        snapshot["chart_data"] = snapshot["chart_data"][:20]
    return snapshot


def save_checkpoint_snapshot(thread_id: str, checkpoint_id: str | None, state: dict[str, Any]) -> None:
    _ensure_checkpoint_schema()
    checkpoint_key = checkpoint_id or f"{thread_id}:latest"
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app_context.langgraph_checkpoints (
                    thread_id,
                    checkpoint_id,
                    state_data
                )
                VALUES (%s, %s, %s)
                """,
                (thread_id, checkpoint_key, Json(_state_snapshot(state))),
            )
