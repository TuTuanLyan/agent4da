import json
import os
from datetime import datetime, timezone
from uuid import uuid4

from services.trino_service import execute_query_to_dicts, get_trino_connection


CONTEXT_CATALOG = os.getenv("AGENT_CONTEXT_CATALOG", "postgres")
CONTEXT_SCHEMA = os.getenv("AGENT_CONTEXT_SCHEMA", "app_context")
DEFAULT_USER_ID = os.getenv("AGENT_DEFAULT_USER_ID", "default")
MAX_CONTEXT_TURNS = int(os.getenv("AGENT_MAX_CONTEXT_TURNS", "6"))

_ENSURED = False


class ContextStoreError(RuntimeError):
    pass


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def table_name(name):
    return f"{CONTEXT_CATALOG}.{CONTEXT_SCHEMA}.{name}"


def sql_literal(value):
    if value is None:
        return "NULL"

    if value == "current_timestamp":
        return "CAST(current_timestamp AS timestamp(3))"

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (int, float)):
        return str(value)

    text = str(value).replace("'", "''")
    return f"'{text}'"


def json_text(value):
    return json.dumps(value or {}, ensure_ascii=False, default=str)


def parse_json_text(value, default=None):
    if not value:
        return default if default is not None else {}

    try:
        return json.loads(value)
    except Exception:
        return default if default is not None else {}


def get_context_connection():
    connection = get_trino_connection(catalog=CONTEXT_CATALOG, schema="public")
    if connection is None:
        raise ContextStoreError(
            "Cannot connect to Trino postgres catalog for app_context."
        )
    return connection


def execute_context_sql(sql, raise_on_error=True):
    return execute_query_to_dicts(
        get_context_connection(),
        sql,
        raise_on_error=raise_on_error,
    )


def ensure_context_tables(force=False):
    global _ENSURED

    if _ENSURED and not force:
        return {
            "available": True,
            "catalog": CONTEXT_CATALOG,
            "schema": CONTEXT_SCHEMA,
        }

    ddl_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {CONTEXT_CATALOG}.{CONTEXT_SCHEMA}",
        f"""
        CREATE TABLE IF NOT EXISTS {table_name("users")} (
            user_id VARCHAR,
            username VARCHAR,
            created_at TIMESTAMP(3)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {table_name("chat_sessions")} (
            session_id VARCHAR,
            user_id VARCHAR,
            session_name VARCHAR,
            started_at TIMESTAMP(3),
            created_at TIMESTAMP(3)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {table_name("chat_messages")} (
            message_id VARCHAR,
            session_id VARCHAR,
            role VARCHAR,
            content VARCHAR,
            payload_json VARCHAR,
            created_at TIMESTAMP(3)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {table_name("session_contexts")} (
            context_id VARCHAR,
            session_id VARCHAR,
            compact_context_json VARCHAR,
            created_at TIMESTAMP(3)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {table_name("langgraph_checkpoints")} (
            checkpoint_id VARCHAR,
            thread_id VARCHAR,
            state_data_json VARCHAR,
            created_at TIMESTAMP(3)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {table_name("ai_query_logs")} (
            log_id VARCHAR,
            request_id VARCHAR,
            session_id VARCHAR,
            user_question VARCHAR,
            generated_sql VARCHAR,
            execution_status VARCHAR,
            execution_time_ms BIGINT,
            row_count BIGINT,
            error_message VARCHAR,
            created_at TIMESTAMP(3)
        )
        """,
    ]

    try:
        for statement in ddl_statements:
            execute_context_sql(statement)
    except Exception as exc:
        _ENSURED = False
        raise ContextStoreError(
            f"Cannot initialize app_context tables: {type(exc).__name__}: {exc}"
        ) from exc

    _ENSURED = True
    return {
        "available": True,
        "catalog": CONTEXT_CATALOG,
        "schema": CONTEXT_SCHEMA,
    }


def context_health():
    try:
        return ensure_context_tables()
    except Exception as exc:
        return {
            "available": False,
            "catalog": CONTEXT_CATALOG,
            "schema": CONTEXT_SCHEMA,
            "error": f"{type(exc).__name__}: {exc}",
        }


def insert_row(table, values):
    columns = ", ".join(values.keys())
    literals = ", ".join(sql_literal(value) for value in values.values())
    execute_context_sql(
        f"INSERT INTO {table_name(table)} ({columns}) VALUES ({literals})"
    )


def create_or_touch_user(user_id=None, username=None):
    ensure_context_tables()
    user_id = user_id or DEFAULT_USER_ID
    rows = execute_context_sql(
        f"""
        SELECT user_id
        FROM {table_name("users")}
        WHERE user_id = {sql_literal(user_id)}
        LIMIT 1
        """
    )
    if not rows:
        insert_row(
            "users",
            {
                "user_id": user_id,
                "username": username or user_id,
                "created_at": "current_timestamp",
            },
        )
    return {"user_id": user_id, "username": username or user_id}


def create_session(user_id=None, session_name=None, session_id=None):
    ensure_context_tables()
    user_id = user_id or DEFAULT_USER_ID
    session_id = session_id or uuid4().hex
    create_or_touch_user(user_id)
    insert_row(
        "chat_sessions",
        {
            "session_id": session_id,
            "user_id": user_id,
            "session_name": session_name or "Agent chat",
            "started_at": "current_timestamp",
            "created_at": "current_timestamp",
        },
    )
    return {
        "session_id": session_id,
        "user_id": user_id,
        "session_name": session_name or "Agent chat",
    }


def get_session(session_id):
    ensure_context_tables()
    rows = execute_context_sql(
        f"""
        SELECT session_id, user_id, session_name, started_at, created_at
        FROM {table_name("chat_sessions")}
        WHERE session_id = {sql_literal(session_id)}
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    return rows[0] if rows else None


def get_or_create_session(session_id=None, user_id=None, session_name=None):
    if session_id:
        existing = get_session(session_id)
        if existing:
            return existing
        return create_session(
            user_id=user_id,
            session_name=session_name,
            session_id=session_id,
        )

    return create_session(user_id=user_id, session_name=session_name)


def save_message(session_id, role, content, payload=None):
    ensure_context_tables()
    insert_row(
        "chat_messages",
        {
            "message_id": uuid4().hex,
            "session_id": session_id,
            "role": role,
            "content": content or "",
            "payload_json": json_text(payload),
            "created_at": "current_timestamp",
        },
    )


def get_messages(session_id, limit=50):
    ensure_context_tables()
    safe_limit = max(1, min(int(limit or 50), 100))
    rows = execute_context_sql(
        f"""
        SELECT message_id, session_id, role, content, payload_json, created_at
        FROM {table_name("chat_messages")}
        WHERE session_id = {sql_literal(session_id)}
        ORDER BY created_at DESC
        LIMIT {safe_limit}
        """
    )
    rows = list(reversed(rows))
    for row in rows:
        row["payload"] = parse_json_text(row.get("payload_json"), default={})
        row.pop("payload_json", None)
    return rows


def get_latest_context(session_id):
    ensure_context_tables()
    rows = execute_context_sql(
        f"""
        SELECT compact_context_json, created_at
        FROM {table_name("session_contexts")}
        WHERE session_id = {sql_literal(session_id)}
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    if not rows:
        return empty_context()

    context = parse_json_text(rows[0].get("compact_context_json"), default=empty_context())
    context["loaded_from"] = {
        "catalog": CONTEXT_CATALOG,
        "schema": CONTEXT_SCHEMA,
        "created_at": rows[0].get("created_at"),
    }
    return context


def save_context_snapshot(session_id, compact_context):
    ensure_context_tables()
    insert_row(
        "session_contexts",
        {
            "context_id": uuid4().hex,
            "session_id": session_id,
            "compact_context_json": json_text(compact_context),
            "created_at": "current_timestamp",
        },
    )


def save_checkpoint(session_id, state_data):
    ensure_context_tables()
    insert_row(
        "langgraph_checkpoints",
        {
            "checkpoint_id": uuid4().hex,
            "thread_id": session_id,
            "state_data_json": json_text(state_data),
            "created_at": "current_timestamp",
        },
    )


def save_query_log(
    request_id,
    session_id,
    question,
    sql,
    status,
    execution_time_ms,
    row_count,
    error_message=None,
):
    ensure_context_tables()
    insert_row(
        "ai_query_logs",
        {
            "log_id": uuid4().hex,
            "request_id": request_id,
            "session_id": session_id,
            "user_question": question or "",
            "generated_sql": sql or "",
            "execution_status": status or "unknown",
            "execution_time_ms": int(execution_time_ms or 0),
            "row_count": int(row_count or 0),
            "error_message": error_message or "",
            "created_at": "current_timestamp",
        },
    )


def get_query_logs(session_id, limit=50):
    ensure_context_tables()
    safe_limit = max(1, min(int(limit or 50), 100))
    return execute_context_sql(
        f"""
        SELECT request_id, session_id, user_question, generated_sql,
               execution_status, execution_time_ms, row_count,
               error_message, created_at
        FROM {table_name("ai_query_logs")}
        WHERE session_id = {sql_literal(session_id)}
        ORDER BY created_at DESC
        LIMIT {safe_limit}
        """
    )


def empty_context():
    return {
        "conversation_summary": "",
        "turns": [],
        "last_question": "",
        "last_sql": "",
        "last_result_columns": [],
        "last_result_sample": [],
        "last_chart_suggestion": {},
        "last_answer_kind": "",
        "last_text_answer": "",
        "updated_at": None,
    }


def build_compact_context(previous_context, final_answer):
    previous_context = previous_context or empty_context()
    result = final_answer.get("result") or {}
    rows = result.get("rows") or []
    chart_suggestion = final_answer.get("chart_suggestion") or (
        (final_answer.get("visualization") or {}).get("chart_spec") or {}
    )

    turn = {
        "question": final_answer.get("question") or "",
        "answer_kind": final_answer.get("answer_kind") or "",
        "text_answer": final_answer.get("text_answer") or "",
        "sql": final_answer.get("sql") or "",
        "columns": result.get("columns") or [],
        "row_count": result.get("row_count") or 0,
        "sample_rows": rows[:3],
        "chart_suggestion": {
            "chart_type": chart_suggestion.get("chart_type") or chart_suggestion.get("type"),
            "x": chart_suggestion.get("x"),
            "y": chart_suggestion.get("y"),
        },
    }

    turns = list(previous_context.get("turns") or [])
    turns.append(turn)
    turns = turns[-MAX_CONTEXT_TURNS:]

    summary_parts = []
    for item in turns[-3:]:
        question = item.get("question") or ""
        sql = item.get("sql") or ""
        columns = ", ".join(item.get("columns") or [])
        if question:
            summary_parts.append(f"User asked: {question}")
        if columns:
            summary_parts.append(f"Returned columns: {columns}")
        if sql:
            summary_parts.append(f"Executed SQL: {sql}")

    return {
        "conversation_summary": "\n".join(summary_parts),
        "turns": turns,
        "last_question": turn["question"],
        "last_sql": turn["sql"],
        "last_result_columns": turn["columns"],
        "last_result_sample": turn["sample_rows"],
        "last_chart_suggestion": turn["chart_suggestion"],
        "last_answer_kind": turn["answer_kind"],
        "last_text_answer": turn["text_answer"],
        "updated_at": now_iso(),
    }
