import os

from dotenv import load_dotenv
import psycopg2

load_dotenv()


def _connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "agent4da"),
        user=os.getenv("POSTGRES_USER", "bigdata"),
        password=os.getenv("POSTGRES_PASSWORD", "#3Bigdata"),
    )


def _ensure_context_tables(cursor) -> None:
    cursor.execute("CREATE SCHEMA IF NOT EXISTS app_context")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_context.chat_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT,
            session_name TEXT,
            started_at TIMESTAMP DEFAULT NOW(),
            last_updated TIMESTAMP DEFAULT NOW()
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_context.ai_query_logs (
            log_id BIGSERIAL PRIMARY KEY,
            session_id TEXT,
            user_question TEXT,
            generated_sql TEXT,
            execution_status TEXT,
            execution_time_ms BIGINT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
    )


def ensure_session(session_id: str, user_id: str | None) -> None:
    with _connect() as conn:
        with conn.cursor() as cursor:
            _ensure_context_tables(cursor)
            cursor.execute(
                """
                INSERT INTO app_context.chat_sessions (session_id, user_id, session_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (session_id) DO UPDATE
                SET user_id = COALESCE(EXCLUDED.user_id, app_context.chat_sessions.user_id),
                    last_updated = NOW()
                """,
                (session_id, user_id, f"Session {session_id}"),
            )


def log_query(
    session_id: str,
    user_question: str,
    generated_sql: str,
    execution_status: str,
    execution_time_ms: int,
    error_message: str | None = None,
) -> None:
    with _connect() as conn:
        with conn.cursor() as cursor:
            _ensure_context_tables(cursor)
            cursor.execute(
                """
                INSERT INTO app_context.ai_query_logs (
                    session_id,
                    user_question,
                    generated_sql,
                    execution_status,
                    execution_time_ms,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    user_question,
                    generated_sql,
                    execution_status,
                    execution_time_ms,
                    error_message,
                ),
            )
