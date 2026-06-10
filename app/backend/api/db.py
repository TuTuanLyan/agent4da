from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator
from uuid import UUID

import psycopg
from fastapi import HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Json

from .settings import get_settings

log = logging.getLogger("agent4da.db")


def json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    return value


def json_param(value: Any) -> Json:
    return Json(json_ready(value))


@contextmanager
def db_conn() -> Iterator[psycopg.Connection]:
    settings = get_settings()
    conn = psycopg.connect(settings.psycopg_dsn, row_factory=dict_row, connect_timeout=5)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def require_db() -> psycopg.Connection:
    try:
        return psycopg.connect(get_settings().psycopg_dsn, row_factory=dict_row, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Postgres unavailable: {exc.__class__.__name__}") from exc


def init_db() -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS app;

    CREATE TABLE IF NOT EXISTS app.users (
      id UUID PRIMARY KEY,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role IN ('user', 'admin')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_app_users_email_lower
      ON app.users (lower(email));

    CREATE TABLE IF NOT EXISTS app.refresh_tokens (
      jti TEXT PRIMARY KEY,
      user_id UUID NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      expires_at TIMESTAMPTZ NOT NULL,
      revoked_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS ix_app_refresh_tokens_user_created
      ON app.refresh_tokens(user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS ix_app_refresh_tokens_active_expires
      ON app.refresh_tokens(expires_at)
      WHERE revoked_at IS NULL;

    CREATE TABLE IF NOT EXISTS app.user_preferences (
      user_id UUID PRIMARY KEY REFERENCES app.users(id) ON DELETE CASCADE,
      theme TEXT NOT NULL DEFAULT 'system',
      default_chart_type TEXT NOT NULL DEFAULT 'auto',
      default_model TEXT,
      preferred_language TEXT NOT NULL DEFAULT 'vi',
      export_delimiter TEXT NOT NULL DEFAULT ',',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS app.chat_sessions (
      id UUID PRIMARY KEY,
      user_id UUID NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      title TEXT,
      is_pinned BOOLEAN NOT NULL DEFAULT false,
      pinned_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_used_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS app.query_runs (
      id UUID PRIMARY KEY,
      user_id UUID NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      session_id UUID REFERENCES app.chat_sessions(id) ON DELETE SET NULL,
      question TEXT NOT NULL,
      generated_sql TEXT,
      guard_status TEXT,
      columns JSONB NOT NULL DEFAULT '[]'::jsonb,
      rows JSONB NOT NULL DEFAULT '[]'::jsonb,
      row_count INTEGER NOT NULL DEFAULT 0,
      error TEXT,
      latency_ms INTEGER,
      summary TEXT,
      insights JSONB NOT NULL DEFAULT '[]'::jsonb,
      key_numbers JSONB NOT NULL DEFAULT '[]'::jsonb,
      chart_suggestion JSONB,
      chart_type TEXT,
      chart JSONB,
      chart_data JSONB NOT NULL DEFAULT '[]'::jsonb,
      agent_engine TEXT NOT NULL DEFAULT 'legacy',
      status TEXT NOT NULL,
      turn_index INTEGER,
      agent_trace JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS app.favorite_runs (
      user_id UUID NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      run_id UUID NOT NULL REFERENCES app.query_runs(id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (user_id, run_id)
    );

    CREATE TABLE IF NOT EXISTS app.agent_feedback (
      id UUID PRIMARY KEY,
      user_id UUID NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      run_id UUID,
      session_id UUID,
      feedback_type TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS ix_chat_sessions_user_last
      ON app.chat_sessions(user_id, last_used_at DESC);
    CREATE INDEX IF NOT EXISTS ix_query_runs_user_created
      ON app.query_runs(user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS ix_query_runs_session_turn
      ON app.query_runs(session_id, turn_index);

    ALTER TABLE app.query_runs ADD COLUMN IF NOT EXISTS rows JSONB NOT NULL DEFAULT '[]'::jsonb;
    ALTER TABLE app.query_runs ADD COLUMN IF NOT EXISTS summary TEXT;
    ALTER TABLE app.query_runs ADD COLUMN IF NOT EXISTS chart JSONB;

    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'app' AND table_name = 'query_runs' AND column_name = 'result_json'
      ) THEN
        EXECUTE 'UPDATE app.query_runs SET rows = result_json WHERE rows = ''[]''::jsonb AND result_json IS NOT NULL';
      END IF;
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'app' AND table_name = 'query_runs' AND column_name = 'summary_text'
      ) THEN
        EXECUTE 'UPDATE app.query_runs SET summary = summary_text WHERE summary IS NULL AND summary_text IS NOT NULL';
      END IF;
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'app' AND table_name = 'query_runs' AND column_name = 'chart_payload'
      ) THEN
        EXECUTE 'UPDATE app.query_runs SET chart = chart_payload WHERE chart IS NULL AND chart_payload IS NOT NULL';
      END IF;
    END $$;
    """
    try:
        with db_conn() as conn:
            conn.execute(ddl)
    except Exception as exc:  # noqa: BLE001
        log.warning("Postgres schema bootstrap skipped: %s", exc)
