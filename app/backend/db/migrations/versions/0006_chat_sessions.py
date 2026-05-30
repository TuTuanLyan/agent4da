"""add chat_sessions + conversation/engine columns on query_runs

Revision ID: 0006_chat_sessions
Revises: 0005_layer_stats
Create Date: 2026-05-30

Adds the chat-session model backing the Ask workspace:
- a `chat_sessions` table (one row per conversation thread);
- `session_id` / `turn_index` on `query_runs` so a run can belong to a thread;
- `insights` (JSONB) and `agent_engine` columns used by the result block and the
  Settings agent-engine readout.

The `query_runs.session_id` FK is ON DELETE SET NULL so deleting a chat keeps its
runs visible in History.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "0006_chat_sessions"
down_revision: Union[str, None] = "0005_layer_stats"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "chat_sessions",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "last_used_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.users.id"],
            name="fk_chat_sessions_user_id_users",
            ondelete="CASCADE",
        ),
        schema="app",
    )
    op.create_index(
        "ix_chat_sessions_user_id", "chat_sessions", ["user_id"], schema="app"
    )
    op.create_index(
        "ix_chat_sessions_last_used_at",
        "chat_sessions",
        ["last_used_at"],
        schema="app",
    )

    # Conversation + engine columns on query_runs.
    op.add_column(
        "query_runs",
        sa.Column("session_id", UUID(as_uuid=True), nullable=True),
        schema="app",
    )
    op.add_column(
        "query_runs",
        sa.Column("turn_index", sa.Integer(), nullable=True),
        schema="app",
    )
    op.add_column(
        "query_runs",
        sa.Column("insights", JSONB(), nullable=True),
        schema="app",
    )
    op.add_column(
        "query_runs",
        sa.Column(
            "agent_engine",
            sa.String(length=16),
            nullable=False,
            server_default="legacy",
        ),
        schema="app",
    )
    op.create_foreign_key(
        "fk_query_runs_session_id_chat_sessions",
        "query_runs",
        "chat_sessions",
        ["session_id"],
        ["id"],
        source_schema="app",
        referent_schema="app",
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_query_runs_session_id", "query_runs", ["session_id"], schema="app"
    )


def downgrade() -> None:
    op.drop_index("ix_query_runs_session_id", table_name="query_runs", schema="app")
    op.drop_constraint(
        "fk_query_runs_session_id_chat_sessions",
        "query_runs",
        schema="app",
        type_="foreignkey",
    )
    op.drop_column("query_runs", "agent_engine", schema="app")
    op.drop_column("query_runs", "insights", schema="app")
    op.drop_column("query_runs", "turn_index", schema="app")
    op.drop_column("query_runs", "session_id", schema="app")

    op.drop_index(
        "ix_chat_sessions_last_used_at", table_name="chat_sessions", schema="app"
    )
    op.drop_index("ix_chat_sessions_user_id", table_name="chat_sessions", schema="app")
    op.drop_table("chat_sessions", schema="app")
