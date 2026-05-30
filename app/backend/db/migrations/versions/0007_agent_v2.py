"""agent v2: query_runs trace/chart columns + agent_checkpoint_snapshots

Revision ID: 0007_agent_v2
Revises: 0006_chat_sessions
Create Date: 2026-05-30

Backs the v2 LangGraph engine:
- new `query_runs` columns: `chart_payload`, `chart_data`, `agent_trace`
  (JSONB), `retry_count` (int), `model_used` (text);
- `agent_checkpoint_snapshots` table holding a compact per-turn final-state
  snapshot (thread_id + checkpoint_id + state_data), used instead of the
  langgraph Postgres saver.

All additions are nullable / additive so existing rows and the legacy engine
keep working unchanged.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "0007_agent_v2"
down_revision: Union[str, None] = "0006_chat_sessions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("query_runs", sa.Column("chart_payload", JSONB(), nullable=True), schema="app")
    op.add_column("query_runs", sa.Column("chart_data", JSONB(), nullable=True), schema="app")
    op.add_column("query_runs", sa.Column("agent_trace", JSONB(), nullable=True), schema="app")
    op.add_column("query_runs", sa.Column("retry_count", sa.Integer(), nullable=True), schema="app")
    op.add_column("query_runs", sa.Column("model_used", sa.String(length=64), nullable=True), schema="app")

    op.create_table(
        "agent_checkpoint_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("thread_id", sa.String(length=128), nullable=False),
        sa.Column("checkpoint_id", sa.String(length=160), nullable=False),
        sa.Column("state_data", JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        schema="app",
    )
    op.create_index(
        "ix_agent_checkpoint_snapshots_thread_id",
        "agent_checkpoint_snapshots",
        ["thread_id"],
        schema="app",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_agent_checkpoint_snapshots_thread_id",
        table_name="agent_checkpoint_snapshots",
        schema="app",
    )
    op.drop_table("agent_checkpoint_snapshots", schema="app")

    op.drop_column("query_runs", "model_used", schema="app")
    op.drop_column("query_runs", "retry_count", schema="app")
    op.drop_column("query_runs", "agent_trace", schema="app")
    op.drop_column("query_runs", "chart_data", schema="app")
    op.drop_column("query_runs", "chart_payload", schema="app")
