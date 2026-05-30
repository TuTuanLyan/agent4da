"""agent contextual learning: feedback and suggestion events

Revision ID: 0008_agent_contextual_learning
Revises: 0007_agent_v2
Create Date: 2026-05-30

Stores explicit user feedback and generated suggestion sets so the v2 agent can
rank future clarification chips from session/history behavior without
fine-tuning the underlying model.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "0008_agent_contextual_learning"
down_revision: Union[str, None] = "0007_agent_v2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "agent_feedback",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("session_id", UUID(as_uuid=True), nullable=True),
        sa.Column("run_id", UUID(as_uuid=True), nullable=True),
        sa.Column("feedback_type", sa.String(length=32), nullable=False),
        sa.Column("selected_suggestion", JSONB(), nullable=True),
        sa.Column("free_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["app.users.id"], name="fk_agent_feedback_user_id_users", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["session_id"], ["app.chat_sessions.id"], name="fk_agent_feedback_session_id_chat_sessions", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["run_id"], ["app.query_runs.id"], name="fk_agent_feedback_run_id_query_runs", ondelete="SET NULL"),
        sa.CheckConstraint(
            "feedback_type in ('positive','negative','suggestion_click','free_text')",
            name="agent_feedback_type_check",
        ),
        schema="app",
    )
    op.create_index("ix_agent_feedback_user_id", "agent_feedback", ["user_id"], schema="app")
    op.create_index("ix_agent_feedback_session_id", "agent_feedback", ["session_id"], schema="app")
    op.create_index("ix_agent_feedback_run_id", "agent_feedback", ["run_id"], schema="app")

    op.create_table(
        "agent_suggestion_events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("session_id", UUID(as_uuid=True), nullable=True),
        sa.Column("run_id", UUID(as_uuid=True), nullable=True),
        sa.Column("input_question", sa.Text(), nullable=False),
        sa.Column("intent", sa.String(length=64), nullable=True),
        sa.Column("suggestions_generated", JSONB(), nullable=False),
        sa.Column("selected_suggestion", JSONB(), nullable=True),
        sa.Column("result_status", sa.String(length=16), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["app.users.id"], name="fk_agent_suggestion_events_user_id_users", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["session_id"], ["app.chat_sessions.id"], name="fk_agent_suggestion_events_session_id_chat_sessions", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["run_id"], ["app.query_runs.id"], name="fk_agent_suggestion_events_run_id_query_runs", ondelete="SET NULL"),
        schema="app",
    )
    op.create_index("ix_agent_suggestion_events_user_id", "agent_suggestion_events", ["user_id"], schema="app")
    op.create_index("ix_agent_suggestion_events_session_id", "agent_suggestion_events", ["session_id"], schema="app")
    op.create_index("ix_agent_suggestion_events_run_id", "agent_suggestion_events", ["run_id"], schema="app")


def downgrade() -> None:
    op.drop_index("ix_agent_suggestion_events_run_id", table_name="agent_suggestion_events", schema="app")
    op.drop_index("ix_agent_suggestion_events_session_id", table_name="agent_suggestion_events", schema="app")
    op.drop_index("ix_agent_suggestion_events_user_id", table_name="agent_suggestion_events", schema="app")
    op.drop_table("agent_suggestion_events", schema="app")

    op.drop_index("ix_agent_feedback_run_id", table_name="agent_feedback", schema="app")
    op.drop_index("ix_agent_feedback_session_id", table_name="agent_feedback", schema="app")
    op.drop_index("ix_agent_feedback_user_id", table_name="agent_feedback", schema="app")
    op.drop_table("agent_feedback", schema="app")
