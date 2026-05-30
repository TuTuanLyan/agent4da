"""add chat session pinning

Revision ID: 0009_chat_session_pin
Revises: 0008_agent_contextual_learning
Create Date: 2026-05-30
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0009_chat_session_pin"
down_revision: Union[str, None] = "0008_agent_contextual_learning"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "chat_sessions",
        sa.Column("is_pinned", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema="app",
    )
    op.add_column(
        "chat_sessions",
        sa.Column("pinned_at", sa.DateTime(timezone=True), nullable=True),
        schema="app",
    )
    op.create_index(
        "ix_chat_sessions_is_pinned", "chat_sessions", ["is_pinned"], schema="app"
    )
    op.create_index(
        "ix_chat_sessions_pinned_at", "chat_sessions", ["pinned_at"], schema="app"
    )


def downgrade() -> None:
    op.drop_index("ix_chat_sessions_pinned_at", table_name="chat_sessions", schema="app")
    op.drop_index("ix_chat_sessions_is_pinned", table_name="chat_sessions", schema="app")
    op.drop_column("chat_sessions", "pinned_at", schema="app")
    op.drop_column("chat_sessions", "is_pinned", schema="app")
