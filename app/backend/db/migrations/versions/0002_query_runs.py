"""add query_runs table

Revision ID: 0002_query_runs
Revises: 0001_init
Create Date: 2026-05-28
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "0002_query_runs"
down_revision: Union[str, None] = "0001_init"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "query_runs",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("generated_sql", sa.Text(), nullable=True),
        sa.Column("guard_status", sa.String(length=32), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("summary_text", sa.Text(), nullable=True),
        sa.Column("key_numbers", JSONB(), nullable=True),
        sa.Column("chart_type", sa.String(length=16), nullable=True),
        sa.Column("chart_suggestion", JSONB(), nullable=True),
        sa.Column("columns", JSONB(), nullable=True),
        sa.Column(
            "result_json",
            JSONB(),
            nullable=True,
        ),
        sa.Column("trino_query_id", sa.String(length=128), nullable=True),
        sa.Column("is_favorite", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="success",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.users.id"],
            name="fk_query_runs_user_id_users",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "status in ('running','success','failed','stopped','blocked')",
            name="query_runs_status_check",
        ),
        sa.CheckConstraint(
            "guard_status is null or guard_status in ('pass','blocked','auto_limited','error')",
            name="query_runs_guard_status_check",
        ),
        sa.CheckConstraint(
            "chart_type is null or chart_type in ('auto','bar','line','pie','table','scatter')",
            name="query_runs_chart_type_check",
        ),
        schema="app",
    )
    op.create_index(
        "ix_query_runs_user_id_created_at",
        "query_runs",
        ["user_id", sa.text("created_at DESC")],
        schema="app",
    )
    op.create_index(
        "ix_query_runs_is_favorite",
        "query_runs",
        ["user_id", "is_favorite"],
        schema="app",
        postgresql_where=sa.text("is_favorite = true"),
    )


def downgrade() -> None:
    op.drop_index("ix_query_runs_is_favorite", table_name="query_runs", schema="app")
    op.drop_index(
        "ix_query_runs_user_id_created_at", table_name="query_runs", schema="app"
    )
    op.drop_table("query_runs", schema="app")
