"""add pipeline_trigger_audit table

Revision ID: 0004_pipeline_audit
Revises: 0003_sample_questions
Create Date: 2026-05-28
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "0004_pipeline_audit"
down_revision: Union[str, None] = "0003_sample_questions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "pipeline_trigger_audit",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("dag_id", sa.String(length=128), nullable=False),
        sa.Column("airflow_run_id", sa.String(length=256), nullable=True),
        sa.Column("conf", JSONB(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="triggered"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.users.id"],
            name="fk_pipeline_audit_user_id_users",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "status in ('triggered','failed')",
            name="pipeline_audit_status_check",
        ),
        schema="app",
    )
    op.create_index(
        "ix_pipeline_audit_dag_created",
        "pipeline_trigger_audit",
        ["dag_id", sa.text("created_at DESC")],
        schema="app",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_pipeline_audit_dag_created",
        table_name="pipeline_trigger_audit",
        schema="app",
    )
    op.drop_table("pipeline_trigger_audit", schema="app")
