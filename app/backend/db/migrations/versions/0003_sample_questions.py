"""add sample_questions + seed defaults

Revision ID: 0003_sample_questions
Revises: 0002_query_runs
Create Date: 2026-05-28
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID


revision: str = "0003_sample_questions"
down_revision: Union[str, None] = "0002_query_runs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sample_questions",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("label", sa.String(length=64), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        schema="app",
    )
    # Seed the four chips from the frontend proposal.
    op.execute(
        """
        INSERT INTO app.sample_questions (label, question, sort_order)
        VALUES
            ('Doanh thu theo ngay',     'Doanh thu theo ngay trong thang 1 nam 2020', 10),
            ('Top brand',               'Top 5 brand co doanh thu cao nhat',          20),
            ('Category conversion',     'Ty le chuyen doi theo danh muc',             30),
            ('Session revenue',         'Doanh thu trung binh moi phien la bao nhieu',40)
        """
    )


def downgrade() -> None:
    op.drop_table("sample_questions", schema="app")
