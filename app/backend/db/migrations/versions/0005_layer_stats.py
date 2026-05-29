"""add layer_stats table

Revision ID: 0005_layer_stats
Revises: 0004_pipeline_audit
Create Date: 2026-05-29
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "0005_layer_stats"
down_revision: Union[str, None] = "0004_pipeline_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "layer_stats",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("layer", sa.String(length=16), nullable=False),
        sa.Column("metric_name", sa.String(length=64), nullable=False),
        sa.Column("metric_value_bigint", sa.BigInteger(), nullable=True),
        sa.Column("metric_value_double", sa.Float(), nullable=True),
        sa.Column("metric_unit", sa.String(length=32), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False, server_default="scheduler"),
        sa.Column("detail", JSONB(), nullable=True),
        sa.Column(
            "measured_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "layer in ('bronze','silver','gold','metadata')",
            name="layer_stats_layer_check",
        ),
        schema="app",
    )
    op.create_index("ix_layer_stats_layer", "layer_stats", ["layer"], schema="app")
    op.create_index(
        "ix_layer_stats_metric_name",
        "layer_stats",
        ["metric_name"],
        schema="app",
    )
    op.create_index(
        "ix_layer_stats_measured_at",
        "layer_stats",
        ["measured_at"],
        schema="app",
    )
    op.create_index(
        "ix_layer_stats_lookup",
        "layer_stats",
        ["layer", "metric_name", sa.text("measured_at DESC")],
        schema="app",
    )


def downgrade() -> None:
    op.drop_index("ix_layer_stats_lookup", table_name="layer_stats", schema="app")
    op.drop_index("ix_layer_stats_measured_at", table_name="layer_stats", schema="app")
    op.drop_index("ix_layer_stats_metric_name", table_name="layer_stats", schema="app")
    op.drop_index("ix_layer_stats_layer", table_name="layer_stats", schema="app")
    op.drop_table("layer_stats", schema="app")
