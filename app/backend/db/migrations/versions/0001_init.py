"""initial schema: users, refresh_tokens, user_preferences

Revision ID: 0001_init
Revises:
Create Date: 2026-05-28
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import CITEXT, UUID


revision: str = "0001_init"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Schema is created by env.py before context.configure(), but be defensive.
    op.execute("CREATE SCHEMA IF NOT EXISTS app")

    # Extensions for citext + pgcrypto (for gen_random_uuid).
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    op.create_table(
        "users",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("email", CITEXT(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column(
            "role",
            sa.String(length=16),
            server_default="user",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("role in ('user','admin')", name="users_role_check"),
        sa.UniqueConstraint("email", name="uq_users_email"),
        schema="app",
    )
    op.create_index("ix_users_email", "users", ["email"], unique=False, schema="app")

    op.create_table(
        "refresh_tokens",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("jti", sa.String(length=64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.users.id"],
            name="fk_refresh_tokens_user_id_users",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("jti", name="uq_refresh_tokens_jti"),
        schema="app",
    )
    op.create_index(
        "ix_refresh_tokens_user_id",
        "refresh_tokens",
        ["user_id"],
        unique=False,
        schema="app",
    )
    op.create_index(
        "ix_refresh_tokens_jti",
        "refresh_tokens",
        ["jti"],
        unique=False,
        schema="app",
    )

    op.create_table(
        "user_preferences",
        sa.Column("user_id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "theme",
            sa.String(length=16),
            server_default="system",
            nullable=False,
        ),
        sa.Column(
            "default_chart_type",
            sa.String(length=16),
            server_default="auto",
            nullable=False,
        ),
        sa.Column("default_model", sa.String(length=64), nullable=True),
        sa.Column(
            "preferred_language",
            sa.String(length=8),
            server_default="vi",
            nullable=False,
        ),
        sa.Column(
            "export_delimiter",
            sa.String(length=4),
            server_default=",",
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.users.id"],
            name="fk_user_preferences_user_id_users",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "theme in ('light','dark','system')",
            name="user_preferences_theme_check",
        ),
        sa.CheckConstraint(
            "default_chart_type in ('auto','bar','line','pie','table')",
            name="user_preferences_chart_check",
        ),
        sa.CheckConstraint(
            "preferred_language in ('vi','en')",
            name="user_preferences_lang_check",
        ),
        schema="app",
    )


def downgrade() -> None:
    op.drop_table("user_preferences", schema="app")
    op.drop_index("ix_refresh_tokens_jti", table_name="refresh_tokens", schema="app")
    op.drop_index("ix_refresh_tokens_user_id", table_name="refresh_tokens", schema="app")
    op.drop_table("refresh_tokens", schema="app")
    op.drop_index("ix_users_email", table_name="users", schema="app")
    op.drop_table("users", schema="app")
