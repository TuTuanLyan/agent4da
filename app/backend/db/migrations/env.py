"""Alembic environment for the Agent4DA backend.

Reads the DB URL from APP_DB_URL (via api.settings) so the same migration
runs locally and in CI without editing alembic.ini.
"""

from __future__ import annotations

import os
import sys

from alembic import context
from sqlalchemy import engine_from_config, pool

# Make `api`, `db`, etc. importable when alembic runs.
BACKEND_ROOT = "/opt/project/app/backend"
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

from api.settings import get_settings  # noqa: E402
from db.base import APP_SCHEMA, Base  # noqa: E402
from db import models  # noqa: F401,E402  # ensure models register on Base.metadata

config = context.config

settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.db_url)

target_metadata = Base.metadata


def include_object(object_, name, type_, reflected, compare_to):
    # Only emit objects in the "app" schema; this prevents Alembic from
    # touching airflow, iceberg, bronze_meta, silver_meta schemas managed
    # by other parts of the project.
    if type_ == "table" and object_.schema and object_.schema != APP_SCHEMA:
        return False
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        version_table="alembic_version",
        version_table_schema=APP_SCHEMA,
        include_schemas=True,
        include_object=include_object,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        # Ensure the "app" schema exists before Alembic looks for its
        # version table.
        from sqlalchemy import text

        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {APP_SCHEMA}"))
        connection.commit()

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table="alembic_version",
            version_table_schema=APP_SCHEMA,
            include_schemas=True,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
