#!/bin/sh
# Backend container entrypoint.
# - Waits for Postgres to be reachable (best-effort, capped).
# - Runs alembic migrations (idempotent).
# - Seeds the bootstrap admin user (idempotent).
# - Execs the original CMD (uvicorn).
set -eu

cd /opt/project/app/backend

echo "[entrypoint] APP_ENV=${APP_ENV:-local} starting up"

# Wait for the DB up to ~60s. Postgres may still be initializing on the
# first compose up. If it never comes up we still try the migration, which
# will fail loudly with a useful error.
python -c '
import os, sys, time, urllib.parse
url = os.environ.get("APP_DB_URL", "")
if not url:
    print("[entrypoint] APP_DB_URL not set, skipping wait")
    sys.exit(0)
try:
    import psycopg
except ImportError:
    print("[entrypoint] psycopg not available, skipping wait")
    sys.exit(0)
# Convert SQLAlchemy URL to a libpq DSN psycopg understands.
dsn = url.replace("postgresql+psycopg://", "postgresql://", 1)
deadline = time.time() + 60
while time.time() < deadline:
    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        print("[entrypoint] postgres is reachable")
        sys.exit(0)
    except Exception as exc:
        print(f"[entrypoint] postgres not ready yet: {exc.__class__.__name__}")
        time.sleep(2)
print("[entrypoint] postgres did not become ready within 60s; continuing anyway")
'

echo "[entrypoint] running alembic upgrade head"
alembic -c /opt/project/app/backend/alembic.ini upgrade head

echo "[entrypoint] seeding bootstrap admin (idempotent)"
python -m scripts.seed_admin || echo "[entrypoint] seed_admin reported a non-fatal issue, continuing"

echo "[entrypoint] handing off to: $*"
exec "$@"
