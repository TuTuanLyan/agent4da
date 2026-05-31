#!/usr/bin/env bash
# Repair the Airflow database role on an EXISTING PostgreSQL volume.
#
# init/01_init_schemas.sh creates the `airflow_user` role + `airflow` schema,
# but Postgres only runs /docker-entrypoint-initdb.d/* on a FRESH data volume.
# If your postgres_data volume predates that script (or the password changed),
# Airflow fails to boot with:
#     FATAL: password authentication failed for user "airflow_user"
#
# This re-applies just the role + schema + grants to the running database.
# It is idempotent and does NOT touch table data (your Iceberg JDBC catalog,
# Airflow metadata, and app schema are preserved). Credentials are read from
# the container's own environment, so no secrets are passed on the host.
#
# Usage (host, repo root):
#     bash script/postgres/fix_airflow_role.sh
#     docker compose up -d airflow      # then restart Airflow
set -euo pipefail

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres-db}"

log() { echo "[fix_airflow_role] $*"; }

state="$(docker inspect -f '{{.State.Status}}' "$POSTGRES_CONTAINER" 2>/dev/null || true)"
if [ "$state" != "running" ]; then
  log "ERROR: container '$POSTGRES_CONTAINER' is not running (state=${state:-absent})."
  log "Start it first:  docker compose up -d postgres"
  exit 1
fi

log "Applying airflow role + schema on $POSTGRES_CONTAINER ..."
# The container expands its own POSTGRES_*/AIRFLOW_DB_* env (from envs/postgre.env);
# superuser connects over the local socket (trust) like init_iceberg_schema.sh.
docker exec -i "$POSTGRES_CONTAINER" sh -s <<'OUTER'
set -eu
: "${AIRFLOW_DB_USER:?Missing AIRFLOW_DB_USER in container env}"
: "${AIRFLOW_DB_PASSWORD:?Missing AIRFLOW_DB_PASSWORD in container env}"
psql -v ON_ERROR_STOP=1 \
  -U "${POSTGRES_USER:-bigdata}" -d "${POSTGRES_DB:-agent4da}" \
  -v airflow_user="$AIRFLOW_DB_USER" \
  -v airflow_password="$AIRFLOW_DB_PASSWORD" \
  -v db_name="${POSTGRES_DB:-agent4da}" <<'EOSQL'
SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = :'airflow_user')
\gexec
SELECT format('ALTER ROLE %I WITH PASSWORD %L', :'airflow_user', :'airflow_password')
\gexec
CREATE SCHEMA IF NOT EXISTS airflow;
GRANT CONNECT ON DATABASE :"db_name" TO :"airflow_user";
GRANT USAGE, CREATE ON SCHEMA airflow TO :"airflow_user";
ALTER ROLE :"airflow_user" SET search_path = airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow GRANT ALL ON TABLES TO :"airflow_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow GRANT ALL ON SEQUENCES TO :"airflow_user";
EOSQL
OUTER

log "Verifying airflow_user can authenticate over TCP (same path Airflow uses) ..."
# -h <container> forces a TCP/scram connection, so this genuinely checks the password.
if docker exec -i "$POSTGRES_CONTAINER" sh -s <<'OUTER'
set -eu
PGPASSWORD="$AIRFLOW_DB_PASSWORD" psql -h "$(hostname)" \
  -U "$AIRFLOW_DB_USER" -d "${POSTGRES_DB:-agent4da}" -tAc "SELECT 1" >/dev/null
OUTER
then
  log "OK: airflow_user authenticates. Now run:  docker compose up -d airflow"
else
  log "Role applied, but the TCP auth check failed. Inspect: docker logs $POSTGRES_CONTAINER --tail 50"
  exit 1
fi
