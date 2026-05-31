#!/usr/bin/env bash
# One-shot recovery for the Agent4DA web console errors:
#   - "Airflow unreachable: ConnectError"
#   - "Dashboard unavailable / Metrics source unavailable"
#   - "Catalog source unavailable"
#
# Applies every fix in the correct order:
#   1. data_network exists
#   2. REBUILD the airflow image (picks up the entrypoint PID-cleanup + the
#      --no-deps Dockerfile change; a plain restart reuses the old image)
#   3. start + wait for Postgres, then repair the airflow_user role
#   4. bring up the rest of the stack
#   5. wait for Trino + Airflow to be healthy
#   6. probe what the backend actually sees
#
# Run on the HOST from anywhere; it cd's to the repo root itself.
#   bash script/recover_console.sh
set -euo pipefail

cd "$(cd "$(dirname "$0")/.." && pwd)"   # repo root (where .env / compose files live)

step() { printf '\n========== %s ==========\n' "$1"; }
command -v docker >/dev/null 2>&1 || { echo "docker not found on PATH"; exit 1; }

step "1/6  data_network"
docker network inspect data_network >/dev/null 2>&1 || docker network create data_network

step "2/6  rebuild airflow image"
docker compose build airflow

step "3/6  start Postgres and repair the airflow role"
docker compose up -d --wait postgres || docker compose up -d postgres
bash script/postgres/fix_airflow_role.sh || echo "(role repair reported an issue; continuing)"

step "4/6  bring up the rest of the stack"
docker compose up -d minio trino spark-master spark-worker kafka
docker compose up -d airflow app-api app-web

step "5/6  wait for Trino + Airflow to come up (up to ~120s)"
for _ in $(seq 1 24); do
  t=no; a=no
  curl -fsS -m4 http://localhost:8082/v1/info        >/dev/null 2>&1 && t=ok
  curl -fsS -m4 http://localhost:8081/api/v1/health   >/dev/null 2>&1 && a=ok
  printf '  trino=%s  airflow=%s\n' "$t" "$a"
  [ "$t" = ok ] && [ "$a" = ok ] && break
  sleep 5
done

step "6/6  diagnose (host + in-network view)"
bash script/diagnose_stack.sh || true

cat <<'NOTE'

------------------------------------------------------------------------------
If Trino + Airflow are OK but Dashboard/Catalog still say "source unavailable":
the Gold + semantic-metadata tables have not been built yet (manual DAGs).
Build them once Bronze/Silver have produced data:

  docker exec airflow airflow dags unpause bronze_pipeline
  docker exec airflow airflow dags unpause silver_pipeline
  docker exec airflow airflow dags unpause gold_pipeline
  docker exec airflow airflow dags unpause gold_metadata_pipeline
  docker exec airflow airflow dags trigger gold_pipeline
  docker exec airflow airflow dags trigger gold_metadata_pipeline

The Catalog page reads iceberg.metadata.semantic_* (built by gold_metadata_pipeline);
the Dashboard reads iceberg.gold.* (built by gold_pipeline).
------------------------------------------------------------------------------
NOTE
