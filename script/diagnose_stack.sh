#!/usr/bin/env bash
# Diagnose (and optionally start) the Agent4DA stack that the web console depends on.
#
# The console's Dashboard, Catalog, and Pipelines pages fail with "source
# unavailable" / "Airflow unreachable: ConnectError" when the app-api container
# cannot reach Trino (trino:8080) or Airflow (airflow:8080) over data_network.
# Those are almost always operational: a container is not running, not on the
# network, or still warming up. This script reports exactly which one and why,
# and -- with --up -- brings the stack up in a conflict-free order.
#
# Usage:
#   bash script/diagnose_stack.sh           # diagnose only (no changes)
#   bash script/diagnose_stack.sh --up      # create network + start stack, then diagnose
#
# Run this on the HOST (where docker lives), from the repo root.
set -euo pipefail

UP=0
for arg in "$@"; do
  case "$arg" in
    --up) UP=1 ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

PASS=0; FAIL=0; WARN=0
ok()   { printf '  [OK]   %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  [FAIL] %s\n' "$1"; FAIL=$((FAIL+1)); }
warn() { printf '  [WARN] %s\n' "$1"; WARN=$((WARN+1)); }
hdr()  { printf '\n== %s ==\n' "$1"; }

command -v docker >/dev/null 2>&1 || { echo "docker not found on PATH"; exit 1; }

# container_state NAME -> prints running|exited|absent
container_state() {
  local s
  s="$(docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || true)"
  [ -z "$s" ] && { echo absent; return; }
  echo "$s"
}

# host_http URL  -> 0 if HTTP <400
host_http() {
  curl -fsS -m 5 -o /dev/null "$1" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Optional bring-up, in dependency order, avoiding the postgres-db duplicate.
# docker-compose.airflow.yml bundles its OWN postgres-db (same container_name
# as docker-compose.postgre.yml). Starting both collides on the name, so we
# start the shared postgres first, then bring up ONLY the airflow service with
# --no-deps so it reuses the running postgres-db.
# ---------------------------------------------------------------------------
if [ "$UP" -eq 1 ]; then
  hdr "Bringing up stack (--up)"
  docker network inspect data_network >/dev/null 2>&1 || {
    echo "  creating data_network"; docker network create data_network >/dev/null
  }
  set +e
  echo "  starting postgres-db ..." ; docker compose -f docker-compose.postgre.yml up -d
  echo "  starting minio ..."       ; docker compose -f docker-compose.minio.yml   up -d
  echo "  starting trino ..."       ; docker compose -f docker-compose.trino.yml   up -d
  echo "  starting spark ..."       ; docker compose -f docker-compose.spark.yml   up -d
  echo "  starting airflow (reusing existing postgres-db, --no-deps) ..."
  docker compose -f docker-compose.airflow.yml up -d --no-deps airflow
  echo "  starting app-api + app-web ..."
  docker compose -f docker-compose.app.yml up -d
  set -e
  echo "  waiting up to ~90s for Trino + Airflow to become query/health ready ..."
  for _ in $(seq 1 18); do
    if host_http http://localhost:8082/v1/info && host_http http://localhost:8081/api/v1/health; then
      break
    fi
    sleep 5
  done
fi

# ---------------------------------------------------------------------------
hdr "Network"
if docker network inspect data_network >/dev/null 2>&1; then
  ok "data_network exists"
else
  bad "data_network missing -> run: docker network create data_network"
fi

# ---------------------------------------------------------------------------
hdr "Containers"
# name:label pairs the console cares about
for pair in \
  "postgres-db:Postgres" "minio:MinIO" "trino:Trino" \
  "airflow:Airflow" "app-api:Backend API" "app-web:Frontend" "spark-master:Spark"; do
  name="${pair%%:*}"; label="${pair##*:}"
  st="$(container_state "$name")"
  case "$st" in
    running) ok "$label ($name) running" ;;
    absent)  if [ "$name" = "spark-master" ]; then warn "$label ($name) not created (optional for the 4 errors)"; else bad "$label ($name) not created -> not started, or failed to start"; fi ;;
    *)       bad "$label ($name) state=$st -> inspect: docker logs $name --tail 50" ;;
  esac
done

# Detect the postgres-db duplicate-definition pitfall.
if grep -qE 'container_name:\s*postgres-db' docker-compose.airflow.yml 2>/dev/null; then
  warn "docker-compose.airflow.yml also defines container_name postgres-db; do NOT run 'make postgre-up' and 'make airflow-up' both as-is. Use this script's --up path (it starts airflow with --no-deps)."
fi

# ---------------------------------------------------------------------------
hdr "Host endpoints (what your browser hits)"
host_http http://localhost:8082/v1/info            && ok "Trino   http://localhost:8082/v1/info"        || bad "Trino   http://localhost:8082 unreachable"
host_http http://localhost:8081/api/v1/health      && ok "Airflow http://localhost:8081/api/v1/health"  || bad "Airflow http://localhost:8081 unreachable (UI moved 8082->8081)"
host_http http://localhost:8083/healthz            && ok "API     http://localhost:8083/healthz"         || bad "API     http://localhost:8083 unreachable"
host_http http://localhost:3000                    && ok "Web     http://localhost:3000"                 || warn "Web   http://localhost:3000 unreachable"
host_http http://localhost:9000/minio/health/live  && ok "MinIO   http://localhost:9000"                 || warn "MinIO http://localhost:9000 unreachable"
host_http http://localhost:8080/json/              && ok "Spark   http://localhost:8080"                 || warn "Spark http://localhost:8080 unreachable (optional)"

# ---------------------------------------------------------------------------
hdr "In-network view (what app-api actually sees -- the authoritative test)"
if [ "$(container_state app-api)" = "running" ]; then
  # Replicate the backend's own reachability check from inside its container,
  # using the same DNS names and the same Airflow creds it already has in env.
  docker exec app-api python - <<'PY' || true
import os, urllib.request, base64
def probe(label, url, auth=None, timeout=4):
    req = urllib.request.Request(url)
    if auth and auth[0]:
        tok = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.add_header("Authorization", "Basic " + tok)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            print(f"  [OK]   {label} -> HTTP {r.status}")
    except Exception as exc:
        print(f"  [FAIL] {label} -> {exc.__class__.__name__}: {exc}")

th = os.getenv("APP_TRINO_HOST", "trino"); tp = os.getenv("APP_TRINO_PORT", "8080")
ab = os.getenv("APP_AIRFLOW_BASE_URL", "http://airflow:8080").rstrip("/")
au = os.getenv("APP_AIRFLOW_USER", ""); ap = os.getenv("APP_AIRFLOW_PASSWORD", "")
probe(f"Trino   {th}:{tp}/v1/info", f"http://{th}:{tp}/v1/info")
probe(f"Airflow {ab}/api/v1/health", f"{ab}/api/v1/health", auth=(au, ap))
PY
else
  warn "app-api not running -> cannot run the in-network probe (start it first, or use --up)"
fi

# ---------------------------------------------------------------------------
hdr "Summary"
printf '  PASS=%d  WARN=%d  FAIL=%d\n' "$PASS" "$WARN" "$FAIL"
if [ "$FAIL" -gt 0 ]; then
  cat <<'TIP'

  Next steps if anything is FAIL:
    1. Bring the stack up cleanly:   bash script/diagnose_stack.sh --up
    2. A container exits on start?   docker logs <name> --tail 80
    3. Trino just FAILs briefly?     it needs ~30-60s after start to serve SQL.
    4. Airflow FAIL but running?     check creds: APP_AIRFLOW_USER / APP_AIRFLOW_PASSWORD in envs/app.env
                                     must match _AIRFLOW_WWW_USER_* in envs/airflow.env.
TIP
  exit 1
fi
echo "  All required services reachable. The console's Dashboard/Catalog/Pipelines should load."
