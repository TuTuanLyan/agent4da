#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
SESSION_ID="${SESSION_ID:-test-agent-checkpoint-001}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres-db}"
POSTGRES_USER="${POSTGRES_USER:-bigdata}"
POSTGRES_DB="${POSTGRES_DB:-agent4da}"

TOTAL=0
PASSED=0
FAILED=0
FAILED_TESTS=()

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name"
    exit 1
  fi
}

payload() {
  local question="$1"
  SESSION_ID="$SESSION_ID" QUESTION="$question" python3 - <<'PY'
import json
import os

print(json.dumps({
    "session_id": os.environ["SESSION_ID"],
    "question": os.environ["QUESTION"],
}, ensure_ascii=False))
PY
}

ask() {
  local question="$1"
  local body
  body="$(payload "$question")"
  curl -sS -X POST "$API_URL/ask" \
    -H 'Content-Type: application/json' \
    -d "$body"
}

psql_scalar() {
  local sql="$1"
  docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "$sql"
}

sql_quote() {
  VALUE="$1" python3 - <<'PY'
import os

print("'" + os.environ["VALUE"].replace("'", "''") + "'")
PY
}

checkpoint_count() {
  local quoted_session_id
  quoted_session_id="$(sql_quote "$SESSION_ID")"
  psql_scalar "SELECT COUNT(*) FROM app_context.langgraph_checkpoints WHERE thread_id = ${quoted_session_id};"
}

official_checkpoint_count() {
  local quoted_session_id
  quoted_session_id="$(sql_quote "$SESSION_ID")"
  psql_scalar "SELECT COUNT(*) FROM app_context.checkpoints WHERE thread_id = ${quoted_session_id};"
}

run_test() {
  local name="$1"
  shift
  TOTAL=$((TOTAL + 1))
  printf '\n[%02d] %s\n' "$TOTAL" "$name"

  set +e
  (
    set -euo pipefail
    "$@"
  )
  local rc=$?
  set -e

  if [[ "$rc" -eq 0 ]]; then
    PASSED=$((PASSED + 1))
    echo "PASS: $name"
  else
    FAILED=$((FAILED + 1))
    FAILED_TESTS+=("$name")
    echo "FAIL: $name"
  fi
}

preflight_trino_catalog() {
  echo "Preflight: checking Trino iceberg_catalog"
  local catalogs
  catalogs="$(docker exec trino trino --execute "SHOW CATALOGS")"
  if ! grep -q '"iceberg_catalog"' <<<"$catalogs"; then
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent checkpoint tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

test_checkpoint_created_and_increases() {
  local before_count
  before_count="$(checkpoint_count 2>/dev/null || echo 0)"

  local response
  response="$(ask "Brand nào có nhiều event nhất?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
assert data.get("status") == "success", data
assert data.get("intent") == "ranking", data
assert "iceberg_catalog.gold.daily_brand_summary" in (data.get("generated_sql") or ""), data
PY

  local after_first
  after_first="$(checkpoint_count)"
  if [[ "$after_first" -le "$before_count" ]]; then
    echo "Expected app_context.langgraph_checkpoints count to increase after first /ask. before=$before_count after=$after_first"
    return 1
  fi

  local official_after_first
  official_after_first="$(official_checkpoint_count)"
  if [[ "$official_after_first" -lt 1 ]]; then
    echo "Expected official LangGraph checkpoint row in app_context.checkpoints for thread_id=$SESSION_ID"
    return 1
  fi

  response="$(ask "Thế category thì sao?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
assert data.get("status") == "success", data
assert data.get("context_used") is True, data
assert data.get("intent") == "ranking", data
assert "iceberg_catalog.gold.daily_category_summary" in (data.get("generated_sql") or ""), data
PY

  local after_second
  after_second="$(checkpoint_count)"
  if [[ "$after_second" -le "$after_first" ]]; then
    echo "Expected app_context.langgraph_checkpoints count to increase after follow-up. first=$after_first second=$after_second"
    return 1
  fi

  echo "Checkpoint count for $SESSION_ID: $after_second"
  echo "Official LangGraph checkpoints for $SESSION_ID: $(official_checkpoint_count)"
}

finish() {
  printf '\nAgent checkpoint tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
  if [[ "$FAILED" -gt 0 ]]; then
    echo "Failed tests:"
    printf ' - %s\n' "${FAILED_TESTS[@]}"
    exit 1
  fi
}

require_command curl
require_command python3
require_command docker

preflight_trino_catalog

run_test "LangGraph checkpoint is persisted by session_id/thread_id" test_checkpoint_created_and_increases

finish
