#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"

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
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent retry tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

test_one_attempt_correction_success() {
  docker exec -i agent4da-api python - <<'PY'
from sql_corrector import correct_sql
from sql_guard import validate_sql
from trino_client import execute_query

metadata = {
    "tables": ["daily_brand_summary"],
    "columns": {
        "daily_brand_summary": [
            {"name": "brand", "type": "varchar"},
            {"name": "revenue", "type": "decimal(18,2)"},
            {"name": "unique_events", "type": "bigint"},
        ],
    },
}

result = correct_sql(
    question="Brand nào có doanh thu cao nhất?",
    intent_result={"intent": "revenue_sales"},
    failed_sql=(
        "SELECT brand, total_revenue "
        "FROM iceberg_catalog.gold.daily_brand_summary "
        "ORDER BY total_revenue DESC LIMIT 10"
    ),
    error_message="Column 'total_revenue' cannot be resolved",
    table_candidates=["daily_brand_summary"],
    metadata_context=metadata,
    attempt_number=1,
)

corrected_sql = result["corrected_sql"]
assert result["can_retry"] is True, result
assert "revenue" in corrected_sql.lower(), result
assert "total_revenue" not in corrected_sql.lower(), result
assert "postgresql.gold" not in corrected_sql.lower(), result
assert "analytics_test" not in corrected_sql.lower(), result
validated_sql = validate_sql(corrected_sql)
columns, rows = execute_query(validated_sql)
assert rows, (columns, rows)
PY
}

test_dangerous_request_no_retry() {
  local response
  response="$(curl -sS -X POST "$API_URL/ask" \
    -H 'Content-Type: application/json' \
    -d '{"session_id":"test-agent-retry-001","question":"Drop bảng fact_events"}')"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
message = f"{data.get('answer') or ''} {data.get('error_message') or ''}".lower()
assert data.get("status") in ("blocked", "error"), data
assert data.get("retry_attempted") is False, data
assert data.get("retry_count", 0) == 0, data
assert not data.get("correction_history"), data
assert data.get("generated_sql") in ("", None), data
assert "chặn" in message or "không an toàn" in message or "blocked" in message, data
PY
}

test_max_retry_exceeded() {
  docker exec -i agent4da-api python - <<'PY'
from agent_graph import execute_sql_node
from sql_corrector import MAX_SQL_RETRY_ATTEMPTS, correct_sql
from sql_guard import validate_sql

bad_sql = "SELECT definitely_missing_metric FROM iceberg_catalog.gold.daily_brand_summary LIMIT 1"
validated_sql = validate_sql(bad_sql)
state = {
    "session_id": "test-agent-retry-max",
    "question": "Internal max retry exceeded test",
    "generated_sql": validated_sql,
    "retry_count": MAX_SQL_RETRY_ATTEMPTS,
    "retry_attempted": True,
    "retry_success": False,
}
state = execute_sql_node(state)
assert state["execute_ok"] is False, state
assert state["can_retry"] is False, state
assert state["status"] == "error", state
assert f"tối đa {MAX_SQL_RETRY_ATTEMPTS}" in state["error_message"], state
assert "postgresql.gold" not in state["generated_sql"].lower(), state
assert "analytics_test" not in state["generated_sql"].lower(), state

limit_result = correct_sql(
    question="Internal retry limit test",
    intent_result={"intent": "ranking"},
    failed_sql=bad_sql,
    error_message="Column 'definitely_missing_metric' cannot be resolved",
    table_candidates=["daily_brand_summary"],
    metadata_context={"tables": ["daily_brand_summary"], "columns": {"daily_brand_summary": []}},
    attempt_number=MAX_SQL_RETRY_ATTEMPTS + 1,
)
assert limit_result["can_retry"] is False, limit_result
assert str(MAX_SQL_RETRY_ATTEMPTS) in limit_result["correction_reason"], limit_result
PY
}

finish() {
  printf '\nAgent retry tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
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

run_test "Correction succeeds on first attempt" test_one_attempt_correction_success
run_test "Dangerous request does not retry" test_dangerous_request_no_retry
run_test "Max retry exceeded fails safely" test_max_retry_exceeded

finish
