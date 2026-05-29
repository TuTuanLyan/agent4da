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

payload() {
  local session_id="$1"
  local question="$2"
  SESSION_ID="$session_id" QUESTION="$question" python3 - <<'PY'
import json
import os

print(json.dumps({
    "session_id": os.environ["SESSION_ID"],
    "question": os.environ["QUESTION"],
}, ensure_ascii=False))
PY
}

ask() {
  local session_id="$1"
  local question="$2"
  local body
  body="$(payload "$session_id" "$question")"
  curl -sS -X POST "$API_URL/ask" \
    -H 'Content-Type: application/json' \
    -d "$body"
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
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent context tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

test_followup_category() {
  local session_id="test-agent-context-001"
  ask "$session_id" "Brand nào có nhiều event nhất?" >/dev/null
  local response
  response="$(ask "$session_id" "Thế category thì sao?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
chart = data.get("chart") or {}
assert data.get("status") == "success", data
assert data.get("context_used") is True, data
assert data.get("resolved_question"), data
assert data.get("intent") == "ranking", data
assert "iceberg_catalog.gold.daily_category_summary" in sql, sql
assert chart.get("type") == "bar", chart
assert "postgresql.gold" not in sql.lower(), sql
assert "analytics_test" not in sql.lower(), sql
PY
}

test_followup_product() {
  local session_id="test-agent-context-002"
  ask "$session_id" "Brand nào có nhiều event nhất?" >/dev/null
  local response
  response="$(ask "$session_id" "Thế sản phẩm thì sao?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
assert data.get("status") == "success", data
assert data.get("context_used") is True, data
assert data.get("intent") == "ranking", data
assert "iceberg_catalog.gold.daily_product_summary" in sql, sql
assert "postgresql.gold" not in sql.lower(), sql
assert "analytics_test" not in sql.lower(), sql
PY
}

test_followup_top_5() {
  local session_id="test-agent-context-003"
  ask "$session_id" "Top 10 brand có nhiều event nhất?" >/dev/null
  local response
  response="$(ask "$session_id" "Top 5 thôi")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os
import re

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
assert data.get("status") == "success", data
assert data.get("context_used") is True, data
assert data.get("intent") == "ranking", data
assert "iceberg_catalog.gold.daily_brand_summary" in sql, sql
assert re.search(r"\bLIMIT\s+5\b", sql, flags=re.IGNORECASE), sql
PY
}

test_chart_previous() {
  local session_id="test-agent-context-004"
  ask "$session_id" "Brand nào có nhiều event nhất?" >/dev/null
  local response
  response="$(ask "$session_id" "Vẽ biểu đồ câu trên")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
answer = (data.get("answer") or "").lower()
chart = data.get("chart") or {}
assert data.get("status") == "success", data
assert data.get("context_used") is True, data
assert chart.get("recommended") is True, chart
assert chart.get("type") == "bar", chart
assert "gợi ý biểu đồ" in answer or "goi y bieu do" in answer, answer
PY
}

test_explain_previous_sql() {
  local session_id="test-agent-context-005"
  ask "$session_id" "Brand nào có nhiều event nhất?" >/dev/null
  local response
  response="$(ask "$session_id" "Giải thích SQL vừa rồi")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
answer = (data.get("answer") or "").lower()
assert data.get("status") == "success", data
assert data.get("context_used") is True, data
assert data.get("intent") == "explain_sql", data
assert "iceberg_catalog.gold.daily_brand_summary" in data.get("generated_sql", ""), data
assert "sql" in answer and ("gold" in answer or "bảng" in answer or "bang" in answer), answer
PY
}

finish() {
  printf '\nAgent context tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
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

run_test "Follow-up category" test_followup_category
run_test "Follow-up product" test_followup_product
run_test "Follow-up top 5" test_followup_top_5
run_test "Chart previous result" test_chart_previous
run_test "Explain previous SQL" test_explain_previous_sql

finish
