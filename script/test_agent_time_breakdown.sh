#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
SESSION_ID="${SESSION_ID:-test-agent-time-breakdown-001}"

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
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent time/breakdown tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

assert_gold_only_sql() {
  local sql="$1"
  SQL="$sql" python3 - <<'PY'
import os

sql = (os.environ.get("SQL") or "").lower()
assert "iceberg_catalog.gold" in sql, sql
for token in ("postgresql.gold", "analytics_test", "bronze", "silver"):
    assert token not in sql, f"Generated SQL contains disallowed token: {token}"
PY
}

test_latest_day_event_count() {
  local response
  response="$(ask "$SESSION_ID-latest-event" "Số event trong ngày gần nhất là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = (data.get("generated_sql") or "").lower()
time_range = data.get("time_range") or {}
applied = data.get("applied_time_filter") or {}
assert data.get("status") == "success", data
assert data.get("intent") == "metric_overview", data
assert time_range.get("type") == "latest", data
assert applied.get("type") == "latest", data
assert "iceberg_catalog.gold.daily_event_summary" in sql, sql
assert "max(event_date)" in sql, sql
PY
  assert_gold_only_sql "$(RESPONSE="$response" python3 - <<'PY'
import json, os
print(json.loads(os.environ["RESPONSE"]).get("generated_sql") or "")
PY
)"
}

test_latest_day_brand_ranking() {
  local response
  response="$(ask "$SESSION_ID-latest-brand" "Brand nào có nhiều event nhất trong ngày gần nhất?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = (data.get("generated_sql") or "").lower()
assert data.get("status") == "success", data
assert data.get("intent") == "ranking", data
assert data.get("dimension") == "brand", data
assert (data.get("time_range") or {}).get("type") == "latest", data
assert (data.get("applied_time_filter") or {}).get("type") == "latest", data
assert "iceberg_catalog.gold.daily_brand_summary" in sql, sql
assert "max(event_date)" in sql, sql
PY
}

test_exact_date() {
  local response
  response="$(ask "$SESSION_ID-exact-date" "Số event trong ngày 2020-01-01 là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = (data.get("generated_sql") or "").lower()
time_range = data.get("time_range") or {}
assert data.get("status") == "success", data
assert time_range.get("type") == "exact_date", data
assert time_range.get("start") == "2020-01-01", data
assert "date '2020-01-01'" in sql, sql
assert "iceberg_catalog.gold.daily_event_summary" in sql, sql
PY
}

test_trend_by_day_no_latest_filter() {
  local response
  response="$(ask "$SESSION_ID-trend-day" "Số event theo ngày là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = (data.get("generated_sql") or "").lower()
assert data.get("status") == "success", data
assert data.get("intent") == "trend", data
assert data.get("time_grain") == "day", data
assert data.get("time_range") in (None, {}), data
assert data.get("applied_time_filter") in (None, {}), data
assert data.get("chart_type") == "line", data
assert "max(event_date)" not in sql, sql
assert "date '2020-01-01'" not in sql, sql
PY
}

test_breakdown_prefers_pie() {
  local response
  response="$(ask "$SESSION_ID-breakdown-pie" "Cơ cấu view và cart hiện tại như thế nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
chart = data.get("chart") or {}
chart_data = data.get("chart_data") or []
assert data.get("status") == "success", data
assert data.get("intent") == "breakdown", data
assert data.get("analysis_type") == "breakdown", data
assert chart.get("recommended") is True, chart
assert data.get("chart_type") == "pie", data
assert chart.get("type") == "pie", chart
assert chart_data and chart_data == chart.get("data"), data
assert all("label" in item and "value" in item for item in chart_data), chart_data
assert sum(item["value"] for item in chart_data) > 0, chart_data
PY
}

test_today_safe_filter() {
  local response
  response="$(ask "$SESSION_ID-today" "Số event hôm nay là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = (data.get("generated_sql") or "").lower()
time_range = data.get("time_range") or {}
assert data.get("status") == "success", data
assert time_range.get("type") == "today", data
assert (data.get("applied_time_filter") or {}).get("type") == "today", data
assert "current_date" in sql, sql
assert "2026-05-28" not in sql, sql
if data.get("row_count") == 0:
    assert data.get("warnings") or "không có dữ liệu" in (data.get("answer") or "").lower(), data
PY
}

finish() {
  printf '\nAgent time/breakdown tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
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

run_test "Latest day event count uses MAX(event_date)" test_latest_day_event_count
run_test "Latest day brand ranking uses date filter" test_latest_day_brand_ranking
run_test "Exact date uses DATE literal" test_exact_date
run_test "Trend by day does not force latest filter" test_trend_by_day_no_latest_filter
run_test "Breakdown prefers pie chart" test_breakdown_prefers_pie
run_test "Today uses safe CURRENT_DATE filter" test_today_safe_filter

finish
