#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
SESSION_ID="${SESSION_ID:-test-agent-nlu-001}"

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
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent NLU tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

assert_no_disallowed_sql() {
  local sql="$1"
  SQL="$sql" python3 - <<'PY'
import os

sql = (os.environ.get("SQL") or "").lower()
for token in ("postgresql.gold", "analytics_test", "bronze", "silver"):
    assert token not in sql, f"Generated SQL contains disallowed token: {token}"
PY
}

test_top_k() {
  local response
  response="$(ask "$SESSION_ID-topk" "Top 5 brand có nhiều event nhất")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os
import re

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
assert data.get("status") == "success", data
assert data.get("intent") == "ranking", data
assert data.get("analysis_type") == "topk", data
assert data.get("metric") == "total_events", data
assert data.get("comparison_entities") == [], data
assert data.get("time_grain") is None, data
assert data.get("sort_direction") == "desc", data
assert data.get("nlu_confidence") in ("high", "medium"), data
assert data.get("table_candidates") == ["daily_brand_summary"], data
assert "iceberg_catalog.gold.daily_brand_summary" in sql, sql
assert re.search(r"\bLIMIT\s+5\b", sql, re.I), sql
PY
  assert_no_disallowed_sql "$(RESPONSE="$response" python3 - <<'PY'
import json, os
print(json.loads(os.environ["RESPONSE"]).get("generated_sql") or "")
PY
)"
}

test_trend_by_day() {
  local response
  response="$(ask "$SESSION_ID-trend" "Số event theo ngày là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
assert data.get("status") == "success", data
assert data.get("intent") == "trend", data
assert data.get("analysis_type") == "time_series", data
assert data.get("time_grain") == "day", data
assert data.get("metric") == "total_events", data
assert data.get("chart_type") == "line", data
assert "iceberg_catalog.gold.daily_event_summary" in sql, sql
PY
}

test_revenue_trend() {
  local response
  response="$(ask "$SESSION_ID-revenue-trend" "Doanh thu theo ngày là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
rows = data.get("rows") or []
warnings = data.get("warnings") or []
assert data.get("status") == "success", data
assert data.get("intent") in ("trend", "revenue_sales"), data
assert data.get("analysis_type") == "time_series", data
assert data.get("metric") == "revenue", data
assert data.get("time_grain") == "day", data
assert "iceberg_catalog.gold.daily_event_summary" in sql, sql
revenue_values = []
for row in rows:
    for key, value in row.items():
        if "revenue" in key.lower() and isinstance(value, (int, float)) and not isinstance(value, bool):
            revenue_values.append(value)
if revenue_values and all(value == 0 for value in revenue_values):
    assert warnings, data
PY
}

test_comparison() {
  local response
  response="$(ask "$SESSION_ID-comparison" "So sánh apple và samsung theo số event")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = (data.get("generated_sql") or "").lower()
assert data.get("status") == "success", data
assert data.get("intent") == "comparison", data
assert data.get("analysis_type") == "comparison", data
assert data.get("dimension") == "brand", data
assert data.get("metric") == "total_events", data
assert data.get("comparison_entities") == ["apple", "samsung"], data
assert data.get("filters") and data["filters"][0]["values"] == ["apple", "samsung"], data
assert "iceberg_catalog.gold.daily_brand_summary" in sql, sql
assert "apple" in sql and "samsung" in sql, sql
assert "postgresql.gold" not in sql and "analytics_test" not in sql, sql
PY
}

test_breakdown() {
  local response
  response="$(ask "$SESSION_ID-breakdown" "Cơ cấu view và cart hiện tại như thế nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
chart = data.get("chart") or {}
chart_data = data.get("chart_data") or []
assert data.get("status") == "success", data
assert data.get("intent") in ("breakdown", "metric_overview"), data
assert data.get("analysis_type") in ("breakdown", "overview"), data
assert data.get("chart_type") in ("bar", "pie"), data
assert chart_data, data
assert chart.get("type") == data.get("chart_type"), data
if chart.get("type") == "bar":
    assert "pie" in (chart.get("alternative_types") or []), chart
    labels = {item.get("x") for item in chart_data}
    assert "total_views" in labels and "total_carts" in labels, chart_data
else:
    assert all("label" in item and "value" in item for item in chart_data), chart_data
PY
}

test_unsupported() {
  local response
  response="$(ask "$SESSION_ID-unsupported" "Thời tiết Hà Nội hôm nay thế nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
assert data.get("intent") == "unsupported", data
assert data.get("analysis_type") == "unsupported", data
assert data.get("generated_sql") in ("", None), data
PY
}

finish() {
  printf '\nAgent NLU tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
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

run_test "Top K NLU extraction" test_top_k
run_test "Trend by day NLU extraction" test_trend_by_day
run_test "Revenue trend NLU extraction" test_revenue_trend
run_test "Comparison NLU extraction" test_comparison
run_test "Breakdown NLU extraction" test_breakdown
run_test "Unsupported outside domain" test_unsupported

finish
