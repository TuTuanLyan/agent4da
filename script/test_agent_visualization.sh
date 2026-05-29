#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
SESSION_ID="${SESSION_ID:-test-agent-visualization-001}"

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
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent visualization tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

test_ranking_bar_chart() {
  local response
  response="$(ask "$SESSION_ID-ranking" "Brand nào có nhiều event nhất?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
chart = data.get("chart") or {}
chart_data = data.get("chart_data") or []
assert data.get("status") == "success", data
assert chart.get("recommended") is True, chart
assert chart.get("type") == "bar", chart
assert data.get("chart_type") == "bar", data
assert chart_data and chart_data == chart.get("data"), data
assert all("x" in item and "y" in item for item in chart_data), chart_data
assert chart.get("x") == "brand", chart
assert chart.get("y") in ("unique_events", "total_events", "event_count"), chart
assert chart.get("columns", {}).get("x") == chart.get("x"), chart
assert chart.get("columns", {}).get("y") == chart.get("y"), chart
PY
}

test_overview_metrics_chart() {
  local response
  response="$(ask "$SESSION_ID-overview" "Có bao nhiêu lượt view và cart?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
chart = data.get("chart") or {}
chart_data = data.get("chart_data") or []
labels = {item.get("x") for item in chart_data}
assert data.get("status") == "success", data
assert chart.get("recommended") is True, chart
assert chart.get("type") == "bar", chart
assert data.get("chart_type") == "bar", data
assert chart.get("chart_data_mode") == "metrics_as_categories", chart
assert chart_data and chart_data == chart.get("data"), data
assert "total_views" in labels or "view_count" in labels, chart_data
assert "total_carts" in labels or "cart_count" in labels, chart_data
PY
}

test_trend_line_chart() {
  local response
  response="$(ask "$SESSION_ID-trend" "Số event theo ngày là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
chart = data.get("chart") or {}
chart_data = data.get("chart_data") or []
rows = data.get("rows") or []
assert data.get("status") == "success", data
if rows and any(any(key in row for key in ("event_date", "date", "day", "hour", "event_hour")) for row in rows):
    assert chart.get("recommended") is True, chart
    assert chart.get("type") == "line", chart
    assert data.get("chart_type") == "line", data
    assert chart_data and chart_data == chart.get("data"), data
    assert all("x" in item and "y" in item for item in chart_data), chart_data
else:
    assert chart.get("recommended") is False, chart
    assert data.get("chart_type") is None, data
    assert chart_data == [], data
PY
}

test_revenue_scalar_no_chart() {
  local response
  response="$(ask "$SESSION_ID-revenue" "Tổng doanh thu hiện tại là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
chart = data.get("chart") or {}
reason = (chart.get("reason") or "").lower()
assert data.get("status") == "success", data
assert chart.get("recommended") is False, chart
assert data.get("chart_type") is None, data
assert data.get("chart_data") == [], data
assert "kpi" in reason or "scalar" in reason or "0" in reason or "doanh thu" in reason, chart
PY
}

test_pie_support_internal() {
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code/api python3 - <<'PY'
from chart_recommender import normalize_pie_data, recommend_chart

rows = [
    {"event_type": "view", "event_count": 971},
    {"event_type": "cart", "event_count": 28},
]
pie_data = normalize_pie_data(rows, "event_type", "event_count", limit=6)
assert pie_data == [
    {"label": "view", "value": 971},
    {"label": "cart", "value": 28},
], pie_data

chart = recommend_chart(
    question="Tỷ trọng event type hiện tại là gì?",
    intent="breakdown",
    rows=rows,
    row_count=len(rows),
    generated_sql="SELECT event_type, event_count FROM iceberg_catalog.gold.fact_events",
    table_candidates=["fact_events"],
    used_tables=["iceberg_catalog.gold.fact_events"],
    warnings=[],
)
assert chart["recommended"] is True, chart
assert chart["type"] == "pie", chart
assert chart["data"] == pie_data, chart
assert chart["columns"]["label"] == "event_type", chart
assert chart["columns"]["value"] == "event_count", chart
PY
}

test_previous_chart_context() {
  local session_id="$SESSION_ID-previous"
  ask "$session_id" "Brand nào có nhiều event nhất?" >/dev/null
  local response
  response="$(ask "$session_id" "Vẽ biểu đồ câu trên")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
chart = data.get("chart") or {}
chart_data = data.get("chart_data") or []
assert data.get("status") == "success", data
assert data.get("context_used") is True, data
assert chart.get("recommended") is True, chart
assert data.get("chart_type") == chart.get("type"), data
assert chart_data and chart_data == chart.get("data"), data
assert all("x" in item and "y" in item for item in chart_data), chart_data
PY
}

finish() {
  printf '\nAgent visualization tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
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

run_test "Ranking bar chart data" test_ranking_bar_chart
run_test "Overview metrics chart data" test_overview_metrics_chart
run_test "Trend line chart data" test_trend_line_chart
run_test "Revenue scalar has no chart" test_revenue_scalar_no_chart
run_test "Pie chart support internal" test_pie_support_internal
run_test "Previous chart context returns chart data" test_previous_chart_context

finish
