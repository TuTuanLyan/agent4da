#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
SESSION_ID="${SESSION_ID:-agent-regression-test}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

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

json_payload() {
  local question="$1"
  QUESTION="$question" SESSION_ID="$SESSION_ID" python3 - <<'PY'
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
  local payload
  payload="$(json_payload "$question")"
  curl -sS -X POST "$API_URL/ask" \
    -H 'Content-Type: application/json' \
    -d "$payload"
}

assert_no_disallowed_sql() {
  local sql="$1"
  SQL="$sql" python3 - <<'PY'
import os

sql = (os.environ.get("SQL") or "").lower()
blocked = ("postgresql.gold", "analytics_test", "bronze", "silver")
for token in blocked:
    if token in sql:
        raise SystemExit(f"Generated SQL contains disallowed token: {token}")
PY
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
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent regression tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

test_health() {
  local response
  response="$(curl -sS "$API_URL/health")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
assert data.get("status") == "ok", data
PY
}

test_metadata_tables() {
  local response
  response="$(ask "Hệ thống có những bảng Gold nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
assert data.get("status") == "success", data
assert data.get("intent") == "metadata_tables", data
assert data.get("row_count", 0) >= 1, data
assert "SHOW TABLES FROM iceberg_catalog.gold" in sql, sql
assert "postgresql.gold" not in sql.lower(), sql
assert "analytics_test" not in sql.lower(), sql
assert data.get("chart", {}).get("recommended") is False, data.get("chart")
PY
}

test_metadata_columns() {
  local response
  response="$(ask "Bảng daily_brand_summary có những cột nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
rows = data.get("rows") or []
assert data.get("status") == "success", data
assert data.get("intent") == "metadata_columns", data
assert rows, data
assert all("column_name" in row and "data_type" in row for row in rows), rows[:3]
assert data.get("chart", {}).get("recommended") is False, data.get("chart")
PY
}

test_ranking() {
  local response
  response="$(ask "Brand nào có nhiều event nhất?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
answer = data.get("answer") or ""
chart = data.get("chart") or {}
assert data.get("status") == "success", data
assert data.get("intent") == "ranking", data
assert "iceberg_catalog.gold.daily_brand_summary" in sql, sql
assert "postgresql.gold" not in sql.lower(), sql
assert "analytics_test" not in sql.lower(), sql
assert "đứng đầu" in answer.lower() or "dung dau" in answer.lower(), answer
assert chart.get("recommended") is True, chart
assert chart.get("type") == "bar", chart
assert data.get("retry_attempted") is False, data
PY
}

test_overview() {
  local response
  response="$(ask "Có bao nhiêu lượt view và cart?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
rows = data.get("rows") or []
answer = (data.get("answer") or "").lower()
assert data.get("status") == "success", data
assert data.get("intent") == "metric_overview", data
assert "view" in answer and "cart" in answer, answer
assert rows, data
assert any("total_views" in row or "total_carts" in row for row in rows), rows
assert "chart" in data, data
PY
}

test_revenue_caveat() {
  local response
  response="$(ask "Tổng doanh thu hiện tại là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
rows = data.get("rows") or []
warnings = data.get("warnings") or []
answer = (data.get("answer") or "").lower()
chart = data.get("chart") or {}
assert data.get("status") == "success", data
assert data.get("intent") == "revenue_sales", data
assert "doanh thu" in answer or "revenue" in answer, answer

revenue_values = []
for row in rows:
    for column_name, value in row.items():
        if "revenue" in column_name.lower() or "amount" in column_name.lower():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                revenue_values.append(value)

if revenue_values and all(value == 0 for value in revenue_values):
    assert warnings, data
    if len(rows) == 1:
        assert chart.get("recommended") is False, chart
PY
}

test_unsupported() {
  local response
  response="$(ask "Thời tiết Hà Nội hôm nay thế nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
answer = (data.get("answer") or "").lower()
generated_sql = data.get("generated_sql")
assert data.get("intent") == "unsupported", data
assert generated_sql in ("", None), data
assert "ngoài phạm vi" in answer or "ngoai pham vi" in answer, answer
assert "e-commerce" in answer and "gold" in answer, answer
PY
}

test_dangerous_blocked() {
  local response
  response="$(ask "Drop bảng fact_events")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
message = f"{data.get('answer') or ''} {data.get('error_message') or ''}".lower()
assert data.get("status") in ("blocked", "error"), data
assert data.get("retry_attempted") is False, data
assert data.get("generated_sql") in ("", None), data
assert "chặn" in message or "không an toàn" in message or "blocked" in message, data
PY
}

test_forced_sql_correction() {
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$ROOT_DIR/code/api" python3 - <<'PY'
from sql_corrector import correct_sql

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
assert " revenue" in corrected_sql.lower(), result
assert "total_revenue" not in corrected_sql.lower(), result
assert "postgresql.gold" not in corrected_sql.lower(), result
assert "analytics_test" not in corrected_sql.lower(), result
assert "iceberg_catalog.gold.daily_brand_summary" in corrected_sql, result
PY
}

finish() {
  printf '\nAgent regression tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
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

run_test "Health check" test_health
run_test "Metadata tables" test_metadata_tables
run_test "Metadata columns" test_metadata_columns
run_test "Ranking" test_ranking
run_test "Overview" test_overview
run_test "Revenue caveat" test_revenue_caveat
run_test "Unsupported question" test_unsupported
run_test "Dangerous request blocked" test_dangerous_blocked
run_test "Forced SQL correction" test_forced_sql_correction

finish
