#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
SESSION_ID="${SESSION_ID:-test-agent-llm-insight-001}"

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
    echo "Trino missing iceberg_catalog. Restore trino/etc/catalog/iceberg_catalog.properties before running Agent LLM insight tests."
    exit 1
  fi
  echo "Preflight passed: iceberg_catalog is available"
}

test_ranking_insight() {
  local response
  response="$(ask "Brand nào có nhiều event nhất?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
rows = data.get("rows") or []
answer = data.get("answer") or ""
insights = data.get("insights") or []
source = data.get("insight_source")
print(f"Insight source: {source}")
if source == "rule_based":
    print("LLM insight fallback used for ranking.")

assert data.get("status") == "success", data
assert data.get("intent") == "ranking", data
assert source in ("llm", "rule_based"), data
assert isinstance(data.get("llm_insight_used"), bool), data
assert insights, data
assert rows, data

top_row = rows[0]
brand = str(top_row.get("brand") or "")
metric_value = None
for key, value in top_row.items():
    if key.endswith("_id") or key == "id":
        continue
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        metric_value = value
        break

assert brand and brand.lower() in answer.lower(), (answer, top_row)
assert metric_value is not None and str(metric_value) in answer, (answer, top_row)
assert "trong dữ liệu hiện tại" in answer.lower(), answer
PY
}

test_overview_insight() {
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
assert data.get("insight_source") in ("llm", "rule_based"), data
assert rows, data
row = rows[0]
assert "view" in answer and "cart" in answer, answer
if "total_views" in row:
    assert str(row["total_views"]) in answer, (answer, row)
if "total_carts" in row:
    assert str(row["total_carts"]) in answer, (answer, row)
assert "trong dữ liệu hiện tại" in answer, answer
PY
}

test_revenue_caveat_insight() {
  local response
  response="$(ask "Tổng doanh thu hiện tại là bao nhiêu?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
rows = data.get("rows") or []
warnings = data.get("warnings") or []
answer = (data.get("answer") or "").lower()
combined = " ".join([answer, *[str(item).lower() for item in data.get("insights") or []]])
assert data.get("status") == "success", data
assert data.get("intent") == "revenue_sales", data
assert data.get("insight_source") in ("llm", "rule_based"), data
assert rows, data
assert "doanh thu" in answer or "revenue" in answer, answer

revenue_values = []
for row in rows:
    for column_name, value in row.items():
        if "revenue" in column_name.lower() or "amount" in column_name.lower():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                revenue_values.append(value)

if revenue_values and all(value == 0 for value in revenue_values):
    assert warnings, data
    assert "0" in answer, answer
    assert "purchase" in combined or "doanh thu đang bằng 0" in combined or "fact_sales" in combined, data
PY
}

test_metadata_rule_based() {
  local response
  response="$(ask "Hệ thống có những bảng Gold nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
assert data.get("status") == "success", data
assert data.get("intent") == "metadata_tables", data
assert data.get("insight_source") == "rule_based", data
assert data.get("llm_insight_used") is False, data
assert data.get("row_count", 0) >= 1, data
assert data.get("rows"), data
PY
}

finish() {
  printf '\nAgent LLM insight tests: %d passed, %d failed, %d total.\n' "$PASSED" "$FAILED" "$TOTAL"
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

run_test "Ranking LLM insight or fallback" test_ranking_insight
run_test "Overview LLM insight or fallback" test_overview_insight
run_test "Revenue caveat insight" test_revenue_caveat_insight
run_test "Metadata remains rule-based" test_metadata_rule_based

finish
