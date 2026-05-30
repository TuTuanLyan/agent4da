#!/usr/bin/env bash

set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
SESSION_ID="${SESSION_ID:-agent-semantic-metadata-test}"

TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0
FAILED_TESTS=()

run_test() {
  local name="$1"
  shift
  TOTAL=$((TOTAL + 1))
  echo "==> ${name}"
  if "$@"; then
    PASSED=$((PASSED + 1))
    echo "PASS: ${name}"
  else
    FAILED=$((FAILED + 1))
    FAILED_TESTS+=("${name}")
    echo "FAIL: ${name}" >&2
  fi
}

ask() {
  local question="$1"
  python3 - "$API_URL" "$SESSION_ID" "$question" <<'PY'
import json
import sys
import urllib.request

api_url, session_id, question = sys.argv[1:4]
payload = json.dumps({"session_id": session_id, "question": question}).encode()
request = urllib.request.Request(
    f"{api_url.rstrip('/')}/ask",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=90) as response:
    print(response.read().decode())
PY
}

semantic_available() {
  docker exec trino trino --execute "SHOW TABLES FROM iceberg_catalog.metadata" 2>/dev/null \
    | grep -q '"semantic_table_catalog"' \
    && docker exec trino trino --execute "SHOW TABLES FROM iceberg_catalog.metadata" 2>/dev/null \
    | grep -q '"semantic_column_catalog"'
}

test_runtime_semantic_metadata() {
  if semantic_available; then
    docker exec trino trino --execute "SELECT * FROM iceberg_catalog.metadata.semantic_table_catalog LIMIT 5" >/dev/null
    docker exec trino trino --execute "SELECT * FROM iceberg_catalog.metadata.semantic_column_catalog LIMIT 10" >/dev/null
    return 0
  fi

  echo "SKIP detail: Trino runtime has no iceberg_catalog.metadata semantic tables yet."
  SKIPPED=$((SKIPPED + 1))
  return 0
}

test_agent_metadata_business_question() {
  local response
  response="$(ask "Hệ thống có những metadata business nào?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
assert data.get("status") == "success", data
assert data.get("intent") == "metadata_business", data
metadata = data.get("metadata_used") or {}
assert "semantic_available" in metadata, metadata
assert "semantic_tables" in metadata, metadata
assert "semantic_columns" in metadata, metadata
if metadata.get("semantic_available"):
    assert data.get("metadata_source") == "semantic", data
    assert metadata.get("semantic_tables"), metadata
else:
    assert data.get("metadata_source") == "technical", data
    assert data.get("warnings"), data
PY
}

test_brand_revenue_uses_semantic_when_available() {
  local response
  response="$(ask "Brand nào có doanh thu cao nhất?")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
sql = data.get("generated_sql") or ""
sql_lower = sql.lower()
metadata = data.get("metadata_used") or {}
assert data.get("status") == "success", data
assert "iceberg_catalog.gold.daily_brand_summary" in sql_lower, sql
assert "postgresql.gold" not in sql_lower and "analytics_test" not in sql_lower, sql
if metadata.get("semantic_available"):
    assert data.get("metadata_source") == "semantic", data
    assert "group by" in sql_lower and "brand" in sql_lower, sql
    assert "sum(" in sql_lower and "revenue" in sql_lower, sql
else:
    assert data.get("metadata_source") == "technical", data
PY
}

test_metadata_service_fallback() {
  docker exec agent4da-api python - <<'PY'
import metadata_service

original_execute_query = metadata_service.execute_query

def fake_execute_query(sql):
    if "iceberg_catalog.metadata" in sql:
        raise RuntimeError("metadata unavailable in fallback test")
    return original_execute_query(sql)

metadata_service.execute_query = fake_execute_query
metadata_service._CACHE["metadata"] = None
metadata_service._CACHE["expires_at"] = 0.0
metadata = metadata_service.get_gold_metadata(refresh=True)
assert metadata["tables"], metadata
assert metadata["columns"], metadata
assert metadata["semantic_available"] is False, metadata
assert metadata["metadata_source"] == "technical", metadata
PY
}

echo "Preflight: checking API health"
python3 - "$API_URL" <<'PY'
import sys
import urllib.request

api_url = sys.argv[1].rstrip("/")
with urllib.request.urlopen(f"{api_url}/health", timeout=20) as response:
    body = response.read().decode()
assert "ok" in body.lower(), body
PY

run_test "Runtime semantic metadata availability" test_runtime_semantic_metadata
run_test "Agent business metadata question" test_agent_metadata_business_question
run_test "Brand revenue semantic context or technical fallback" test_brand_revenue_uses_semantic_when_available
run_test "metadata_service technical fallback" test_metadata_service_fallback

echo "Agent semantic metadata tests: ${PASSED} passed, ${FAILED} failed, ${SKIPPED} skipped detail checks."
if [ "$FAILED" -ne 0 ]; then
  printf 'Failed tests:\n' >&2
  printf -- '- %s\n' "${FAILED_TESTS[@]}" >&2
  exit 1
fi
