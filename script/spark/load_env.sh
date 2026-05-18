#!/usr/bin/env bash
# Load local env files for manual Spark submit scripts.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

load_env_file() {
  local file_path="$1"
  if [ -f "${file_path}" ]; then
    set -a
    # shellcheck source=/dev/null
    . "${file_path}"
    set +a
  fi
}

require_env() {
  local name="$1"
  local value="${!name:-}"
  if [ -z "${value}" ]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 1
  fi
}

load_env_file "${PROJECT_ROOT}/envs/minio.env"
load_env_file "${PROJECT_ROOT}/envs/iceberg.env"
load_env_file "${PROJECT_ROOT}/envs/spark.env"
load_env_file "${PROJECT_ROOT}/envs/airflow.env"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_BUCKET_BRONZE="${MINIO_BUCKET_BRONZE:-bronze}"
MINIO_BUCKET_SILVER="${MINIO_BUCKET_SILVER:-silver}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

