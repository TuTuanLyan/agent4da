#!/usr/bin/env bash
# Download one-time local JAR dependencies for Spark + Iceberg Stage 1.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JARS_DIR="${PROJECT_ROOT}/jars"
MAVEN_CENTRAL="https://repo1.maven.org/maven2"

ICEBERG_VERSION="1.10.1"
ICEBERG_JAR="iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar"
ICEBERG_URL="${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/${ICEBERG_JAR}"

POSTGRES_VERSION="42.7.4"
POSTGRES_JAR="postgresql-${POSTGRES_VERSION}.jar"
POSTGRES_URL="${MAVEN_CENTRAL}/org/postgresql/postgresql/${POSTGRES_VERSION}/${POSTGRES_JAR}"

log() {
  echo "[download_iceberg_jars] $*"
}

download_if_missing() {
  local file_name="$1"
  local url="$2"
  local target="${JARS_DIR}/${file_name}"

  if [[ -f "${target}" ]]; then
    log "Skip existing ${target}"
    return
  fi

  log "Downloading ${file_name}"
  log "From: ${url}"

  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --connect-timeout 20 -o "${target}.tmp" "${url}"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "${target}.tmp" "${url}"
  else
    log "ERROR: curl or wget is required."
    exit 1
  fi

  mv "${target}.tmp" "${target}"
  log "Saved ${target}"
}

log "Project root: ${PROJECT_ROOT}"
mkdir -p "${JARS_DIR}"
log "JAR directory: ${JARS_DIR}"

download_if_missing "${ICEBERG_JAR}" "${ICEBERG_URL}"
download_if_missing "${POSTGRES_JAR}" "${POSTGRES_URL}"

log "Done."
