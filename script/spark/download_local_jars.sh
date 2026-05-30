#!/usr/bin/env bash
# Download the local Spark dependency bundle expected by Airflow and Spark.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JARS_DIR="${PROJECT_ROOT}/jars"
MAVEN_CENTRAL="${MAVEN_CENTRAL:-https://repo1.maven.org/maven2}"

log() {
  echo "[download_local_jars] $*"
}

download_if_missing() {
  local file_name="$1"
  local path="$2"
  local target="${JARS_DIR}/${file_name}"
  local url="${MAVEN_CENTRAL}/${path}"

  if [[ -s "${target}" ]]; then
    log "Skip existing ${file_name}"
    return
  fi

  log "Downloading ${file_name}"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --connect-timeout 20 -o "${target}.tmp" "${url}"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "${target}.tmp" "${url}"
  else
    log "ERROR: curl or wget is required."
    exit 1
  fi
  mv "${target}.tmp" "${target}"
}

mkdir -p "${JARS_DIR}"
log "JAR directory: ${JARS_DIR}"

download_if_missing "org.apache.hadoop_hadoop-aws-3.4.2.jar" \
  "org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar"
download_if_missing "org.apache.hadoop_hadoop-client-api-3.4.2.jar" \
  "org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.jar"
download_if_missing "org.apache.hadoop_hadoop-client-runtime-3.4.2.jar" \
  "org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.jar"
download_if_missing "software.amazon.awssdk_bundle-2.29.52.jar" \
  "software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar"
download_if_missing "org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar" \
  "org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar"
download_if_missing "org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar" \
  "org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.1/spark-token-provider-kafka-0-10_2.13-4.1.1.jar"
download_if_missing "org.apache.kafka_kafka-clients-3.9.1.jar" \
  "org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar"
download_if_missing "org.apache.commons_commons-pool2-2.12.1.jar" \
  "org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar"
download_if_missing "org.lz4_lz4-java-1.8.0.jar" \
  "org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar"
download_if_missing "org.xerial.snappy_snappy-java-1.1.10.8.jar" \
  "org/xerial/snappy/snappy-java/1.1.10.8/snappy-java-1.1.10.8.jar"
download_if_missing "org.slf4j_slf4j-api-2.0.17.jar" \
  "org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar"
download_if_missing "org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar" \
  "org/scala-lang/modules/scala-parallel-collections_2.13/1.2.0/scala-parallel-collections_2.13-1.2.0.jar"
download_if_missing "iceberg-spark-runtime-4.0_2.13-1.10.1.jar" \
  "org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
download_if_missing "postgresql-42.7.4.jar" \
  "org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"

log "Done."
