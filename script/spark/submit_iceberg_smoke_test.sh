#!/usr/bin/env bash
# Submit IcebergSmokeTest manually from the host via the Airflow container.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=script/spark/load_env.sh
. "${SCRIPT_DIR}/load_env.sh"

require_env MINIO_ACCESS_KEY
require_env MINIO_SECRET_KEY
require_env ICEBERG_JDBC_USER
require_env ICEBERG_JDBC_PASSWORD

SPARK_SUBMIT_CONTAINER="${SPARK_SUBMIT_CONTAINER:-airflow}"
SPARK_DRIVER_PYTHON="${SPARK_DRIVER_PYTHON:-/usr/local/bin/python3}"
SPARK_EXECUTOR_PYTHON="${SPARK_EXECUTOR_PYTHON:-/usr/bin/python3}"
ICEBERG_CATALOG_NAME="${ICEBERG_CATALOG_NAME:-iceberg_catalog}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3a://gold/warehouse/}"
ICEBERG_JDBC_URI="${ICEBERG_JDBC_URI:-jdbc:postgresql://postgres-db:5432/agent4da}"
ICEBERG_JDBC_SCHEMA="${ICEBERG_JDBC_SCHEMA:-iceberg}"

JARS_DIR="/opt/project/jars"
JARS=(
  "${JARS_DIR}/org.apache.hadoop_hadoop-aws-3.4.2.jar"
  "${JARS_DIR}/org.apache.hadoop_hadoop-client-api-3.4.2.jar"
  "${JARS_DIR}/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar"
  "${JARS_DIR}/software.amazon.awssdk_bundle-2.29.52.jar"
  "${JARS_DIR}/org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar"
  "${JARS_DIR}/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar"
  "${JARS_DIR}/org.apache.kafka_kafka-clients-3.9.1.jar"
  "${JARS_DIR}/org.apache.commons_commons-pool2-2.12.1.jar"
  "${JARS_DIR}/org.lz4_lz4-java-1.8.0.jar"
  "${JARS_DIR}/org.xerial.snappy_snappy-java-1.1.10.8.jar"
  "${JARS_DIR}/org.slf4j_slf4j-api-2.0.17.jar"
  "${JARS_DIR}/org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar"
  "${JARS_DIR}/iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
  "${JARS_DIR}/postgresql-42.7.4.jar"
)

join_by() {
  local delimiter="$1"
  shift
  local first="$1"
  shift
  printf "%s" "${first}"
  printf "%s" "${@/#/${delimiter}}"
}

CLASSPATH="$(join_by ":" "${JARS[@]}")"

echo "==> Submitting IcebergSmokeTest from container ${SPARK_SUBMIT_CONTAINER} ..."

docker exec \
  -e MINIO_ENDPOINT="${MINIO_ENDPOINT}" \
  -e MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" \
  -e MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
  -e ICEBERG_CATALOG_NAME="${ICEBERG_CATALOG_NAME}" \
  -e ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE}" \
  -e ICEBERG_JDBC_URI="${ICEBERG_JDBC_URI}" \
  -e ICEBERG_JDBC_USER="${ICEBERG_JDBC_USER}" \
  -e ICEBERG_JDBC_PASSWORD="${ICEBERG_JDBC_PASSWORD}" \
  -e ICEBERG_JDBC_SCHEMA="${ICEBERG_JDBC_SCHEMA}" \
  "${SPARK_SUBMIT_CONTAINER}" \
  /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --driver-class-path "${CLASSPATH}" \
  --conf "spark.executor.extraClassPath=${CLASSPATH}" \
  --conf "spark.pyspark.python=${SPARK_EXECUTOR_PYTHON}" \
  --conf "spark.pyspark.driver.python=${SPARK_DRIVER_PYTHON}" \
  --conf "spark.executorEnv.PYSPARK_PYTHON=${SPARK_EXECUTOR_PYTHON}" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}.uri=${ICEBERG_JDBC_URI}" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}.jdbc.user=${ICEBERG_JDBC_USER}" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}.jdbc.password=${ICEBERG_JDBC_PASSWORD}" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}.jdbc.currentSchema=${ICEBERG_JDBC_SCHEMA}" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}.warehouse=${ICEBERG_WAREHOUSE}" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG_NAME}.io-impl=org.apache.iceberg.hadoop.HadoopFileIO" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}" \
  --conf "spark.driver.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  --conf "spark.executor.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  --conf "spark.executorEnv.MINIO_ENDPOINT=${MINIO_ENDPOINT}" \
  --conf "spark.executorEnv.MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}" \
  --conf "spark.executorEnv.MINIO_SECRET_KEY=${MINIO_SECRET_KEY}" \
  --conf "spark.executorEnv.ICEBERG_CATALOG_NAME=${ICEBERG_CATALOG_NAME}" \
  --conf "spark.executorEnv.ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE}" \
  --conf "spark.executorEnv.ICEBERG_JDBC_URI=${ICEBERG_JDBC_URI}" \
  --conf "spark.executorEnv.ICEBERG_JDBC_USER=${ICEBERG_JDBC_USER}" \
  --conf "spark.executorEnv.ICEBERG_JDBC_PASSWORD=${ICEBERG_JDBC_PASSWORD}" \
  --conf "spark.executorEnv.ICEBERG_JDBC_SCHEMA=${ICEBERG_JDBC_SCHEMA}" \
  /opt/project/code/spark/tools/iceberg_smoke_test.py

echo "==> Done."
