#!/usr/bin/env bash
# Submit SilverEcommerceEventsJob manually.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=script/spark/load_env.sh
. "${SCRIPT_DIR}/load_env.sh"

require_env MINIO_ACCESS_KEY
require_env MINIO_SECRET_KEY

SPARK_SUBMIT_CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"
SPARK_DRIVER_PYTHON_IN_SPARK="${SPARK_DRIVER_PYTHON_IN_SPARK:-/usr/bin/python3}"
SPARK_EXECUTOR_PYTHON="${SPARK_EXECUTOR_PYTHON:-/usr/bin/python3}"
SILVER_WRITE_MODE="${SILVER_WRITE_MODE:-append}"

JARS_DIR="/opt/project/jars"
CLASSPATH="${JARS_DIR}/org.apache.hadoop_hadoop-aws-3.4.2.jar:\
${JARS_DIR}/org.apache.hadoop_hadoop-client-api-3.4.2.jar:\
${JARS_DIR}/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar:\
${JARS_DIR}/software.amazon.awssdk_bundle-2.29.52.jar:\
${JARS_DIR}/org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar:\
${JARS_DIR}/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar:\
${JARS_DIR}/org.apache.kafka_kafka-clients-3.9.1.jar:\
${JARS_DIR}/org.apache.commons_commons-pool2-2.12.1.jar:\
${JARS_DIR}/org.lz4_lz4-java-1.8.0.jar:\
${JARS_DIR}/org.xerial.snappy_snappy-java-1.1.10.8.jar:\
${JARS_DIR}/org.slf4j_slf4j-api-2.0.17.jar:\
${JARS_DIR}/org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar"

echo "==> Submitting SilverEcommerceEventsJob ..."

docker exec \
  -e MINIO_ENDPOINT="${MINIO_ENDPOINT}" \
  -e MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" \
  -e MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
  -e MINIO_BUCKET_BRONZE="${MINIO_BUCKET_BRONZE}" \
  -e MINIO_BUCKET_SILVER="${MINIO_BUCKET_SILVER}" \
  -e SILVER_WRITE_MODE="${SILVER_WRITE_MODE}" \
  "${SPARK_SUBMIT_CONTAINER}" \
  /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --driver-class-path "${CLASSPATH}" \
  --conf "spark.executor.extraClassPath=${CLASSPATH}" \
  --conf "spark.pyspark.python=${SPARK_EXECUTOR_PYTHON}" \
  --conf "spark.pyspark.driver.python=${SPARK_DRIVER_PYTHON_IN_SPARK}" \
  --conf "spark.executorEnv.PYSPARK_PYTHON=${SPARK_EXECUTOR_PYTHON}" \
  --conf "spark.executorEnv.MINIO_ENDPOINT=${MINIO_ENDPOINT}" \
  --conf "spark.executorEnv.MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}" \
  --conf "spark.executorEnv.MINIO_SECRET_KEY=${MINIO_SECRET_KEY}" \
  --conf "spark.executorEnv.MINIO_BUCKET_BRONZE=${MINIO_BUCKET_BRONZE}" \
  --conf "spark.executorEnv.MINIO_BUCKET_SILVER=${MINIO_BUCKET_SILVER}" \
  --conf "spark.executorEnv.SILVER_WRITE_MODE=${SILVER_WRITE_MODE}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}" \
  --conf "spark.driver.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  --conf "spark.executor.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  /opt/spark/work/silver_job.py

echo "==> Done."
