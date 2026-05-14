#!/bin/bash
# =============================================================================
# submit_silver.sh - submit SilverEcommerceEventsJob manually.
#
# Use this to test/debug directly without triggering Airflow.
# Jars are mounted at /opt/project/jars/ in Spark containers.
# =============================================================================

set -e

JARS_DIR="/opt/project/jars"

# Driver/executor classpath uses colon-separated paths.
# Do not use --jars because /opt/project/jars is already mounted into Spark
# containers. Passing --jars makes Spark copy jars into log/spark/app-* on
# every run.
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
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=admin \
  -e MINIO_SECRET_KEY=Admin123! \
  -e MINIO_BUCKET_BRONZE=bronze \
  -e MINIO_BUCKET_SILVER=silver \
  -e SILVER_WRITE_MODE="${SILVER_WRITE_MODE:-append}" \
  spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-class-path "${CLASSPATH}" \
  --conf "spark.executor.extraClassPath=${CLASSPATH}" \
  --conf "spark.executorEnv.MINIO_ENDPOINT=http://minio:9000" \
  --conf "spark.executorEnv.MINIO_ACCESS_KEY=admin" \
  --conf "spark.executorEnv.MINIO_SECRET_KEY=Admin123!" \
  --conf "spark.executorEnv.MINIO_BUCKET_BRONZE=bronze" \
  --conf "spark.executorEnv.MINIO_BUCKET_SILVER=silver" \
  --conf "spark.executorEnv.SILVER_WRITE_MODE=${SILVER_WRITE_MODE:-append}" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.driver.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  --conf "spark.executor.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  /opt/spark/work/silver_job.py

echo "==> Done."
