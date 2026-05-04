#!/bin/bash
# =============================================================================
# submit_bronze.sh — submit BronzeBatchJob thủ công (bypass Airflow)
#
# Dùng để test/debug trực tiếp mà không cần trigger DAG.
# Jars mount tại /opt/project/jars/ trong spark-master và spark-worker.
#
# FIX: --driver-class-path dùng ":" (Linux path separator), KHÔNG phải ","
#      --jars dùng "," (danh sách distribute — khác với classpath)
# =============================================================================

set -e

JARS_DIR="/opt/project/jars"

# Danh sách jars để distribute đến executor (--jars, separator = ",")
JARS_LIST="${JARS_DIR}/org.apache.hadoop_hadoop-aws-3.4.2.jar,\
${JARS_DIR}/org.apache.hadoop_hadoop-client-api-3.4.2.jar,\
${JARS_DIR}/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar,\
${JARS_DIR}/software.amazon.awssdk_bundle-2.29.52.jar,\
${JARS_DIR}/org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar,\
${JARS_DIR}/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar,\
${JARS_DIR}/org.apache.kafka_kafka-clients-3.9.1.jar,\
${JARS_DIR}/org.apache.commons_commons-pool2-2.12.1.jar,\
${JARS_DIR}/org.lz4_lz4-java-1.8.0.jar,\
${JARS_DIR}/org.xerial.snappy_snappy-java-1.1.10.8.jar,\
${JARS_DIR}/org.slf4j_slf4j-api-2.0.17.jar,\
${JARS_DIR}/org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar"

# Classpath cho driver và executor (separator = ":" — Linux convention)
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

echo "==> Submitting BronzeBatchJob ..."

docker exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars "${JARS_LIST}" \
  --driver-class-path "${CLASSPATH}" \
  --conf "spark.executor.extraClassPath=${CLASSPATH}" \
  --conf "spark.executorEnv.KAFKA_BOOTSTRAP=kafka-kraft:29092" \
  --conf "spark.executorEnv.KAFKA_TOPIC=ecommerce_events" \
  --conf "spark.executorEnv.MINIO_ENDPOINT=http://minio:9000" \
  --conf "spark.executorEnv.MINIO_ACCESS_KEY=admin" \
  --conf "spark.executorEnv.MINIO_SECRET_KEY=Admin123!" \
  --conf "spark.executorEnv.MINIO_BUCKET_BRONZE=bronze" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.driver.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=warn" \
  /opt/spark/work/bronze_job.py

echo "==> Done."