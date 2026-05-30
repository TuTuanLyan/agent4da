#!/usr/bin/env bash
# Submit GoldSchemaInitJob manually from the host via the Airflow container.

set -euo pipefail

SPARK_SUBMIT_CONTAINER="${SPARK_SUBMIT_CONTAINER:-airflow}"
RESET_GOLD_SCHEMA="${RESET_GOLD_SCHEMA:-false}"
ENABLE_GOLD_SCHEMA_TEST_DATA="${ENABLE_GOLD_SCHEMA_TEST_DATA:-false}"

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

echo "==> Submitting GoldSchemaInitJob from container ${SPARK_SUBMIT_CONTAINER} ..."
echo "==> RESET_GOLD_SCHEMA=${RESET_GOLD_SCHEMA}"
echo "==> ENABLE_GOLD_SCHEMA_TEST_DATA=${ENABLE_GOLD_SCHEMA_TEST_DATA}"

docker exec \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=admin \
  -e MINIO_SECRET_KEY='change_me' \
  -e ICEBERG_CATALOG_NAME=iceberg_catalog \
  -e ICEBERG_NAMESPACE=gold \
  -e ICEBERG_WAREHOUSE=s3a://gold/warehouse/ \
  -e ICEBERG_JDBC_URI=jdbc:postgresql://postgres-db:5432/agent4da \
  -e ICEBERG_JDBC_USER=bigdata \
  -e ICEBERG_JDBC_PASSWORD='change_me' \
  -e ICEBERG_JDBC_SCHEMA=iceberg \
  -e RESET_GOLD_SCHEMA="${RESET_GOLD_SCHEMA}" \
  -e ENABLE_GOLD_SCHEMA_TEST_DATA="${ENABLE_GOLD_SCHEMA_TEST_DATA}" \
  "${SPARK_SUBMIT_CONTAINER}" \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-class-path "${CLASSPATH}" \
  --conf "spark.executor.extraClassPath=${CLASSPATH}" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.uri=jdbc:postgresql://postgres-db:5432/agent4da" \
  --conf "spark.sql.catalog.iceberg_catalog.jdbc.user=bigdata" \
  --conf "spark.sql.catalog.iceberg_catalog.jdbc.password=change_me" \
  --conf "spark.sql.catalog.iceberg_catalog.jdbc.currentSchema=iceberg" \
  --conf "spark.sql.catalog.iceberg_catalog.warehouse=s3a://gold/warehouse/" \
  --conf "spark.sql.catalog.iceberg_catalog.io-impl=org.apache.iceberg.hadoop.HadoopFileIO" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=admin" \
  --conf "spark.hadoop.fs.s3a.secret.key=change_me" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.driver.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  --conf "spark.executor.extraJavaOptions=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN" \
  --conf "spark.executorEnv.MINIO_ENDPOINT=http://minio:9000" \
  --conf "spark.executorEnv.MINIO_ACCESS_KEY=admin" \
  --conf "spark.executorEnv.MINIO_SECRET_KEY=change_me" \
  --conf "spark.executorEnv.ICEBERG_CATALOG_NAME=iceberg_catalog" \
  --conf "spark.executorEnv.ICEBERG_NAMESPACE=gold" \
  --conf "spark.executorEnv.ICEBERG_WAREHOUSE=s3a://gold/warehouse/" \
  --conf "spark.executorEnv.ICEBERG_JDBC_URI=jdbc:postgresql://postgres-db:5432/agent4da" \
  --conf "spark.executorEnv.ICEBERG_JDBC_USER=bigdata" \
  --conf "spark.executorEnv.ICEBERG_JDBC_PASSWORD=change_me" \
  --conf "spark.executorEnv.ICEBERG_JDBC_SCHEMA=iceberg" \
  --conf "spark.executorEnv.RESET_GOLD_SCHEMA=${RESET_GOLD_SCHEMA}" \
  --conf "spark.executorEnv.ENABLE_GOLD_SCHEMA_TEST_DATA=${ENABLE_GOLD_SCHEMA_TEST_DATA}" \
  /opt/project/code/spark/gold_schema_init.py

echo "==> Done."
