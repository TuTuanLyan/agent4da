"""
DAG: iceberg_smoke_test_pipeline
Smoke test: Spark + Iceberg JDBC Catalog + MinIO warehouse.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


_J = "/opt/project/jars"

_JARS = [
    # Hadoop S3A
    f"{_J}/org.apache.hadoop_hadoop-aws-3.4.2.jar",
    f"{_J}/org.apache.hadoop_hadoop-client-api-3.4.2.jar",
    f"{_J}/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar",
    # AWS SDK
    f"{_J}/software.amazon.awssdk_bundle-2.29.52.jar",
    # Kafka connector jars are kept here to match bronze_pipeline classpath.
    f"{_J}/org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar",
    f"{_J}/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar",
    f"{_J}/org.apache.kafka_kafka-clients-3.9.1.jar",
    # Runtime deps
    f"{_J}/org.apache.commons_commons-pool2-2.12.1.jar",
    f"{_J}/org.lz4_lz4-java-1.8.0.jar",
    f"{_J}/org.xerial.snappy_snappy-java-1.1.10.8.jar",
    f"{_J}/org.slf4j_slf4j-api-2.0.17.jar",
    f"{_J}/org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar",
    # Iceberg JDBC Catalog
    f"{_J}/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
    f"{_J}/postgresql-42.7.4.jar",
]

CLASSPATH = ":".join(_JARS)


default_args = {
    "owner": "agent4da",
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="iceberg_smoke_test_pipeline",
    description="Smoke test Spark + Iceberg JDBC Catalog + MinIO warehouse",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["gold", "iceberg", "spark", "smoke-test"],
)
def iceberg_smoke_test_pipeline():
    SparkSubmitOperator(
        task_id="spark_iceberg_smoke_test",
        conn_id="spark_default",
        application="/opt/project/code/spark/iceberg_smoke_test.py",
        # Do not pass --jars here. The same /opt/project/jars path is mounted
        # in Airflow, spark-master and spark-worker, so local classpath is
        # enough and avoids copying large jars into log/spark/app-* per run.
        jars=None,
        driver_class_path=CLASSPATH,
        conf={
            "spark.executor.extraClassPath": CLASSPATH,
            "spark.pyspark.python": "/usr/bin/python3",
            "spark.pyspark.driver.python": "/usr/bin/python3",
            "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3",
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.iceberg_catalog":
                "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg_catalog.catalog-impl":
                "org.apache.iceberg.jdbc.JdbcCatalog",
            "spark.sql.catalog.iceberg_catalog.uri":
                "jdbc:postgresql://postgres-db:5432/agent4da",
            "spark.sql.catalog.iceberg_catalog.jdbc.user": "bigdata",
            "spark.sql.catalog.iceberg_catalog.jdbc.password": "#3Bigdata",
            "spark.sql.catalog.iceberg_catalog.jdbc.currentSchema": "iceberg",
            "spark.sql.catalog.iceberg_catalog.warehouse": "s3a://gold/warehouse/",
            "spark.sql.catalog.iceberg_catalog.io-impl":
                "org.apache.iceberg.hadoop.HadoopFileIO",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "Admin123!",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.shuffle.partitions": "4",
            "spark.driver.extraJavaOptions":
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
            "spark.executor.extraJavaOptions":
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
            "spark.executorEnv.MINIO_ENDPOINT": "http://minio:9000",
            "spark.executorEnv.MINIO_ACCESS_KEY": "admin",
            "spark.executorEnv.MINIO_SECRET_KEY": "Admin123!",
            "spark.executorEnv.ICEBERG_CATALOG_NAME": "iceberg_catalog",
            "spark.executorEnv.ICEBERG_WAREHOUSE": "s3a://gold/warehouse/",
            "spark.executorEnv.ICEBERG_JDBC_URI":
                "jdbc:postgresql://postgres-db:5432/agent4da",
            "spark.executorEnv.ICEBERG_JDBC_USER": "bigdata",
            "spark.executorEnv.ICEBERG_JDBC_PASSWORD": "#3Bigdata",
            "spark.executorEnv.ICEBERG_JDBC_SCHEMA": "iceberg",
        },
        packages=None,
        name="IcebergSmokeTest",
        verbose=True,
        execution_timeout=timedelta(minutes=10),
    )


iceberg_smoke_test_pipeline()
