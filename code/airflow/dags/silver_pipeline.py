"""
DAG: silver_pipeline
Spark Batch: MinIO bronze Parquet -> MinIO silver clean Parquet.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Jars are mounted at /opt/project/jars in Airflow and Spark containers.
# Use local classpath only; do not pass --jars, otherwise Spark copies these
# jars into log/spark/app-* for every application run.
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
]

CLASSPATH = ":".join(_JARS)


default_args = {
    "owner": "agent4da",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="silver_pipeline",
    description="Spark batch: MinIO bronze Parquet -> MinIO silver clean Parquet",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "minio", "spark"],
)
def silver_pipeline():
    SparkSubmitOperator(
        task_id="spark_silver_job",
        conn_id="spark_default",
        application="/opt/project/code/spark/silver_job.py",
        jars=None,
        driver_class_path=CLASSPATH,
        conf={
            "spark.executor.extraClassPath": CLASSPATH,
            "spark.pyspark.python": "/usr/bin/python3",
            "spark.pyspark.driver.python": "/usr/bin/python3",
            "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3",
            "spark.driver.extraJavaOptions":
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
            "spark.executor.extraJavaOptions":
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
            "spark.executorEnv.MINIO_ENDPOINT": "http://minio:9000",
            "spark.executorEnv.MINIO_ACCESS_KEY": "admin",
            "spark.executorEnv.MINIO_SECRET_KEY": "Admin123!",
            "spark.executorEnv.MINIO_BUCKET_BRONZE": "bronze",
            "spark.executorEnv.MINIO_BUCKET_SILVER": "silver",
            "spark.sql.shuffle.partitions": "4",
        },
        packages=None,
        name="SilverEcommerceEventsJob",
        verbose=True,
        execution_timeout=timedelta(minutes=15),
    )


silver_pipeline()
