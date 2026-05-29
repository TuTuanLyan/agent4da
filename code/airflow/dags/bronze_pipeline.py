"""
DAG: bronze_pipeline
Spark Batch: Kafka → MinIO bronze (Parquet), offset-based incremental load.

Schedule: chạy mỗi 10 phút.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_common import (
    base_spark_conf,
    build_classpath,
    build_local_jars_csv,
    env,
    minio_executor_conf,
)


CLASSPATH = build_classpath()
LOCAL_JARS = build_local_jars_csv()

# ---------------------------------------------------------------------------
default_args = {
    "owner": "agent4da",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="bronze_pipeline",
    description="Spark batch: Kafka → MinIO bronze (Parquet)",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="*/10 * * * *",  # cron string — tránh deprecation warning của timedelta
    catchup=False,
    max_active_runs=1,        # tránh race condition trên offset file trong MinIO
    tags=["bronze", "kafka", "spark"],
)
def bronze_pipeline():
    conf = base_spark_conf(CLASSPATH)
    conf.update(minio_executor_conf())
    conf.update(
        {
            "spark.executorEnv.KAFKA_BOOTSTRAP": env("KAFKA_BOOTSTRAP", "kafka-kraft:29092"),
            "spark.executorEnv.KAFKA_TOPIC": env("KAFKA_TOPIC", "ecommerce_events"),
            "spark.executorEnv.MINIO_BUCKET_BRONZE": env("MINIO_BUCKET_BRONZE", "bronze"),
        }
    )

    SparkSubmitOperator(
        task_id="spark_bronze_job",

        # Connection — định nghĩa qua AIRFLOW_CONN_SPARK_DEFAULT trong compose
        conn_id="spark_default",

        # Script — path trong container airflow (volume ./code → /opt/project/code)
        application="/opt/project/code/spark/bronze_job.py",

        # Dùng --jars với scheme local: (jar đã mount sẵn ở /opt/project/jars
        # trên mọi node). Cần --jars để Kafka datasource (.format("kafka"))
        # được nạp vào application classloader — chỉ extraClassPath/
        # --driver-class-path là KHÔNG đủ cho datasource discovery. Scheme
        # local: giúp Spark KHÔNG copy jar vào worker-logs mỗi lần chạy, tránh
        # phình log/spark hàng chục GB (DAG chạy mỗi 10 phút).
        jars=LOCAL_JARS,

        # --driver-class-path: JVM classpath cho driver process (colon-separated)
        driver_class_path=CLASSPATH,

        conf=conf,

        # Không dùng packages — tránh Ivy resolver chạy mỗi lần submit
        packages=None,

        name="BronzeBatchJob",
        verbose=True,

        # Task timeout — không để treo indefinitely
        execution_timeout=timedelta(minutes=15),
    )


bronze_pipeline()
