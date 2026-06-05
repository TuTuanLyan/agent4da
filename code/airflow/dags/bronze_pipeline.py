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
    env,
    minio_executor_conf,
)


CLASSPATH = build_classpath()

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
            "spark.executorEnv.ETL_PARTITION_STATE_PATH": env(
                "ETL_PARTITION_STATE_PATH",
                "s3a://bronze/_state/etl_partition_status.json",
            ),
        }
    )

    SparkSubmitOperator(
        task_id="spark_bronze_job",

        # Connection — định nghĩa qua AIRFLOW_CONN_SPARK_DEFAULT trong compose
        conn_id="spark_default",

        # Script — path trong container airflow (volume ./code → /opt/project/code)
        application="/opt/project/code/spark/bronze_job.py",

        # Không dùng --jars: tránh Spark copy jar vào log/spark/app-* mỗi lần chạy.
        jars=None,

        # --driver-class-path: JVM classpath cho driver process (colon-separated)
        driver_class_path=CLASSPATH,

        conf=conf,
        env_vars={
            "KAFKA_BOOTSTRAP": env("KAFKA_BOOTSTRAP", "kafka-kraft:29092"),
            "KAFKA_TOPIC": env("KAFKA_TOPIC", "ecommerce_events"),
            "MINIO_BUCKET_BRONZE": env("MINIO_BUCKET_BRONZE", "bronze"),
            "ETL_PARTITION_STATE_PATH": env(
                "ETL_PARTITION_STATE_PATH",
                "s3a://bronze/_state/etl_partition_status.json",
            ),
        },

        # Không dùng packages — tránh Ivy resolver chạy mỗi lần submit
        packages=None,

        name="BronzeBatchJob",
        verbose=True,

        # Task timeout — không để treo indefinitely
        execution_timeout=timedelta(minutes=15),
    )


bronze_pipeline()
