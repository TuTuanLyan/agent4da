"""
DAG: silver_pipeline
Spark Batch: MinIO bronze Parquet -> MinIO silver clean Parquet.
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
    conf = base_spark_conf(CLASSPATH)
    conf.update(minio_executor_conf())
    conf.update(
        {
            "spark.executorEnv.MINIO_BUCKET_BRONZE": env("MINIO_BUCKET_BRONZE", "bronze"),
            "spark.executorEnv.MINIO_BUCKET_SILVER": env("MINIO_BUCKET_SILVER", "silver"),
            "spark.executorEnv.SILVER_WRITE_MODE": env("SILVER_WRITE_MODE", "append"),
        }
    )

    SparkSubmitOperator(
        task_id="spark_silver_job",
        conn_id="spark_default",
        application="/opt/project/code/spark/silver_job.py",
        # --jars với scheme local: (xem chú thích trong bronze_pipeline.py):
        # datasource discoverable mà không copy jar vào worker-logs mỗi lần chạy.
        jars=LOCAL_JARS,
        driver_class_path=CLASSPATH,
        conf=conf,
        packages=None,
        name="SilverEcommerceEventsJob",
        verbose=True,
        execution_timeout=timedelta(minutes=15),
    )


silver_pipeline()
