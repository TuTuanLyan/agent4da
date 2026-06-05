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
    env,
    minio_executor_conf,
)


CLASSPATH = build_classpath()


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
            "spark.executorEnv.ETL_PARTITION_STATE_PATH": env(
                "ETL_PARTITION_STATE_PATH",
                "s3a://bronze/_state/etl_partition_status.json",
            ),
            "spark.executorEnv.MAX_SILVER_DATES_PER_RUN": env("MAX_SILVER_DATES_PER_RUN", "7"),
            "spark.executorEnv.SILVER_FULL_SCAN_FALLBACK": env("SILVER_FULL_SCAN_FALLBACK", "false"),
        }
    )

    SparkSubmitOperator(
        task_id="spark_silver_job",
        conn_id="spark_default",
        application="/opt/project/code/spark/silver_job.py",
        jars=None,
        driver_class_path=CLASSPATH,
        conf=conf,
        env_vars={
            "MINIO_BUCKET_BRONZE": env("MINIO_BUCKET_BRONZE", "bronze"),
            "MINIO_BUCKET_SILVER": env("MINIO_BUCKET_SILVER", "silver"),
            "SILVER_WRITE_MODE": env("SILVER_WRITE_MODE", "append"),
            "ETL_PARTITION_STATE_PATH": env(
                "ETL_PARTITION_STATE_PATH",
                "s3a://bronze/_state/etl_partition_status.json",
            ),
            "MAX_SILVER_DATES_PER_RUN": env("MAX_SILVER_DATES_PER_RUN", "7"),
            "SILVER_FULL_SCAN_FALLBACK": env("SILVER_FULL_SCAN_FALLBACK", "false"),
        },
        packages=None,
        name="SilverEcommerceEventsJob",
        verbose=True,
        execution_timeout=timedelta(minutes=15),
    )


silver_pipeline()
