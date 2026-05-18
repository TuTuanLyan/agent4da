"""
DAG: gold_pipeline
Consolidated Spark Gold Job: Silver Parquet to Gold Iceberg analytics tables.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_common import (
    base_spark_conf,
    build_classpath,
    env,
    iceberg_executor_conf,
    iceberg_spark_conf,
    minio_executor_conf,
    minio_spark_conf,
)

CLASSPATH = build_classpath(include_iceberg=True)


default_args = {
    "owner": "agent4da",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="gold_pipeline",
    description="Consolidated Spark Gold Job: Silver Parquet to Gold Iceberg analytics tables",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["gold", "iceberg", "spark", "etl"],
)
def gold_pipeline():
    conf = base_spark_conf(CLASSPATH)
    conf.update(minio_spark_conf())
    conf.update(iceberg_spark_conf())
    conf.update(minio_executor_conf())
    conf.update(iceberg_executor_conf())
    conf.update(
        {
            "spark.executorEnv.SILVER_EVENTS_PATH": env(
                "SILVER_EVENTS_PATH",
                "s3a://silver/ecommerce_events/",
            ),
            "spark.executorEnv.GOLD_RUN_MODE": env("GOLD_RUN_MODE", "all"),
            "spark.executorEnv.GOLD_REFRESH_MODE": env("GOLD_REFRESH_MODE", "full_refresh"),
            "spark.executorEnv.GOLD_DRY_RUN": env("GOLD_DRY_RUN", "false"),
            "spark.executorEnv.GOLD_VALIDATE_TABLES": env("GOLD_VALIDATE_TABLES", "true"),
        }
    )

    SparkSubmitOperator(
        task_id="gold_job",
        conn_id="spark_default",
        application="/opt/project/code/spark/gold_job.py",
        # Use local classpath only; --jars copies large jars into log/spark/app-*.
        jars=None,
        driver_class_path=CLASSPATH,
        conf=conf,
        packages=None,
        name="GoldJob",
        verbose=True,
        execution_timeout=timedelta(minutes=30),
    )


gold_pipeline()
