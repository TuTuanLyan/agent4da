"""
DAG: gold_metadata_pipeline
Manual semantic metadata pipeline for Gold tables.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_common import (
    BASE_JARS,
    JARS_DIR,
    base_spark_conf,
    env,
    minio_executor_conf,
    require_env,
)


ICEBERG_CATALOG_NAME = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
ICEBERG_JDBC_URI = env("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres-db:5432/agent4da")
ICEBERG_JDBC_SCHEMA = env("ICEBERG_JDBC_SCHEMA", "iceberg")
ICEBERG_JDBC_USER = require_env("ICEBERG_JDBC_USER")
ICEBERG_JDBC_PASSWORD = require_env("ICEBERG_JDBC_PASSWORD")
METADATA_WAREHOUSE = env(
    "GOLD_METADATA_ICEBERG_WAREHOUSE",
    "s3a://test/metadata/warehouse",
)

STAGING_NAMESPACE = env("GOLD_STAGING_NAMESPACE", "gold_staging")
GOLD_NAMESPACE = env("GOLD_NAMESPACE", "gold")
METADATA_NAMESPACE = env("METADATA_NAMESPACE", "metadata")
METADATA_BASE_PATH = env("METADATA_BASE_PATH", "s3a://test/metadata")
REFRESH_MODE = env("GOLD_METADATA_REFRESH_MODE", "full_refresh")

GOLD_JARS = [
    f"{JARS_DIR}/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
    f"{JARS_DIR}/postgresql-42.7.4.jar",
]
LOCAL_JARS = BASE_JARS + GOLD_JARS
CLASSPATH = ":".join(LOCAL_JARS)
JARS_CSV = ",".join(LOCAL_JARS)


default_args = {
    "owner": "agent4da",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def metadata_spark_conf(warehouse):
    conf = base_spark_conf(CLASSPATH)
    conf.update(minio_executor_conf())
    conf.update(
        {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}": (
                "org.apache.iceberg.spark.SparkCatalog"
            ),
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.catalog-impl": (
                "org.apache.iceberg.jdbc.JdbcCatalog"
            ),
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.uri": ICEBERG_JDBC_URI,
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.jdbc.user": ICEBERG_JDBC_USER,
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.jdbc.password": (
                ICEBERG_JDBC_PASSWORD
            ),
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.jdbc.currentSchema": (
                ICEBERG_JDBC_SCHEMA
            ),
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse": warehouse,
            f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.io-impl": (
                "org.apache.iceberg.hadoop.HadoopFileIO"
            ),
            "spark.executorEnv.ICEBERG_CATALOG_NAME": ICEBERG_CATALOG_NAME,
            "spark.executorEnv.ICEBERG_JDBC_URI": ICEBERG_JDBC_URI,
            "spark.executorEnv.ICEBERG_JDBC_USER": ICEBERG_JDBC_USER,
            "spark.executorEnv.ICEBERG_JDBC_PASSWORD": ICEBERG_JDBC_PASSWORD,
            "spark.executorEnv.ICEBERG_JDBC_SCHEMA": ICEBERG_JDBC_SCHEMA,
            "spark.executorEnv.GOLD_METADATA_ICEBERG_WAREHOUSE": warehouse,
            "spark.executorEnv.GOLD_ICEBERG_WAREHOUSE": warehouse,
        }
    )
    return conf


def metadata_env_vars(warehouse):
    return {
        "MINIO_ENDPOINT": env("MINIO_ENDPOINT", "http://minio:9000"),
        "MINIO_ACCESS_KEY": require_env("MINIO_ACCESS_KEY"),
        "MINIO_SECRET_KEY": require_env("MINIO_SECRET_KEY"),
        "ICEBERG_CATALOG_NAME": ICEBERG_CATALOG_NAME,
        "ICEBERG_JDBC_URI": ICEBERG_JDBC_URI,
        "ICEBERG_JDBC_USER": ICEBERG_JDBC_USER,
        "ICEBERG_JDBC_PASSWORD": ICEBERG_JDBC_PASSWORD,
        "ICEBERG_JDBC_SCHEMA": ICEBERG_JDBC_SCHEMA,
        "GOLD_METADATA_ICEBERG_WAREHOUSE": warehouse,
        "GOLD_ICEBERG_WAREHOUSE": warehouse,
    }


@dag(
    dag_id="gold_metadata_pipeline",
    description="Manual semantic metadata pipeline for Gold AI Agent context",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gold", "metadata", "ai-agent", "iceberg", "spark"],
)
def gold_metadata_pipeline():
    build_metadata = SparkSubmitOperator(
        task_id="gold_build_metadata",
        conn_id="spark_default",
        application="/opt/project/code/spark/gold/tasks/gold_build_metadata.py",
        application_args=[
            "--catalog-name",
            ICEBERG_CATALOG_NAME,
            "--metadata-namespace",
            METADATA_NAMESPACE,
            "--gold-namespace",
            GOLD_NAMESPACE,
            "--staging-namespace",
            STAGING_NAMESPACE,
            "--metadata-base-path",
            METADATA_BASE_PATH,
            "--refresh-mode",
            REFRESH_MODE,
        ],
        jars=JARS_CSV,
        driver_class_path=CLASSPATH,
        conf=metadata_spark_conf(METADATA_WAREHOUSE),
        env_vars=metadata_env_vars(METADATA_WAREHOUSE),
        name="GoldBuildMetadata",
        verbose=True,
        execution_timeout=timedelta(minutes=15),
    )

    validate_metadata = SparkSubmitOperator(
        task_id="gold_validate_metadata",
        conn_id="spark_default",
        application="/opt/project/code/spark/gold/tasks/gold_validate_metadata.py",
        application_args=[
            "--catalog-name",
            ICEBERG_CATALOG_NAME,
            "--metadata-namespace",
            METADATA_NAMESPACE,
            "--gold-namespace",
            GOLD_NAMESPACE,
            "--staging-namespace",
            STAGING_NAMESPACE,
        ],
        jars=JARS_CSV,
        driver_class_path=CLASSPATH,
        conf=metadata_spark_conf(METADATA_WAREHOUSE),
        env_vars=metadata_env_vars(METADATA_WAREHOUSE),
        name="GoldValidateMetadata",
        verbose=True,
        execution_timeout=timedelta(minutes=15),
    )

    build_metadata >> validate_metadata


gold_metadata_pipeline()
