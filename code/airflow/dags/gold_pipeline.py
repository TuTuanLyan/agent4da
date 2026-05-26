"""
DAG: gold_pipeline
Spark Gold pipeline: staging events -> facts -> dimensions -> summaries.
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
GOLD_BUCKET = env("MINIO_BUCKET_GOLD", "gold")
GOLD_STORAGE_ROOT = env("GOLD_STORAGE_ROOT", f"s3a://{GOLD_BUCKET}").rstrip("/")
GOLD_STAGING_BASE_PATH = env(
    "GOLD_STAGING_BASE_PATH",
    f"{GOLD_STORAGE_ROOT}/gold_staging",
).rstrip("/")
GOLD_BASE_PATH = env("GOLD_BASE_PATH", f"{GOLD_STORAGE_ROOT}/gold").rstrip("/")
GOLD_STAGING_WAREHOUSE = env(
    "GOLD_STAGING_ICEBERG_WAREHOUSE",
    f"{GOLD_STAGING_BASE_PATH}/warehouse",
)
GOLD_WAREHOUSE = env("GOLD_ICEBERG_WAREHOUSE", f"{GOLD_BASE_PATH}/warehouse")

GOLD_JARS = [
    f"{JARS_DIR}/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
    f"{JARS_DIR}/postgresql-42.7.4.jar",
]
LOCAL_JARS = BASE_JARS + GOLD_JARS
CLASSPATH = ":".join(LOCAL_JARS)
JARS_CSV = ",".join(LOCAL_JARS)

STAGING_NAMESPACE = "gold_staging"
STAGING_TABLE = "stg_events"
STAGING_PATH = f"{GOLD_STAGING_BASE_PATH}/stg_events"
GOLD_NAMESPACE = "gold"
FACT_EVENTS_TABLE = "fact_events"
FACT_SALES_TABLE = "fact_sales"
FACT_EVENTS_PATH = f"{GOLD_BASE_PATH}/fact_events"
FACT_SALES_PATH = f"{GOLD_BASE_PATH}/fact_sales"
DIM_TIME_TABLE = "dim_time"
DIM_PRODUCT_TABLE = "dim_product"
DIM_USER_TABLE = "dim_user"
DIM_SESSION_TABLE = "dim_session"
DIM_TIME_PATH = f"{GOLD_BASE_PATH}/dim_time"
DIM_PRODUCT_PATH = f"{GOLD_BASE_PATH}/dim_product"
DIM_USER_PATH = f"{GOLD_BASE_PATH}/dim_user"
DIM_SESSION_PATH = f"{GOLD_BASE_PATH}/dim_session"
DAILY_EVENT_SUMMARY_TABLE = "daily_event_summary"
DAILY_PRODUCT_SUMMARY_TABLE = "daily_product_summary"
DAILY_CATEGORY_SUMMARY_TABLE = "daily_category_summary"
DAILY_BRAND_SUMMARY_TABLE = "daily_brand_summary"
DAILY_EVENT_SUMMARY_PATH = f"{GOLD_BASE_PATH}/daily_event_summary"
DAILY_PRODUCT_SUMMARY_PATH = f"{GOLD_BASE_PATH}/daily_product_summary"
DAILY_CATEGORY_SUMMARY_PATH = f"{GOLD_BASE_PATH}/daily_category_summary"
DAILY_BRAND_SUMMARY_PATH = f"{GOLD_BASE_PATH}/daily_brand_summary"
REFRESH_MODE = "full_refresh"


default_args = {
    "owner": "agent4da",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def gold_spark_conf(warehouse):
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
            "spark.executorEnv.GOLD_STAGING_ICEBERG_WAREHOUSE": (
                GOLD_STAGING_WAREHOUSE
            ),
            "spark.executorEnv.GOLD_ICEBERG_WAREHOUSE": warehouse,
            "spark.executorEnv.MINIO_BUCKET_GOLD": GOLD_BUCKET,
            "spark.executorEnv.GOLD_STORAGE_ROOT": GOLD_STORAGE_ROOT,
            "spark.executorEnv.GOLD_STAGING_BASE_PATH": GOLD_STAGING_BASE_PATH,
            "spark.executorEnv.GOLD_BASE_PATH": GOLD_BASE_PATH,
        }
    )
    return conf


def gold_env_vars(warehouse):
    return {
        "MINIO_ENDPOINT": env("MINIO_ENDPOINT", "http://minio:9000"),
        "MINIO_ACCESS_KEY": require_env("MINIO_ACCESS_KEY"),
        "MINIO_SECRET_KEY": require_env("MINIO_SECRET_KEY"),
        "ICEBERG_CATALOG_NAME": ICEBERG_CATALOG_NAME,
        "ICEBERG_JDBC_URI": ICEBERG_JDBC_URI,
        "ICEBERG_JDBC_USER": ICEBERG_JDBC_USER,
        "ICEBERG_JDBC_PASSWORD": ICEBERG_JDBC_PASSWORD,
        "ICEBERG_JDBC_SCHEMA": ICEBERG_JDBC_SCHEMA,
        "GOLD_STAGING_ICEBERG_WAREHOUSE": GOLD_STAGING_WAREHOUSE,
        "GOLD_ICEBERG_WAREHOUSE": warehouse,
        "MINIO_BUCKET_GOLD": GOLD_BUCKET,
        "GOLD_STORAGE_ROOT": GOLD_STORAGE_ROOT,
        "GOLD_STAGING_BASE_PATH": GOLD_STAGING_BASE_PATH,
        "GOLD_BASE_PATH": GOLD_BASE_PATH,
    }


def summary_application_args(summary):
    return [
        "--catalog-name",
        ICEBERG_CATALOG_NAME,
        "--source-namespace",
        GOLD_NAMESPACE,
        "--target-namespace",
        GOLD_NAMESPACE,
        "--fact-events-table",
        FACT_EVENTS_TABLE,
        "--fact-sales-table",
        FACT_SALES_TABLE,
        "--dim-product-table",
        DIM_PRODUCT_TABLE,
        "--daily-event-summary-table",
        DAILY_EVENT_SUMMARY_TABLE,
        "--daily-product-summary-table",
        DAILY_PRODUCT_SUMMARY_TABLE,
        "--daily-category-summary-table",
        DAILY_CATEGORY_SUMMARY_TABLE,
        "--daily-brand-summary-table",
        DAILY_BRAND_SUMMARY_TABLE,
        "--daily-event-summary-path",
        DAILY_EVENT_SUMMARY_PATH,
        "--daily-product-summary-path",
        DAILY_PRODUCT_SUMMARY_PATH,
        "--daily-category-summary-path",
        DAILY_CATEGORY_SUMMARY_PATH,
        "--daily-brand-summary-path",
        DAILY_BRAND_SUMMARY_PATH,
        "--summary",
        summary,
        "--refresh-mode",
        REFRESH_MODE,
    ]


def summary_task(task_id, summary, name):
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application="/opt/project/code/spark/gold/tasks/gold_build_summaries.py",
        application_args=summary_application_args(summary),
        jars=JARS_CSV,
        driver_class_path=CLASSPATH,
        conf=gold_spark_conf(GOLD_WAREHOUSE),
        env_vars=gold_env_vars(GOLD_WAREHOUSE),
        name=name,
        verbose=True,
        execution_timeout=timedelta(minutes=20),
    )


@dag(
    dag_id="gold_pipeline",
    description="Spark Gold pipeline: staging events -> facts -> dimensions -> summaries",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gold", "iceberg", "spark"],
)
def gold_pipeline():
    prepare_events = SparkSubmitOperator(
        task_id="gold_prepare_events",
        conn_id="spark_default",
        application="/opt/project/code/spark/gold/tasks/gold_prepare_events.py",
        application_args=[
            "--silver-path",
            env("SILVER_EVENTS_PATH", "s3a://silver/ecommerce_events/"),
            "--catalog-name",
            ICEBERG_CATALOG_NAME,
            "--namespace",
            STAGING_NAMESPACE,
            "--output-table",
            STAGING_TABLE,
            "--output-path",
            STAGING_PATH,
            "--refresh-mode",
            REFRESH_MODE,
        ],
        jars=JARS_CSV,
        driver_class_path=CLASSPATH,
        conf=gold_spark_conf(GOLD_STAGING_WAREHOUSE),
        env_vars=gold_env_vars(GOLD_STAGING_WAREHOUSE),
        name="GoldPrepareEvents",
        verbose=True,
        execution_timeout=timedelta(minutes=20),
    )

    build_facts = SparkSubmitOperator(
        task_id="gold_build_facts",
        conn_id="spark_default",
        application="/opt/project/code/spark/gold/tasks/gold_build_facts.py",
        application_args=[
            "--catalog-name",
            ICEBERG_CATALOG_NAME,
            "--staging-namespace",
            STAGING_NAMESPACE,
            "--staging-table",
            STAGING_TABLE,
            "--target-namespace",
            GOLD_NAMESPACE,
            "--fact-events-table",
            FACT_EVENTS_TABLE,
            "--fact-sales-table",
            FACT_SALES_TABLE,
            "--fact-events-path",
            FACT_EVENTS_PATH,
            "--fact-sales-path",
            FACT_SALES_PATH,
            "--refresh-mode",
            REFRESH_MODE,
        ],
        jars=JARS_CSV,
        driver_class_path=CLASSPATH,
        conf=gold_spark_conf(GOLD_WAREHOUSE),
        env_vars=gold_env_vars(GOLD_WAREHOUSE),
        name="GoldBuildFacts",
        verbose=True,
        execution_timeout=timedelta(minutes=20),
    )

    build_dimensions = SparkSubmitOperator(
        task_id="gold_build_dimensions",
        conn_id="spark_default",
        application="/opt/project/code/spark/gold/tasks/gold_build_dimensions.py",
        application_args=[
            "--catalog-name",
            ICEBERG_CATALOG_NAME,
            "--source-namespace",
            GOLD_NAMESPACE,
            "--target-namespace",
            GOLD_NAMESPACE,
            "--staging-namespace",
            STAGING_NAMESPACE,
            "--staging-table",
            STAGING_TABLE,
            "--fact-events-table",
            FACT_EVENTS_TABLE,
            "--fact-sales-table",
            FACT_SALES_TABLE,
            "--dim-time-table",
            DIM_TIME_TABLE,
            "--dim-product-table",
            DIM_PRODUCT_TABLE,
            "--dim-user-table",
            DIM_USER_TABLE,
            "--dim-session-table",
            DIM_SESSION_TABLE,
            "--dim-time-path",
            DIM_TIME_PATH,
            "--dim-product-path",
            DIM_PRODUCT_PATH,
            "--dim-user-path",
            DIM_USER_PATH,
            "--dim-session-path",
            DIM_SESSION_PATH,
            "--refresh-mode",
            REFRESH_MODE,
        ],
        jars=JARS_CSV,
        driver_class_path=CLASSPATH,
        conf=gold_spark_conf(GOLD_WAREHOUSE),
        env_vars=gold_env_vars(GOLD_WAREHOUSE),
        name="GoldBuildDimensions",
        verbose=True,
        execution_timeout=timedelta(minutes=20),
    )

    build_daily_event_summary = summary_task(
        "gold_build_daily_event_summary",
        "event",
        "GoldBuildDailyEventSummary",
    )
    build_daily_product_summary = summary_task(
        "gold_build_daily_product_summary",
        "product",
        "GoldBuildDailyProductSummary",
    )
    build_daily_category_summary = summary_task(
        "gold_build_daily_category_summary",
        "category",
        "GoldBuildDailyCategorySummary",
    )
    build_daily_brand_summary = summary_task(
        "gold_build_daily_brand_summary",
        "brand",
        "GoldBuildDailyBrandSummary",
    )

    prepare_events >> build_facts >> build_dimensions
    build_dimensions >> [
        build_daily_event_summary,
        build_daily_product_summary,
        build_daily_category_summary,
        build_daily_brand_summary,
    ]


gold_pipeline()
