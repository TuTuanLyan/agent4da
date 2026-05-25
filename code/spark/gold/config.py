"""Shared Gold configuration and Spark/Iceberg session helpers."""

from dataclasses import dataclass

from common.config import env, load_minio_config, require_env
from common.s3a import apply_s3a_options

from gold.identifiers import table_identifier


DEFAULT_CATALOG = "iceberg_catalog"
DEFAULT_STAGING_NAMESPACE = "gold_staging"
DEFAULT_GOLD_NAMESPACE = "gold"
DEFAULT_METADATA_NAMESPACE = "metadata"

DEFAULT_GOLD_BUCKET = env("MINIO_BUCKET_GOLD", "gold")
DEFAULT_GOLD_STORAGE_ROOT = env("GOLD_STORAGE_ROOT", f"s3a://{DEFAULT_GOLD_BUCKET}")
DEFAULT_GOLD_STORAGE_ROOT = DEFAULT_GOLD_STORAGE_ROOT.rstrip("/")
DEFAULT_STAGING_BASE_PATH = env(
    "GOLD_STAGING_BASE_PATH",
    f"{DEFAULT_GOLD_STORAGE_ROOT}/gold_staging",
).rstrip("/")
DEFAULT_GOLD_BASE_PATH = env(
    "GOLD_BASE_PATH",
    f"{DEFAULT_GOLD_STORAGE_ROOT}/gold",
).rstrip("/")
DEFAULT_METADATA_BASE_PATH = env(
    "GOLD_METADATA_BASE_PATH",
    f"{DEFAULT_GOLD_STORAGE_ROOT}/metadata",
).rstrip("/")


def _location_prefix(path):
    return f"{path.rstrip('/')}/"


DEFAULT_ALLOWED_LOCATION_PREFIXES = sorted(
    {
        _location_prefix(DEFAULT_GOLD_STORAGE_ROOT),
        _location_prefix(DEFAULT_STAGING_BASE_PATH),
        _location_prefix(DEFAULT_GOLD_BASE_PATH),
        _location_prefix(DEFAULT_METADATA_BASE_PATH),
    }
)

STG_EVENTS = "stg_events"
FACT_EVENTS = "fact_events"
FACT_SALES = "fact_sales"
DIM_TIME = "dim_time"
DIM_PRODUCT = "dim_product"
DIM_USER = "dim_user"
DIM_SESSION = "dim_session"
DAILY_EVENT_SUMMARY = "daily_event_summary"
DAILY_PRODUCT_SUMMARY = "daily_product_summary"
DAILY_CATEGORY_SUMMARY = "daily_category_summary"
DAILY_BRAND_SUMMARY = "daily_brand_summary"
TABLE_CATALOG = "table_catalog"
COLUMN_CATALOG = "column_catalog"
METRIC_CATALOG = "metric_catalog"
JOIN_CATALOG = "join_catalog"

DEFAULT_REFRESH_MODE = "full_refresh"
DEFAULT_SILVER_PATH = "s3a://silver/ecommerce_events/"

DEFAULT_STAGING_WAREHOUSE = f"{DEFAULT_STAGING_BASE_PATH}/warehouse"
DEFAULT_GOLD_WAREHOUSE = f"{DEFAULT_GOLD_BASE_PATH}/warehouse"
DEFAULT_METADATA_WAREHOUSE = f"{DEFAULT_METADATA_BASE_PATH}/warehouse"


def table_location(base_path, table):
    return f"{base_path.rstrip('/')}/{table}"


@dataclass(frozen=True)
class IcebergRuntimeConfig:
    minio: object
    jdbc_uri: str
    jdbc_user: str
    jdbc_password: str
    jdbc_schema: str
    warehouse: str
    shuffle_partitions: str


def load_runtime_config(default_warehouse, warehouse_env_var="GOLD_ICEBERG_WAREHOUSE"):
    warehouse = env(warehouse_env_var)
    if warehouse is None and warehouse_env_var != "GOLD_ICEBERG_WAREHOUSE":
        warehouse = env("GOLD_ICEBERG_WAREHOUSE")
    if warehouse is None:
        warehouse = default_warehouse

    return IcebergRuntimeConfig(
        minio=load_minio_config(),
        jdbc_uri=env("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres-db:5432/agent4da"),
        jdbc_user=require_env("ICEBERG_JDBC_USER"),
        jdbc_password=require_env("ICEBERG_JDBC_PASSWORD"),
        jdbc_schema=env("ICEBERG_JDBC_SCHEMA", "iceberg"),
        warehouse=warehouse,
        shuffle_partitions=env("SPARK_SHUFFLE_PARTITIONS", "4"),
    )


def create_spark_session(app_name, catalog_name, runtime_config):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    builder = apply_s3a_options(builder, runtime_config.minio)
    return (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", runtime_config.shuffle_partitions)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(f"spark.sql.catalog.{catalog_name}.uri", runtime_config.jdbc_uri)
        .config(f"spark.sql.catalog.{catalog_name}.jdbc.user", runtime_config.jdbc_user)
        .config(
            f"spark.sql.catalog.{catalog_name}.jdbc.password",
            runtime_config.jdbc_password,
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.jdbc.currentSchema",
            runtime_config.jdbc_schema,
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", runtime_config.warehouse)
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .getOrCreate()
    )


def require_full_refresh(refresh_mode, task_name):
    mode = str(refresh_mode).strip().lower()
    if mode != DEFAULT_REFRESH_MODE:
        raise NotImplementedError(
            f"Incremental/MERGE refresh is not implemented for {task_name}; "
            "use --refresh-mode full_refresh."
        )
    return mode
