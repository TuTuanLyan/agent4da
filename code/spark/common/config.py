"""Environment based configuration for Spark jobs."""

import os
import re
from dataclasses import dataclass


ALLOWED_GOLD_RUN_MODES = {
    "all",
    "schema_only",
    "mvp_only",
    "extended_only",
    "metadata_only",
    "validate_only",
}
ALLOWED_GOLD_REFRESH_MODES = {"full_refresh", "append"}

@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bronze_bucket: str
    silver_bucket: str
    gold_bucket: str


@dataclass(frozen=True)
class IcebergConfig:
    catalog_name: str
    warehouse: str
    jdbc_uri: str
    jdbc_user: str
    jdbc_password: str
    jdbc_schema: str


@dataclass(frozen=True)
class PipelineConfig:
    minio: MinioConfig
    iceberg: IcebergConfig
    kafka_bootstrap: str
    kafka_topic: str
    gold_namespace: str
    metadata_namespace: str
    silver_events_path: str
    silver_write_mode: str
    gold_run_mode: str
    gold_refresh_mode: str
    gold_dry_run: bool
    gold_validate_tables: bool
    spark_shuffle_partitions: str

    @property
    def catalog_name(self):
        return self.iceberg.catalog_name

    @property
    def warehouse(self):
        return self.iceberg.warehouse

    @property
    def jdbc_uri(self):
        return self.iceberg.jdbc_uri

    @property
    def jdbc_user(self):
        return self.iceberg.jdbc_user

    @property
    def jdbc_password(self):
        return self.iceberg.jdbc_password

    @property
    def jdbc_schema(self):
        return self.iceberg.jdbc_schema

    @property
    def run_mode(self):
        return self.gold_run_mode

    @property
    def refresh_mode(self):
        return self.gold_refresh_mode

    @property
    def dry_run(self):
        return self.gold_dry_run

    @property
    def validate_tables(self):
        return self.gold_validate_tables


def env(name, default=""):
    value = os.getenv(name)
    if value is None:
        return default
    return value


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def bool_env(name, default="false"):
    return env(name, default).strip().lower() == "true"


def validate_identifier(identifier, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid {label}: {identifier!r}")


def validate_choice(value, allowed_values, label):
    if value not in allowed_values:
        allowed = ", ".join(sorted(allowed_values))
        raise ValueError(f"Invalid {label}={value!r}. Allowed: {allowed}")


def load_config(validate_gold=True):
    catalog_name = env("ICEBERG_CATALOG_NAME", "iceberg_catalog").strip()
    gold_namespace = env("GOLD_NAMESPACE", "gold").strip()
    metadata_namespace = env("METADATA_NAMESPACE", "metadata").strip()
    gold_run_mode = env("GOLD_RUN_MODE", "all").strip().lower()
    gold_refresh_mode = env("GOLD_REFRESH_MODE", "full_refresh").strip().lower()

    validate_identifier(catalog_name, "ICEBERG_CATALOG_NAME")
    validate_identifier(gold_namespace, "GOLD_NAMESPACE")
    validate_identifier(metadata_namespace, "METADATA_NAMESPACE")
    if validate_gold:
        validate_choice(gold_run_mode, ALLOWED_GOLD_RUN_MODES, "GOLD_RUN_MODE")
        validate_choice(gold_refresh_mode, ALLOWED_GOLD_REFRESH_MODES, "GOLD_REFRESH_MODE")

    minio = MinioConfig(
        endpoint=env("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=required_env("MINIO_ACCESS_KEY"),
        secret_key=required_env("MINIO_SECRET_KEY"),
        bronze_bucket=env("MINIO_BUCKET_BRONZE", "bronze"),
        silver_bucket=env("MINIO_BUCKET_SILVER", "silver"),
        gold_bucket=env("MINIO_BUCKET_GOLD", "gold"),
    )

    jdbc_password = required_env("ICEBERG_JDBC_PASSWORD") if validate_gold else env("ICEBERG_JDBC_PASSWORD")

    iceberg = IcebergConfig(
        catalog_name=catalog_name,
        warehouse=env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/"),
        jdbc_uri=env("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres-db:5432/agent4da"),
        jdbc_user=env("ICEBERG_JDBC_USER", "bigdata"),
        jdbc_password=jdbc_password,
        jdbc_schema=env("ICEBERG_JDBC_SCHEMA", "iceberg"),
    )

    return PipelineConfig(
        minio=minio,
        iceberg=iceberg,
        kafka_bootstrap=env("KAFKA_BOOTSTRAP", "kafka-kraft:29092"),
        kafka_topic=env("KAFKA_TOPIC", "ecommerce_events"),
        gold_namespace=gold_namespace,
        metadata_namespace=metadata_namespace,
        silver_events_path=env("SILVER_EVENTS_PATH", "s3a://silver/ecommerce_events/"),
        silver_write_mode=env("SILVER_WRITE_MODE", "append").strip().lower(),
        gold_run_mode=gold_run_mode,
        gold_refresh_mode=gold_refresh_mode,
        gold_dry_run=bool_env("GOLD_DRY_RUN", "false"),
        gold_validate_tables=bool_env("GOLD_VALIDATE_TABLES", "true"),
        spark_shuffle_partitions=env("SPARK_SHUFFLE_PARTITIONS", "4"),
    )
