"""Iceberg catalog helpers."""

from common.config import validate_identifier
from common.logging_utils import log


def table_name(catalog, namespace, short_name):
    validate_identifier(catalog, "catalog")
    validate_identifier(namespace, "namespace")
    validate_identifier(short_name, "table_name")
    return f"{catalog}.{namespace}.{short_name}"


def build_iceberg_config_dict(iceberg_config):
    catalog = iceberg_config.catalog_name
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"spark.sql.catalog.{catalog}.uri": iceberg_config.jdbc_uri,
        f"spark.sql.catalog.{catalog}.jdbc.user": iceberg_config.jdbc_user,
        f"spark.sql.catalog.{catalog}.jdbc.password": iceberg_config.jdbc_password,
        f"spark.sql.catalog.{catalog}.jdbc.currentSchema": iceberg_config.jdbc_schema,
        f"spark.sql.catalog.{catalog}.warehouse": iceberg_config.warehouse,
        f"spark.sql.catalog.{catalog}.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    }


def apply_iceberg_configs(builder, iceberg_config):
    for key, value in build_iceberg_config_dict(iceberg_config).items():
        builder = builder.config(key, value)
    return builder


def run_sql(spark, sql, description=None):
    if description:
        log("GoldJob", description)
    return spark.sql(sql)


def ensure_namespace(spark, catalog, namespace):
    full_namespace = f"{catalog}.{namespace}"
    run_sql(
        spark,
        f"CREATE NAMESPACE IF NOT EXISTS {full_namespace}",
        f"Ensuring namespace {full_namespace}",
    )

