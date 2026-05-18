"""Shared SparkSession factory."""

from pyspark.sql import SparkSession

from common.config import load_config
from common.iceberg import apply_iceberg_configs
from common.s3a import apply_s3a_configs


def create_spark_session(
    app_name,
    enable_iceberg=False,
    pipeline_config=None,
    session_timezone=None,
):
    config = pipeline_config or load_config()
    builder = SparkSession.builder.appName(app_name)
    builder = apply_s3a_configs(builder, config.minio)

    if enable_iceberg:
        builder = apply_iceberg_configs(builder, config.iceberg)

    if session_timezone:
        builder = builder.config("spark.sql.session.timeZone", session_timezone)

    return (
        builder
        .config("spark.sql.shuffle.partitions", config.spark_shuffle_partitions)
        .getOrCreate()
    )

