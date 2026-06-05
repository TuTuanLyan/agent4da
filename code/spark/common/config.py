"""Small env-based settings for Bronze and Silver Spark jobs."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class BronzeConfig:
    kafka_bootstrap: str
    kafka_topic: str
    minio: MinioConfig
    bronze_bucket: str
    output_path: str
    offset_file: str
    partition_state_path: str
    shuffle_partitions: str


@dataclass(frozen=True)
class SilverConfig:
    minio: MinioConfig
    bronze_bucket: str
    silver_bucket: str
    input_path: str
    valid_output_path: str
    invalid_output_path: str
    write_mode: str
    partition_state_path: str
    max_dates_per_run: int
    full_scan_fallback: bool
    shuffle_partitions: str


def env(name, default=None):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def require_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def env_int(name, default):
    raw_value = env(name)
    if raw_value is None:
        return int(default)
    try:
        return int(raw_value)
    except ValueError:
        print(f"[Config] Invalid {name}={raw_value!r}; using {default}.")
        return int(default)


def env_bool(name, default=False):
    raw_value = env(name)
    if raw_value is None:
        return bool(default)
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_minio_config():
    return MinioConfig(
        endpoint=env("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=require_env("MINIO_ACCESS_KEY"),
        secret_key=require_env("MINIO_SECRET_KEY"),
    )


def load_bronze_config():
    minio = load_minio_config()
    topic = env("KAFKA_TOPIC", "ecommerce_events")
    bronze_bucket = env("MINIO_BUCKET_BRONZE", "bronze")
    output_path = f"s3a://{bronze_bucket}/ecommerce_events/"
    state_path = env(
        "ETL_PARTITION_STATE_PATH",
        f"s3a://{bronze_bucket}/_state/etl_partition_status.json",
    )

    return BronzeConfig(
        kafka_bootstrap=env("KAFKA_BOOTSTRAP", "kafka-kraft:29092"),
        kafka_topic=topic,
        minio=minio,
        bronze_bucket=bronze_bucket,
        output_path=output_path,
        offset_file=f"s3a://{bronze_bucket}/_offsets/{topic}.json",
        partition_state_path=state_path,
        shuffle_partitions=env("SPARK_SHUFFLE_PARTITIONS", "4"),
    )


def load_silver_config():
    minio = load_minio_config()
    bronze_bucket = env("MINIO_BUCKET_BRONZE", "bronze")
    silver_bucket = env("MINIO_BUCKET_SILVER", "silver")

    return SilverConfig(
        minio=minio,
        bronze_bucket=bronze_bucket,
        silver_bucket=silver_bucket,
        input_path=f"s3a://{bronze_bucket}/ecommerce_events/",
        valid_output_path=f"s3a://{silver_bucket}/ecommerce_events/",
        invalid_output_path=f"s3a://{silver_bucket}/ecommerce_events_invalid/",
        write_mode=env("SILVER_WRITE_MODE", "append").strip().lower(),
        partition_state_path=env(
            "ETL_PARTITION_STATE_PATH",
            f"s3a://{bronze_bucket}/_state/etl_partition_status.json",
        ),
        max_dates_per_run=env_int("MAX_SILVER_DATES_PER_RUN", 7),
        full_scan_fallback=env_bool("SILVER_FULL_SCAN_FALLBACK", False),
        shuffle_partitions=env("SPARK_SHUFFLE_PARTITIONS", "4"),
    )
