"""Shared Airflow DAG settings for Spark submit jobs."""

import os


JARS_DIR = "/opt/project/jars"

BASE_JARS = [
    f"{JARS_DIR}/org.apache.hadoop_hadoop-aws-3.4.2.jar",
    f"{JARS_DIR}/org.apache.hadoop_hadoop-client-api-3.4.2.jar",
    f"{JARS_DIR}/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar",
    f"{JARS_DIR}/software.amazon.awssdk_bundle-2.29.52.jar",
    f"{JARS_DIR}/org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar",
    f"{JARS_DIR}/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar",
    f"{JARS_DIR}/org.apache.kafka_kafka-clients-3.9.1.jar",
    f"{JARS_DIR}/org.apache.commons_commons-pool2-2.12.1.jar",
    f"{JARS_DIR}/org.lz4_lz4-java-1.8.0.jar",
    f"{JARS_DIR}/org.xerial.snappy_snappy-java-1.1.10.8.jar",
    f"{JARS_DIR}/org.slf4j_slf4j-api-2.0.17.jar",
    f"{JARS_DIR}/org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar",
]

ICEBERG_JARS = [
    f"{JARS_DIR}/iceberg-spark-runtime-4.0_2.13-1.10.1.jar",
    f"{JARS_DIR}/postgresql-42.7.4.jar",
]


def env(name, default):
    return os.getenv(name, default)


DRIVER_PYTHON = env("SPARK_DRIVER_PYTHON", "/usr/local/bin/python3")
EXECUTOR_PYTHON = env("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3")


def require_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        raise ValueError(f"Missing required Airflow environment variable: {name}")
    return value


def build_classpath(include_iceberg=False):
    jars = list(BASE_JARS)
    if include_iceberg:
        jars.extend(ICEBERG_JARS)
    return ":".join(jars)


def base_spark_conf(classpath):
    return {
        "spark.executor.extraClassPath": classpath,
        "spark.pyspark.python": EXECUTOR_PYTHON,
        "spark.pyspark.driver.python": DRIVER_PYTHON,
        "spark.executorEnv.PYSPARK_PYTHON": EXECUTOR_PYTHON,
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": EXECUTOR_PYTHON,
        "spark.driver.extraJavaOptions": "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
        "spark.executor.extraJavaOptions": "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
        "spark.sql.shuffle.partitions": env("SPARK_SHUFFLE_PARTITIONS", "4"),
    }


def minio_executor_conf():
    return {
        "spark.executorEnv.MINIO_ENDPOINT": env("MINIO_ENDPOINT", "http://minio:9000"),
        "spark.executorEnv.MINIO_ACCESS_KEY": require_env("MINIO_ACCESS_KEY"),
        "spark.executorEnv.MINIO_SECRET_KEY": require_env("MINIO_SECRET_KEY"),
    }


def minio_spark_conf():
    return {
        "spark.hadoop.fs.s3a.endpoint": env("MINIO_ENDPOINT", "http://minio:9000"),
        "spark.hadoop.fs.s3a.access.key": require_env("MINIO_ACCESS_KEY"),
        "spark.hadoop.fs.s3a.secret.key": require_env("MINIO_SECRET_KEY"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }


def iceberg_spark_conf():
    catalog = env("ICEBERG_CATALOG_NAME", "iceberg_catalog")
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"spark.sql.catalog.{catalog}.uri": env(
            "ICEBERG_JDBC_URI",
            "jdbc:postgresql://postgres-db:5432/agent4da",
        ),
        f"spark.sql.catalog.{catalog}.jdbc.user": require_env("ICEBERG_JDBC_USER"),
        f"spark.sql.catalog.{catalog}.jdbc.password": require_env("ICEBERG_JDBC_PASSWORD"),
        f"spark.sql.catalog.{catalog}.jdbc.currentSchema": env("ICEBERG_JDBC_SCHEMA", "iceberg"),
        f"spark.sql.catalog.{catalog}.warehouse": env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/"),
        f"spark.sql.catalog.{catalog}.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    }


def iceberg_executor_conf():
    return {
        "spark.executorEnv.ICEBERG_CATALOG_NAME": env("ICEBERG_CATALOG_NAME", "iceberg_catalog"),
        "spark.executorEnv.GOLD_NAMESPACE": env("GOLD_NAMESPACE", "gold"),
        "spark.executorEnv.METADATA_NAMESPACE": env("METADATA_NAMESPACE", "metadata"),
        "spark.executorEnv.ICEBERG_WAREHOUSE": env("ICEBERG_WAREHOUSE", "s3a://gold/warehouse/"),
        "spark.executorEnv.ICEBERG_JDBC_URI": env(
            "ICEBERG_JDBC_URI",
            "jdbc:postgresql://postgres-db:5432/agent4da",
        ),
        "spark.executorEnv.ICEBERG_JDBC_USER": require_env("ICEBERG_JDBC_USER"),
        "spark.executorEnv.ICEBERG_JDBC_PASSWORD": require_env("ICEBERG_JDBC_PASSWORD"),
        "spark.executorEnv.ICEBERG_JDBC_SCHEMA": env("ICEBERG_JDBC_SCHEMA", "iceberg"),
    }
