"""Small shared settings for Bronze and Silver Spark DAGs."""

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

def env(name, default):
    return os.getenv(name, default)


DRIVER_PYTHON = env("SPARK_DRIVER_PYTHON", "/usr/local/bin/python3")
EXECUTOR_PYTHON = env("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3")


def require_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        raise ValueError(f"Missing required Airflow environment variable: {name}")
    return value


def build_classpath():
    return ":".join(BASE_JARS)


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
