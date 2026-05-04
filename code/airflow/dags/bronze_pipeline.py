"""
DAG: bronze_pipeline
Spark Batch: Kafka → MinIO bronze (Parquet), offset-based incremental load.

Schedule: 5 phút để test → đổi "0 * * * *" khi production.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ---------------------------------------------------------------------------
# Jars — mount tại /opt/project/jars/ trong cả airflow và spark containers
#
# Phân biệt 2 loại:
#   JARS_DISTRIBUTE : --jars → Spark copy đến executor trước khi task chạy
#   CLASSPATH       : --driver-class-path + spark.executor.extraClassPath
#                     → JVM thêm vào classpath khi load classes
#
# Bỏ bundle-2.29.52.jar plain (trùng với software.amazon.awssdk_bundle-2.29.52.jar)
# → tránh SLF4J duplicate binding + ClassLoader conflict
# ---------------------------------------------------------------------------
_J = "/opt/project/jars"

_JARS = [
    # Hadoop S3A
    f"{_J}/org.apache.hadoop_hadoop-aws-3.4.2.jar",
    f"{_J}/org.apache.hadoop_hadoop-client-api-3.4.2.jar",
    f"{_J}/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar",
    # AWS SDK — chỉ dùng bản có prefix tên đầy đủ, bỏ bundle-2.29.52.jar plain
    f"{_J}/software.amazon.awssdk_bundle-2.29.52.jar",
    # Kafka connector
    f"{_J}/org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar",
    f"{_J}/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar",
    f"{_J}/org.apache.kafka_kafka-clients-3.9.1.jar",
    # Runtime deps
    f"{_J}/org.apache.commons_commons-pool2-2.12.1.jar",
    f"{_J}/org.lz4_lz4-java-1.8.0.jar",
    f"{_J}/org.xerial.snappy_snappy-java-1.1.10.8.jar",
    f"{_J}/org.slf4j_slf4j-api-2.0.17.jar",
    f"{_J}/org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar",
]

# --jars: comma-separated (Spark convention)
JARS_CSV = ",".join(_JARS)

# classpath: colon-separated (Linux JVM convention)
CLASSPATH = ":".join(_JARS)

# ---------------------------------------------------------------------------
default_args = {
    "owner": "agent4da",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="bronze_pipeline",
    description="Spark batch: Kafka → MinIO bronze (Parquet)",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="*/5 * * * *",   # cron string — tránh deprecation warning của timedelta
    catchup=False,
    max_active_runs=1,        # tránh race condition trên offset file trong MinIO
    tags=["bronze", "kafka", "spark"],
)
def bronze_pipeline():

    SparkSubmitOperator(
        task_id="spark_bronze_job",

        # Connection — định nghĩa qua AIRFLOW_CONN_SPARK_DEFAULT trong compose
        conn_id="spark_default",

        # Script — path trong container airflow (volume ./code → /opt/project/code)
        application="/opt/project/code/spark/bronze_job.py",

        # --jars: distribute các jar đến driver + executor (comma-separated)
        jars=JARS_CSV,

        # --driver-class-path: JVM classpath cho driver process (colon-separated)
        driver_class_path=CLASSPATH,

        conf={
            # JVM classpath cho executor processes (colon-separated)
            "spark.executor.extraClassPath": CLASSPATH,

            # Suppress SLF4J duplicate binding từ AWS bundle
            "spark.driver.extraJavaOptions":
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",
            "spark.executor.extraJavaOptions":
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN",

            # Truyền env vars xuống executor — bronze_job.py đọc os.getenv()
            # Driver đọc trực tiếp từ container env (airflow.env)
            # Executor (spark-worker) cần được truyền tường minh
            "spark.executorEnv.KAFKA_BOOTSTRAP":     "kafka-kraft:29092",
            "spark.executorEnv.KAFKA_TOPIC":         "ecommerce_events",
            "spark.executorEnv.MINIO_ENDPOINT":      "http://minio:9000",
            "spark.executorEnv.MINIO_ACCESS_KEY":    "admin",
            "spark.executorEnv.MINIO_SECRET_KEY":    "Admin123!",
            "spark.executorEnv.MINIO_BUCKET_BRONZE": "bronze",

            # Giới hạn shuffle — cluster nhỏ không cần 200
            "spark.sql.shuffle.partitions": "4",
        },

        # Không dùng packages — tránh Ivy resolver chạy mỗi lần submit
        packages=None,

        name="BronzeBatchJob",
        verbose=True,

        # Task timeout — không để treo indefinitely
        execution_timeout=timedelta(minutes=15),
    )


bronze_pipeline()