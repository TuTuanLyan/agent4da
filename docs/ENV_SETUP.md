# Environment Setup

`envs/*.env` chứa biến chạy local dev. Không đưa production secrets vào code,
DAG, compose hoặc docs. Thay các giá trị `change_me` bằng secret thật trên máy
triển khai.

## Secret Variables

```bash
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=change_me
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=change_me

# PostgreSQL
POSTGRES_USER=bigdata
POSTGRES_PASSWORD=change_me
POSTGRES_DB=agent4da

# Airflow database user created by init/01_init_schemas.sh
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=change_me

# Iceberg JDBC
ICEBERG_JDBC_USER=bigdata
ICEBERG_JDBC_PASSWORD=change_me

# Airflow UI and webserver
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=change_me
AIRFLOW__WEBSERVER__SECRET_KEY=change_me
AIRFLOW__CORE__FERNET_KEY=change_me

# External APIs
GROQ_API_KEY=change_me
```

## Non-secret Variables

```bash
KAFKA_BOOTSTRAP=kafka-kraft:29092
KAFKA_TOPIC=ecommerce_events

MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET_BRONZE=bronze
MINIO_BUCKET_SILVER=silver
MINIO_BUCKET_GOLD=gold

SPARK_MASTER_URL=spark://spark-master:7077
SPARK_SHUFFLE_PARTITIONS=4
SPARK_DRIVER_PYTHON=/usr/local/bin/python3
SPARK_EXECUTOR_PYTHON=/usr/bin/python3

ICEBERG_CATALOG_NAME=iceberg_catalog
ICEBERG_WAREHOUSE=s3a://gold/warehouse/
ICEBERG_JDBC_URI=jdbc:postgresql://postgres-db:5432/agent4da
ICEBERG_JDBC_SCHEMA=iceberg

GOLD_NAMESPACE=gold
METADATA_NAMESPACE=metadata
SILVER_EVENTS_PATH=s3a://silver/ecommerce_events/
GOLD_RUN_MODE=all
GOLD_REFRESH_MODE=full_refresh
GOLD_DRY_RUN=false
GOLD_VALIDATE_TABLES=true
```

## Files

- `envs/minio.env`: MinIO account, access key, bucket names.
- `envs/postgre.env`: Postgres database and Airflow DB role secrets.
- `envs/airflow.env`: Airflow DB connection, admin user, Kafka runtime vars.
- `envs/iceberg.env`: Iceberg catalog/JDBC vars and Gold run options.
- `envs/spark.env`: Spark submit defaults.
- `envs/groq.env`: optional LLM/API key for agent experiments.
