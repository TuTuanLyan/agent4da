# Spark Jobs

Project dùng Spark cho Medallion Architecture.

## Main Jobs

- `bronze_job.py`: đọc Kafka topic `ecommerce_events`, ghi Parquet vào `s3a://bronze/ecommerce_events/`.
- `silver_job.py`: đọc Bronze Parquet, chuẩn hóa dữ liệu, ghi valid/invalid Silver Parquet.
- `gold_job.py`: tạo Iceberg schema, build Gold analytics tables và semantic metadata.

## Shared Modules

- `common/`: config, SparkSession, S3A, Iceberg, logging, data quality helpers.
- `gold/`: schemas, DDL, readers, builders, writers, validators cho Gold.
- `_archive/`: Gold stage jobs cũ, không còn là entrypoint chính.
- `tools/iceberg_smoke_test.py`: smoke test Spark + Iceberg JDBC Catalog + MinIO.

## Manual Run

```bash
./script/spark/submit_bronze.sh
./script/spark/submit_silver.sh
./script/spark/submit_gold.sh
```

Gold run mode examples:

```bash
GOLD_RUN_MODE=schema_only ./script/spark/submit_gold.sh
GOLD_RUN_MODE=all GOLD_REFRESH_MODE=full_refresh ./script/spark/submit_gold.sh
GOLD_RUN_MODE=validate_only ./script/spark/submit_gold.sh
```

Iceberg smoke test:

```bash
./script/spark/submit_iceberg_smoke_test.sh
```

## JARs

Không dùng `--packages`. JAR local được mount tại:

```bash
/opt/project/jars
```

DAG và submit scripts truyền JAR qua:

```bash
--driver-class-path
spark.executor.extraClassPath
```

Airflow container dùng Python driver `3.10` tại `/usr/local/bin/python3`.
Spark worker dùng executor Python `3.10` tại `/usr/bin/python3`. Hai path này
được cấu hình qua `SPARK_DRIVER_PYTHON` và `SPARK_EXECUTOR_PYTHON` để tránh lỗi
PySpark `PYTHON_VERSION_MISMATCH`.

## Logs

Airflow UI là nơi đọc log chính khi chạy bằng DAG. Spark worker logs vẫn ở:

```bash
log/spark/app-*/0/stdout
log/spark/app-*/0/stderr
```
