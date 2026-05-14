# Spark Jobs

Project dùng Spark để xử lý Medallion Architecture.

Hiện có 2 job:

- `bronze_job.py`: đọc Kafka topic `ecommerce_events`, ghi Parquet vào `s3a://bronze/ecommerce_events/`.
- `silver_job.py`: đọc Bronze Parquet, chuẩn hóa dữ liệu, ghi valid vào `s3a://silver/ecommerce_events/` và invalid vào `s3a://silver/ecommerce_events_invalid/`.

## JARs

Không dùng `--packages`.

Các JAR local được mount tại:

```bash
/opt/project/jars
```

DAG và script submit truyền JAR qua:

```bash
--driver-class-path
spark.executor.extraClassPath
```

Cách này tránh Ivy resolver và tránh tải dependency lại mỗi lần submit.

## Chạy thủ công

Chạy Bronze:

```bash
./script/spark/submit_bronze.sh
```

Chạy Silver:

```bash
./script/spark/submit_silver.sh
```

Test Silver bằng overwrite:

```bash
SILVER_WRITE_MODE=overwrite ./script/spark/submit_silver.sh
```

## Log

Airflow UI là nơi đọc log chính khi chạy bằng DAG.

Spark worker vẫn lưu executor log tại:

```bash
log/spark/app-*/0/stdout
log/spark/app-*/0/stderr
```
