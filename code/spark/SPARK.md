# Spark Jobs

Spark layer hiện được giữ đơn giản trước: Bronze và Silver là hai job đang chạy
chính. Gold hiện tại đã được đưa vào `_archive/` để sau này build lại sạch hơn.

## Current Entrypoints

- `bronze_job.py`: đọc Kafka topic `ecommerce_events`, ghi Parquet vào `s3a://bronze/ecommerce_events/`.
- `silver_job.py`: đọc Bronze Parquet, chuẩn hóa dữ liệu, ghi valid/invalid Silver Parquet.

## Current Structure

```text
code/spark/
├── bronze_job.py
├── silver_job.py
├── common/
│   ├── __init__.py
│   ├── config.py
│   └── s3a.py
└── _archive/
```

## Design Rule

- Bronze config chỉ nằm trong `load_bronze_config()`.
- Silver config chỉ nằm trong `load_silver_config()`.
- Mỗi job tự tạo SparkSession trong chính file job.
- `common/` chỉ chứa helper nhỏ, không chứa factory SparkSession có nhánh Gold/Iceberg.
- Không dùng `spark.jars.packages`; DAG/script dùng JAR local đã mount.
- Gold/Iceberg config sẽ được thêm lại sau trong module riêng, không trộn vào Bronze/Silver.

## Manual Run

```bash
./script/spark/submit_bronze.sh
./script/spark/submit_silver.sh
```

Active submit scripts chỉ load `envs/minio.env`, `envs/spark.env`, và
`envs/airflow.env`. Gold/Iceberg scripts, kể cả script tải Iceberg JAR, đã được
đưa vào `script/spark/_archive/`.

## Airflow DAGs

- `code/airflow/dags/bronze_pipeline.py`
- `code/airflow/dags/silver_pipeline.py`

Gold DAG hiện nằm trong:

```text
code/airflow/dags/_disabled/gold_pipeline.py.disabled
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
