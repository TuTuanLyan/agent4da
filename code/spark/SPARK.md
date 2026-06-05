# Spark Jobs

Spark layer gồm Bronze, Silver và Gold. Bronze/Silver ghi Parquet trên MinIO;
Gold ghi Apache Iceberg tables trên MinIO với PostgreSQL JDBC catalog.

## Current Entrypoints

- `bronze_job.py`: đọc Kafka topic `ecommerce_events` theo offset, parse
  `event_time` thành `event_date`, ghi Parquet partitioned by `event_date` vào
  `s3a://bronze/ecommerce_events/`, và cập nhật manifest state.
- `silver_job.py`: đọc manifest state, xử lý các Bronze partition pending theo
  `event_date`, chuẩn hóa dữ liệu, replace valid/invalid Silver Parquet
  partitions.
- `gold/tasks/gold_prepare_events.py`: claim Gold pending dates từ manifest,
  đọc Silver partitions tương ứng và replace staging partitions.
- `gold/tasks/gold_build_facts.py`: build `fact_events`/`fact_sales` và replace
  theo `event_date`/`sale_date`.
- `gold/tasks/gold_build_dimensions.py`: build dimensions incremental theo
  `event_date` hoặc affected keys.
- `gold/tasks/gold_build_summaries.py`: recompute daily summaries cho active
  dates và replace summary partitions.
- `gold/tasks/gold_build_metadata.py` và `gold_validate_metadata.py`: full
  refresh/validate semantic metadata cho Agent context.
- `gold/tasks/gold_mark_done.py`: mark active Gold dates `DONE` sau khi data
  tasks và metadata validation thành công.

## Current Structure

```text
code/spark/
├── bronze_job.py
├── silver_job.py
├── common/
│   ├── __init__.py
│   ├── partition_state.py
│   ├── config.py
│   └── s3a.py
└── gold/
    ├── config.py
    ├── writers.py
    └── tasks/
```

## Design Rule

- Bronze config chỉ nằm trong `load_bronze_config()`.
- Silver config chỉ nằm trong `load_silver_config()`.
- Gold/Iceberg config nằm trong `code/spark/gold/config.py`, không trộn vào
  Bronze/Silver config.
- Mỗi job tự tạo SparkSession trong chính file job.
- `common/` chỉ chứa helper nhỏ và manifest state interface.
- Không dùng `spark.jars.packages`; DAG/script dùng JAR local đã mount.

## Manual Run

```bash
./script/spark/submit_bronze.sh
./script/spark/submit_silver.sh
```

Active submit scripts chỉ load `envs/minio.env`, `envs/spark.env`, và
`envs/airflow.env`.

State tracking cho Bronze/Silver hiện dùng manifest JSON mặc định:

```text
s3a://bronze/_state/etl_partition_status.json
```

Silver mặc định xử lý tối đa 7 ngày mỗi run, có thể đổi bằng
`MAX_SILVER_DATES_PER_RUN`. Legacy full scan chỉ bật khi
`SILVER_FULL_SCAN_FALLBACK=true`.

Gold mặc định xử lý tối đa 3 ngày mỗi run, có thể đổi bằng
`MAX_GOLD_DATES_PER_RUN`. Default mode là `incremental`; `full_refresh` chỉ dành
cho admin/manual rebuild.

## Airflow DAGs

- `code/airflow/dags/bronze_pipeline.py`
- `code/airflow/dags/silver_pipeline.py`
- `code/airflow/dags/gold_pipeline.py`
- `code/airflow/dags/gold_metadata_pipeline.py`

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
