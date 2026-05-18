# Airflow DAGs

Airflow điều phối Spark jobs qua `SparkSubmitOperator`.

## DAG hiện có

### bronze_pipeline

- File: `bronze_pipeline.py`
- Task: `spark_bronze_job`
- App: `/opt/project/code/spark/bronze_job.py`
- Lịch chạy: mỗi 10 phút
- Mục tiêu: Kafka `ecommerce_events` đến MinIO Bronze Parquet

### silver_pipeline

- File: `silver_pipeline.py`
- Task: `spark_silver_job`
- App: `/opt/project/code/spark/silver_job.py`
- Lịch chạy: mỗi 10 phút
- Mục tiêu: MinIO Bronze Parquet đến MinIO Silver clean Parquet

### gold_pipeline

- File: `gold_pipeline.py`
- Task: `gold_job`
- App: `/opt/project/code/spark/gold_job.py`
- Lịch chạy: manual
- Mục tiêu: Silver clean Parquet đến Gold Iceberg analytics tables và metadata catalog

## Cấu hình chung

- `conn_id`: `spark_default`
- `catchup`: `False`
- `max_active_runs`: `1`
- `retries`: `1`
- `retry_delay`: 3 phút
- `execution_timeout`: 15 phút
- Secret được đọc từ Airflow environment, không hardcode trong DAG.

## JARs

DAG không dùng `packages`.

JAR được mount sẵn tại:

```bash
/opt/project/jars
```

DAG truyền JAR qua:

```bash
driver_class_path
spark.executor.extraClassPath
```

Mục tiêu là tránh Ivy resolver và tránh tải dependency lại mỗi lần chạy.

## Ghi chú

- Bronze vẫn giữ logic offset trên MinIO để hạn chế đọc trùng Kafka.
- Silver dedup valid records theo `source_event_id`.
- Gold chính chỉ còn `gold_pipeline.py`.
- Các DAG Gold stage cũ nằm trong `_disabled/*.py.disabled`.
