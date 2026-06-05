# Airflow DAGs

Airflow điều phối Spark jobs qua `SparkSubmitOperator`.

## DAG hiện có

### bronze_pipeline

- File: `bronze_pipeline.py`
- Task: `spark_bronze_job`
- App: `/opt/project/code/spark/bronze_job.py`
- Lịch chạy: mỗi 10 phút
- Mục tiêu: Kafka `ecommerce_events` đến MinIO Bronze Parquet
- Cơ chế: đọc offset mới, parse `event_time` thành `event_date`, ghi partition
  theo `event_date`, cập nhật manifest state cho các ngày bị ảnh hưởng.

### silver_pipeline

- File: `silver_pipeline.py`
- Task: `spark_silver_job`
- App: `/opt/project/code/spark/silver_job.py`
- Lịch chạy: mỗi 10 phút
- Mục tiêu: MinIO Bronze Parquet đến MinIO Silver clean Parquet
- Cơ chế: đọc manifest state, chọn các ngày
  `bronze_status = DONE AND silver_status = PENDING`, tối đa
  `MAX_SILVER_DATES_PER_RUN` ngày mỗi run, replace Silver partitions theo
  `event_date`.

### gold_pipeline

- File: `gold_pipeline.py`
- Lịch chạy: manual (`schedule=None`) trong stage hiện tại.
- Mode mặc định: `incremental`.
- Mục tiêu: xử lý các ngày Gold pending từ manifest:
  `silver_status = DONE AND gold_status = PENDING`.
- Giới hạn mỗi run: `MAX_GOLD_DATES_PER_RUN`, mặc định `3`.
- Flow:

```text
gold_prepare_events
  -> gold_build_facts
  -> gold_build_dimensions
  -> [daily summary tasks]
  -> refresh_gold_metadata
  -> validate_gold_metadata
  -> mark_gold_done
```

- Full refresh: manual/admin only, chạy bằng:

```bash
airflow dags trigger gold_pipeline --conf '{"mode":"full_refresh"}'
```

- Full refresh không tự động chạy sau Silver và không mark manifest date DONE.

### gold_metadata_pipeline

- Manual DAG riêng cho semantic metadata của Agent.
- Main `gold_pipeline` đã gọi metadata build/validate sau Gold data tasks; DAG
  riêng vẫn dùng cho admin/schema metadata refresh thủ công.

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
- Bronze/Silver state tracking hiện dùng manifest JSON mặc định tại
  `s3a://bronze/_state/etl_partition_status.json`.
- Silver dedup valid records theo `event_fingerprint`.
- Silver legacy full-scan chỉ bật khi `SILVER_FULL_SCAN_FALLBACK=true`.
- Gold incremental dùng cùng manifest state; `mark_gold_done` chỉ chạy sau data
  tasks và metadata validation thành công.
- Gold full refresh là manual/admin only.
