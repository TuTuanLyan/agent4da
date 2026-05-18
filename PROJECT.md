# Agent4DA Project

Agent4DA là hệ thống data pipeline và AI Agent analytics cho e-commerce.
Pipeline chính đi theo Medallion Architecture: Bronze, Silver, Gold.

## Folder Structure

- `app/`: backend/frontend application.
- `code/kafka/`: Kafka producer và tài liệu Kafka.
- `code/spark/`: Spark ETL entrypoints.
- `code/spark/common/`: helper chung cho config, Spark, S3A, Iceberg, logging, data quality.
- `code/spark/gold/`: module build Gold Iceberg tables và metadata catalog.
- `code/spark/_archive/`: Gold stage jobs cũ, giữ để tham khảo.
- `code/spark/tools/`: Spark utility jobs như Iceberg smoke test.
- `code/airflow/dags/`: Airflow DAGs cho Bronze, Silver, Gold.
- `data/`: raw/sample data local.
- `envs/`: environment variables và local dev secrets.
- `jars/`: local Spark/Iceberg/Hadoop jars.
- `docs/`: design và operation docs.
- `script/`: helper scripts chạy thủ công.
- `notebook/`: notebook kiểm tra dữ liệu.
- `init/`: database init scripts.
- `dockerfile/`: custom Dockerfiles và entrypoint.
- `monitoring/`: monitoring configs.

## Data Pipeline

Bronze:
- Đọc raw events từ Kafka.
- Thêm Kafka metadata.
- Ghi Parquet vào MinIO Bronze.

Silver:
- Chuẩn hóa kiểu dữ liệu.
- Enrich category levels.
- Validate và tách valid/invalid records.
- Deduplicate theo `source_event_id`.
- Ghi clean Parquet vào MinIO Silver.

Gold:
- Đọc Silver valid events.
- Ghi Iceberg fact, dimension, summary tables.
- Ghi metadata catalog phục vụ AI Agent.

## Main Entrypoints

- `code/spark/bronze_job.py`
- `code/spark/silver_job.py`
- `code/spark/gold_job.py`
- `code/airflow/dags/bronze_pipeline.py`
- `code/airflow/dags/silver_pipeline.py`
- `code/airflow/dags/gold_pipeline.py`

## Secret Management

- Secrets nằm trong `envs/*.env` cho local dev.
- Không hardcode production secrets trong code, DAG hoặc compose.
- Xem `docs/ENV_SETUP.md` để biết biến cần cấu hình.
