# Agent4DA Project

Agent4DA là hệ thống data pipeline và AI Agent analytics cho e-commerce.
Pipeline chính đi theo Medallion Architecture: Bronze, Silver, Gold.

## Folder Structure

- `app/`: backend/frontend application.
- `code/kafka/`: Kafka producer và tài liệu Kafka.
- `code/spark/`: Spark ETL entrypoints.
- `code/spark/common/`: helper nhỏ cho env config và S3A.
- `code/spark/_archive/`: Gold jobs hiện tại và stage jobs cũ, giữ để build lại sau.
- `code/airflow/dags/`: Airflow DAGs cho Bronze và Silver.
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
- Tạm archive để build lại sau.
- Không còn trộn config Gold/Iceberg vào Bronze/Silver.

## Main Entrypoints

- `code/spark/bronze_job.py`
- `code/spark/silver_job.py`
- `code/airflow/dags/bronze_pipeline.py`
- `code/airflow/dags/silver_pipeline.py`

## Dashboard Metrics

- `code/agent/services/metrics_service.py`: Chứa các câu truy vấn metrics có thể tái sử dụng, được lấy dữ liệu thông qua nền tảng Trino.
- `docs/DASHBOARD_METRICS.md`: Tài liệu đặc tả giữa FE và BE dùng để xây dựng các thẻ hiển thị chỉ số và biểu đồ.

## Secret Management

- Secrets nằm trong `envs/*.env` cho local dev.
- Không hardcode production secrets trong code, DAG hoặc compose.
- Xem `docs/ENV_SETUP.md` để biết biến cần cấu hình.
