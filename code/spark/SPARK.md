# Spark Jobs

Project dùng Spark để xử lý Medallion Architecture.

Các job chính:

- `bronze_job.py`: đọc Kafka topic `ecommerce_events`, ghi Parquet vào `s3a://bronze/ecommerce_events/`.
- `silver_job.py`: đọc Bronze Parquet, chuẩn hóa dữ liệu, ghi valid vào `s3a://silver/ecommerce_events/` và invalid vào `s3a://silver/ecommerce_events_invalid/`.
- `gold_job.py`: job Gold hợp nhất, tạo schema Iceberg, build Gold MVP, Gold Extended và semantic metadata cho Agent.

Các job Stage cũ vẫn được giữ để tham khảo/khôi phục khi cần:

- `iceberg_smoke_test.py`: smoke test hạ tầng Spark + Iceberg JDBC Catalog + MinIO warehouse cho Gold Stage 1.
- `gold_schema_init.py`: tạo Gold MVP Iceberg schema cho Stage 2, chưa nạp dữ liệu thật từ Silver.
- `gold_mvp_job.py`: đọc Silver Parquet sạch và ghi Gold MVP Iceberg tables cho Stage 3.
- `gold_extended_schema_init.py`: tạo Gold Extended Iceberg schema cho Stage 4.
- `gold_extended_job.py`: build Gold Extended analytics tables từ Gold MVP tables.

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

Chạy Iceberg Gold Stage 1 smoke test:

```bash
./script/spark/submit_iceberg_smoke_test.sh
```

Chạy Iceberg Gold Stage 2 schema init:

```bash
./script/spark/submit_gold_schema_init.sh
```

Chạy Iceberg Gold Stage 3 MVP ETL:

```bash
./script/spark/submit_gold_mvp.sh
```

Chạy Iceberg Gold Stage 4 Extended:

```bash
./script/spark/submit_gold_extended_schema_init.sh
./script/spark/submit_gold_extended.sh
```

Chạy Gold job hợp nhất:

```bash
./script/spark/submit_gold.sh
```

Ví dụ chạy từng mode:

```bash
GOLD_RUN_MODE=schema_only ./script/spark/submit_gold.sh
GOLD_RUN_MODE=all GOLD_REFRESH_MODE=full_refresh ./script/spark/submit_gold.sh
GOLD_RUN_MODE=validate_only ./script/spark/submit_gold.sh
```

Tài liệu chi tiết:

- `docs/ICEBERG_STAGE1.md`
- `docs/ICEBERG_STAGE2_GOLD_SCHEMA.md`
- `docs/ICEBERG_STAGE3_GOLD_MVP_ETL.md`
- `docs/ICEBERG_STAGE4_GOLD_EXTENDED.md`
- `docs/ICEBERG_GOLD_JOB.md`

## Log

Airflow UI là nơi đọc log chính khi chạy bằng DAG.

Spark worker vẫn lưu executor log tại:

```bash
log/spark/app-*/0/stdout
log/spark/app-*/0/stderr
```
