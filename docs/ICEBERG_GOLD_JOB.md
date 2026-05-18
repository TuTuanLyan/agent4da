# Iceberg Gold Job

## Mục Tiêu

Final Stage gom Gold Layer về một entrypoint duy nhất:

```text
Silver Parquet -> code/spark/gold_job.py -> Gold Iceberg + Metadata Iceberg
```

Airflow chỉ cần DAG Gold chính `gold_pipeline`. Job `gold_job.py` tự đảm nhiệm tạo namespace, tạo bảng, đọc Silver valid events, build MVP Gold, build Extended Gold, build semantic metadata cho Agent, ghi Iceberg và validate.

## Kiến Trúc

- Input Silver: `s3a://silver/ecommerce_events/`
- Gold namespace: `iceberg_catalog.gold`
- Metadata namespace: `iceberg_catalog.metadata`
- Warehouse: `s3a://gold/warehouse/`
- Catalog: PostgreSQL JDBC Catalog qua `postgres-db:5432/agent4da`
- Storage: MinIO qua `s3a://`

## Gold Tables

Gold MVP:

- `gold.dim_time`: 1 row per event hour
- `gold.dim_product`: 1 row per product
- `gold.fact_events`: 1 row per clean ecommerce event
- `gold.fact_sales`: 1 row per purchase event
- `gold.daily_event_summary`: 1 row per event date

Gold Extended:

- `gold.dim_user`: 1 row per user
- `gold.dim_session`: 1 row per user session
- `gold.daily_product_summary`: 1 row per event date and product
- `gold.daily_category_summary`: 1 row per event date and category hierarchy
- `gold.daily_brand_summary`: 1 row per event date and brand

## Metadata Tables

- `metadata.table_catalog`: mô tả bảng Gold và metadata.
- `metadata.column_catalog`: mô tả các cột quan trọng cho Agent.
- `metadata.metric_catalog`: công thức metric chuẩn như revenue, purchase count, conversion rate.
- `metadata.join_catalog`: quan hệ join được khuyến nghị.

## Mapping Chính

- `source_event_id` -> `fact_events.event_id`, `fact_sales.sale_id`
- `event_ts` -> `dim_time`, `fact_events.event_ts`, `fact_sales.sale_ts`
- `event_date` -> daily summaries
- `event_type` -> flags, counts, conversion metrics
- `product_id`, `category_*`, `brand` -> `dim_product` và product/category/brand summaries
- `price` -> observed prices, `gross_amount`, revenue metrics
- `user_id`, `user_session` -> user/session dimensions

## Run Modes

`GOLD_RUN_MODE` hỗ trợ:

- `all`: tạo schema, build MVP, Extended, Metadata, ghi tất cả và validate.
- `schema_only`: chỉ tạo namespace/bảng và validate.
- `mvp_only`: build và ghi 5 bảng MVP từ Silver.
- `extended_only`: build MVP DataFrame trong memory rồi ghi 5 bảng Extended.
- `metadata_only`: chỉ build và ghi metadata semantic catalog.
- `validate_only`: chỉ chạy validation queries.

## Refresh Modes

`GOLD_REFRESH_MODE` hỗ trợ:

- `full_refresh`: mặc định, dùng `INSERT OVERWRITE` để chạy lại không bị duplicate trong demo/dev.
- `append`: append trực tiếp; có thể trùng dữ liệu nếu chạy lại cùng Silver input.

Incremental/MERGE INTO có thể bổ sung ở phiên bản sau.

## Chạy Bằng Airflow

Trigger DAG:

```bash
gold_pipeline
```

Task chính:

```text
gold_job
```

Airflow DAG dùng local jars trong `/opt/project/jars`, không dùng `--packages` và không dùng `--jars` để tránh Spark copy JAR lớn vào `log/spark/app-*`.

## Chạy Bằng Script

Chạy full pipeline:

```bash
bash script/spark/submit_gold.sh
```

Chỉ tạo schema:

```bash
GOLD_RUN_MODE=schema_only bash script/spark/submit_gold.sh
```

Chạy full refresh:

```bash
GOLD_RUN_MODE=all GOLD_REFRESH_MODE=full_refresh bash script/spark/submit_gold.sh
```

Chỉ validate:

```bash
GOLD_RUN_MODE=validate_only bash script/spark/submit_gold.sh
```

Dry run không ghi Iceberg:

```bash
GOLD_DRY_RUN=true bash script/spark/submit_gold.sh
```

## Kiểm Tra Bằng Spark SQL

```sql
SHOW NAMESPACES IN iceberg_catalog;
SHOW TABLES IN iceberg_catalog.gold;
SHOW TABLES IN iceberg_catalog.metadata;

SELECT COUNT(*) FROM iceberg_catalog.gold.fact_events;
SELECT COUNT(*) FROM iceberg_catalog.gold.fact_sales;
SELECT * FROM iceberg_catalog.gold.daily_event_summary ORDER BY event_date LIMIT 10;
SELECT * FROM iceberg_catalog.gold.daily_product_summary LIMIT 10;
SELECT * FROM iceberg_catalog.metadata.metric_catalog;
```

## Kiểm Tra PostgreSQL Iceberg Catalog

```bash
docker exec -it postgres-db psql -U bigdata -d agent4da
```

```sql
SELECT table_namespace, table_name
FROM iceberg.iceberg_tables
WHERE catalog_name = 'iceberg_catalog'
ORDER BY table_namespace, table_name;
```

## Env Quan Trọng

```text
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=Admin123!
SILVER_EVENTS_PATH=s3a://silver/ecommerce_events/
ICEBERG_CATALOG_NAME=iceberg_catalog
GOLD_NAMESPACE=gold
METADATA_NAMESPACE=metadata
ICEBERG_WAREHOUSE=s3a://gold/warehouse/
ICEBERG_JDBC_URI=jdbc:postgresql://postgres-db:5432/agent4da
ICEBERG_JDBC_USER=bigdata
ICEBERG_JDBC_PASSWORD=#3Bigdata
ICEBERG_JDBC_SCHEMA=iceberg
GOLD_RUN_MODE=all
GOLD_REFRESH_MODE=full_refresh
GOLD_DRY_RUN=false
GOLD_VALIDATE_TABLES=true
```

## Lưu Ý

- Bronze/Silver không bị thay đổi.
- Gold chỉ ghi qua Iceberg catalog, không ghi Parquet folder thủ công.
- Các DAG Gold stage cũ đã được thay thế bởi `gold_pipeline`.
- Nếu còn file DAG Stage 2/3/4 trong project, chúng nên nằm trong `code/airflow/dags/_disabled/*.py.disabled` để Airflow không parse nữa.
- Final Stage chưa tích hợp Trino, LangGraph Agent thật, dashboard hoặc API backend.
