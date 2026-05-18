# Iceberg Gold Layer - Stage 3 Gold MVP ETL

Stage 3 đọc dữ liệu sạch từ Silver Parquet trên MinIO và ghi vào các bảng Gold MVP Iceberg đã tạo ở Stage 2. Stage này chưa tạo Gold mở rộng, chưa tạo semantic metadata catalog cho Agent, chưa tích hợp Trino và không thay đổi Bronze/Silver.

## Input Silver

Path:

```text
s3a://silver/ecommerce_events/
```

Format: Parquet.

Các cột Silver dự kiến:

```text
source_event_id, event_ts, event_year, event_month, event_day, event_hour,
event_type, product_id, category_id, category_code, category_l1, category_l2,
category_l3, brand, price, user_id, user_session, kafka_ts, kafka_partition,
kafka_offset, bronze_ingested_at, silver_processed_at, is_valid,
invalid_reason, event_date
```

Job chỉ xử lý `is_valid = true`, dedup theo `source_event_id`, cast lại các cột quan trọng và lọc các record thiếu key bắt buộc.

## Output Gold MVP

Tất cả output ghi qua Iceberg catalog:

```text
iceberg_catalog.gold.<table_name>
```

Các bảng:

- `fact_events`
- `fact_sales`
- `dim_time`
- `dim_product`
- `daily_event_summary`

## Mapping

`fact_events`
: 1 row per clean event. `event_id = source_event_id`, `session_id = user_session`, flags từ `event_type`.

`fact_sales`
: 1 row per purchase event. Chỉ lấy `event_type = 'purchase'`, `sale_id = source_event_id`, `quantity = 1`, `gross_amount = price`.

`dim_time`
: 1 row per `time_id`, với `time_id = date_format(event_ts, 'yyyyMMddHH')`, thêm day/month/quarter/weekend attributes.

`dim_product`
: 1 row per `product_id`, group từ Silver clean events, lấy category/brand đầu tiên không null và observed price stats.

`daily_event_summary`
: 1 row per `event_date`, gồm event counts, distinct counts, revenue, conversion rate và cart-to-purchase rate.

## Cách Chạy

Điều kiện trước khi chạy:

1. Stage 1 smoke test đã `SUCCESS`.
2. Stage 2 `gold_schema_init_pipeline` đã `SUCCESS`.
3. Silver có dữ liệu tại `s3a://silver/ecommerce_events/`.

Chạy bằng Airflow:

1. Mở `http://localhost:8081`
2. Trigger DAG `gold_mvp_pipeline`
3. Kiểm tra task log có:

```text
[GoldMvpJob] SUCCESS
```

Chạy thủ công:

```bash
bash script/spark/submit_gold_mvp.sh
```

Dry run, chỉ đọc/build/count/schema, không ghi:

```bash
GOLD_DRY_RUN=true bash script/spark/submit_gold_mvp.sh
```

Reset dimension nhỏ trước khi ghi:

```bash
RESET_DIMENSIONS=true bash script/spark/submit_gold_mvp.sh
```

## Kiểm Tra

Spark SQL:

```sql
SELECT COUNT(*) FROM iceberg_catalog.gold.fact_events;
SELECT COUNT(*) FROM iceberg_catalog.gold.fact_sales;
SELECT COUNT(*) FROM iceberg_catalog.gold.dim_time;
SELECT COUNT(*) FROM iceberg_catalog.gold.dim_product;
SELECT COUNT(*) FROM iceberg_catalog.gold.daily_event_summary;

SELECT *
FROM iceberg_catalog.gold.daily_event_summary
ORDER BY event_date
LIMIT 10;
```

PostgreSQL Iceberg catalog metadata:

```bash
docker exec -it postgres-db psql -U bigdata -d agent4da
```

```sql
SELECT catalog_name, table_namespace, table_name, metadata_location
FROM iceberg.iceberg_tables
WHERE catalog_name = 'iceberg_catalog'
  AND table_namespace = 'gold'
ORDER BY table_name;
```

## Write Mode

`GOLD_WRITE_MODE=overwrite_partitions`
: Mặc định cho MVP. `fact_events`, `fact_sales`, `daily_event_summary`, `dim_time` ghi bằng Iceberg `overwritePartitions()`.

`GOLD_WRITE_MODE=append`
: Dễ trùng khi chạy lại. Chỉ nên dùng khi biết rõ dữ liệu đầu vào là incremental.

`dim_product`
: Mặc định append sau khi dedup trong batch. Có thể trùng across runs nếu `RESET_DIMENSIONS=false`. Bật `RESET_DIMENSIONS=true` trong dev/test để `DELETE FROM dim_product` và nạp lại.

MERGE INTO để idempotency tốt hơn sẽ làm ở stage sau nếu cần.

## Known Issues

- Thiếu Iceberg jar: `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog`.
- Thiếu PostgreSQL JDBC jar: `No suitable driver for jdbc:postgresql`.
- Thiếu S3A jar: `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`.
- Table chưa tồn tại: chạy Stage 2 `gold_schema_init_pipeline` trước.
- Silver path chưa có dữ liệu: kiểm tra `s3a://silver/ecommerce_events/`.
- `fact_sales` có thể 0 row nếu Silver chưa có purchase event.
- Duplicate dimension nếu chạy lại nhiều lần với `RESET_DIMENSIONS=false`.

## Env Debug

| Env | Default |
| --- | --- |
| `MINIO_ENDPOINT` | `http://minio:9000` |
| `MINIO_ACCESS_KEY` | `admin` |
| `MINIO_SECRET_KEY` | `Admin123!` |
| `MINIO_BUCKET_SILVER` | `silver` |
| `SILVER_EVENTS_PATH` | `s3a://silver/ecommerce_events/` |
| `ICEBERG_CATALOG_NAME` | `iceberg_catalog` |
| `ICEBERG_NAMESPACE` | `gold` |
| `ICEBERG_WAREHOUSE` | `s3a://gold/warehouse/` |
| `ICEBERG_JDBC_URI` | `jdbc:postgresql://postgres-db:5432/agent4da` |
| `ICEBERG_JDBC_USER` | `bigdata` |
| `ICEBERG_JDBC_PASSWORD` | `#3Bigdata` |
| `ICEBERG_JDBC_SCHEMA` | `iceberg` |
| `GOLD_WRITE_MODE` | `overwrite_partitions` |
| `GOLD_VALIDATE_TABLES` | `true` |
| `GOLD_DRY_RUN` | `false` |
| `RESET_DIMENSIONS` | `false` |
