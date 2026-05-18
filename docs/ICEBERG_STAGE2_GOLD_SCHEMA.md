# Iceberg Gold Layer - Stage 2 Gold MVP Schema

> Deprecated historical stage doc. Gold schema creation now runs through
> `code/spark/gold_job.py` with `GOLD_RUN_MODE=schema_only`.
> Legacy scripts/jobs are archived under `script/spark/_archive/` and
> `code/spark/_archive/`.

Stage 2 chỉ khởi tạo schema Gold MVP trên Apache Iceberg. Stage này chưa nạp dữ liệu thật từ Silver sang Gold, chưa thêm Trino, chưa tạo semantic metadata catalog cho Agent và chưa mở rộng các bảng Gold ngoài MVP.

## Bảng Gold MVP

| Table | Grain | Mục đích |
| --- | --- | --- |
| `dim_time` | 1 row per event hour | Dimension thời gian cho phân tích theo giờ/ngày/tháng |
| `dim_product` | 1 row per product | Dimension sản phẩm, category hierarchy, brand và observed price stats |
| `fact_events` | 1 row per clean event | Fact chính cho event ecommerce sạch từ Silver |
| `fact_sales` | 1 row per purchase event | Fact purchase/sales, quantity mặc định là 1 |
| `daily_event_summary` | 1 row per event date | Summary ngày cho dashboard/query nhanh |

Tất cả bảng nằm trong:

```text
iceberg_catalog.gold.<table_name>
```

## Mapping Dự Kiến Từ Silver

| Silver field | Gold mapping dự kiến |
| --- | --- |
| `source_event_id` | `fact_events.event_id`, `fact_sales.sale_id` |
| `event_ts` | `dim_time`, `fact_events.event_ts`, `fact_sales.sale_ts` |
| `event_date` | `daily_event_summary.event_date` |
| `event_type` | event flags, event counts, purchase filters |
| `product_id`, `category_*`, `brand` | `dim_product` |
| `price` | `fact_sales.gross_amount`, observed prices, revenue |

Stage 3 sẽ hiện thực mapping này bằng ETL thật từ `silver.ecommerce_events`.

## Cách Chạy

Điều kiện trước khi chạy:

1. Stage 1 đã chạy thành công.
2. JAR Iceberg và PostgreSQL JDBC đã có trong `jars/`.
3. PostgreSQL schema `iceberg` đã được init.
4. MinIO bucket `gold` tồn tại.

Chạy bằng Airflow UI:

1. Mở `http://localhost:8081`
2. Trigger DAG `gold_schema_init_pipeline`
3. Kiểm tra task log có dòng:

```text
[GoldSchemaInitJob] SUCCESS
```

Chạy thủ công:

```bash
bash script/spark/submit_gold_schema_init.sh
```

Mặc định job không drop bảng và không insert dữ liệu test.

Reset bảng trong dev/test:

```bash
RESET_GOLD_SCHEMA=true bash script/spark/submit_gold_schema_init.sh
```

Insert test data nhỏ ngày `2020-01-01`:

```bash
ENABLE_GOLD_SCHEMA_TEST_DATA=true bash script/spark/submit_gold_schema_init.sh
```

Production không bật `RESET_GOLD_SCHEMA=true`.

## Cách Kiểm Tra

Spark SQL:

```sql
SHOW TABLES IN iceberg_catalog.gold;
DESCRIBE TABLE iceberg_catalog.gold.fact_events;
```

PostgreSQL JDBC Catalog metadata:

```bash
docker exec -it postgres-db psql -U bigdata -d agent4da
```

```sql
\dt iceberg.*

SELECT catalog_name, table_namespace, table_name, metadata_location
FROM iceberg.iceberg_tables
WHERE catalog_name = 'iceberg_catalog'
  AND table_namespace = 'gold'
ORDER BY table_name;
```

MinIO:

- Bucket: `gold`
- Prefix chính: `warehouse/gold/`
- Các bảng MVP sẽ có metadata/table path dưới prefix này sau khi chạy job.

## Lưu Ý

- Stage 2 chỉ tạo schema.
- Dữ liệu thật sẽ được nạp ở Stage 3.
- `RESET_GOLD_SCHEMA=true` sẽ `DROP TABLE IF EXISTS` rồi tạo lại bảng, chỉ dùng trong môi trường dev/test.
- Nếu `RESET_GOLD_SCHEMA` không phải `true`, job chỉ chạy `CREATE TABLE IF NOT EXISTS`.
- `ENABLE_GOLD_SCHEMA_TEST_DATA=false` là mặc định an toàn.
- DAG/script dùng local JAR classpath, không dùng `--packages` và không truyền `--jars` mặc định để tránh Spark copy JAR vào `log/spark/app-*`.

## Env Cần Theo Dõi Khi Debug

| Env | Default | Ghi chú |
| --- | --- | --- |
| `MINIO_ENDPOINT` | `http://minio:9000` | Endpoint S3A nội bộ Docker network |
| `MINIO_ACCESS_KEY` | `admin` | MinIO access key |
| `MINIO_SECRET_KEY` | `change_me` | MinIO secret key |
| `ICEBERG_CATALOG_NAME` | `iceberg_catalog` | Spark catalog name |
| `ICEBERG_NAMESPACE` | `gold` | Iceberg namespace/schema |
| `ICEBERG_WAREHOUSE` | `s3a://gold/warehouse/` | Warehouse vật lý trên MinIO |
| `ICEBERG_JDBC_URI` | `jdbc:postgresql://postgres-db:5432/agent4da` | PostgreSQL JDBC URI |
| `ICEBERG_JDBC_USER` | `bigdata` | PostgreSQL user |
| `ICEBERG_JDBC_PASSWORD` | `change_me` | PostgreSQL password |
| `ICEBERG_JDBC_SCHEMA` | `iceberg` | PostgreSQL schema chứa Iceberg JDBC Catalog metadata |
| `RESET_GOLD_SCHEMA` | `false` | `true` mới drop/recreate Gold MVP tables |
| `ENABLE_GOLD_SCHEMA_TEST_DATA` | `false` | `true` mới insert vài dòng test ngày `2020-01-01` |

Các env này được truyền trong DAG qua Spark conf/executor env và trong script qua `docker exec -e`.
