# Iceberg Gold Layer - Stage 4 Gold Extended

> Deprecated historical stage doc. Gold extended loading now runs through
> `code/spark/gold_job.py` with `GOLD_RUN_MODE=extended_only`.
> Legacy scripts/jobs are archived under `script/spark/_archive/` and
> `code/spark/_archive/`.

Stage 4 mở rộng Gold Layer bằng các bảng analytics cho user/session, product, category và brand. Nguồn dữ liệu là các bảng Gold MVP đã nạp ở Stage 3. Stage này chưa tạo semantic metadata catalog, chưa tích hợp Trino/Agent và chưa triển khai MERGE INTO nâng cao.

## Bảng Được Tạo

| Table | Grain | Mục đích |
| --- | --- | --- |
| `dim_user` | 1 row per user | Tổng hợp hành vi và revenue theo user |
| `dim_session` | 1 row per user session | Duration, event counts, purchase flag và session revenue |
| `daily_product_summary` | 1 row per event date and product | Hiệu suất sản phẩm theo ngày |
| `daily_category_summary` | 1 row per event date and category hierarchy | Hiệu suất danh mục theo ngày |
| `daily_brand_summary` | 1 row per event date and brand | Hiệu suất thương hiệu theo ngày |

## Nguồn Dữ Liệu

- `iceberg_catalog.gold.fact_events`
- `iceberg_catalog.gold.fact_sales`
- `iceberg_catalog.gold.dim_product`

## Mapping Chính

User/session metrics:

- `first_seen_at`, `last_seen_at`: min/max event timestamp.
- `total_sessions`, `event_count`: distinct session/count event.
- `view/cart/remove/purchase` counts: từ boolean flags trong `fact_events`.
- `total_revenue`, `session_revenue`: từ `fact_sales.gross_amount`.

Product/category/brand daily metrics:

- `view_count`, `cart_count`, `purchase_count`, `remove_from_cart_count`.
- `unique_users`, `unique_sessions`, `unique_products`.
- `revenue`: purchase price sum.
- `conversion_rate`: `purchase_count / view_count`, safe divide.
- `cart_to_purchase_rate`: `purchase_count / cart_count`, safe divide.

## Cách Chạy

Thứ tự:

1. Chạy Stage 1 smoke test.
2. Chạy Stage 2 `gold_schema_init_pipeline`.
3. Chạy Stage 3 `gold_mvp_pipeline`.
4. Chạy Stage 4 `gold_extended_schema_init_pipeline`.
5. Chạy Stage 4 `gold_extended_pipeline`.

Chạy thủ công:

```bash
bash script/spark/submit_gold_extended_schema_init.sh
bash script/spark/submit_gold_extended.sh
```

Dry run ETL, chỉ count/schema, không ghi:

```bash
GOLD_EXTENDED_DRY_RUN=true bash script/spark/submit_gold_extended.sh
```

Reset extended dimension trong dev/test:

```bash
RESET_EXTENDED_DIMENSIONS=true bash script/spark/submit_gold_extended.sh
```

Reset schema extended trong dev/test:

```bash
RESET_GOLD_EXTENDED_SCHEMA=true bash script/spark/submit_gold_extended_schema_init.sh
```

Production không bật các biến reset nếu chưa có kế hoạch rõ ràng.

## Cách Kiểm Tra

Spark SQL:

```sql
SELECT COUNT(*) FROM iceberg_catalog.gold.dim_user;
SELECT COUNT(*) FROM iceberg_catalog.gold.dim_session;
SELECT * FROM iceberg_catalog.gold.daily_product_summary LIMIT 10;
SELECT * FROM iceberg_catalog.gold.daily_category_summary LIMIT 10;
SELECT * FROM iceberg_catalog.gold.daily_brand_summary LIMIT 10;
```

PostgreSQL metadata:

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

## Write Mode Và Idempotency

- `GOLD_EXTENDED_WRITE_MODE=overwrite_partitions` là mặc định.
- Summary tables và `dim_session` dùng `overwritePartitions()`.
- `dim_user` không partition, nên mặc định append và có thể trùng khi chạy lại.
- Bật `RESET_EXTENDED_DIMENSIONS=true` trong dev/test để `DELETE FROM dim_user` trước khi append.
- MERGE INTO/idempotency nâng cao sẽ làm ở stage sau nếu cần.

## Lưu Ý

- Stage 4 chưa tạo semantic metadata catalog.
- Stage 4 chưa tích hợp Trino/Agent.
- `dim_product` có thể từng bị append trùng ở Stage 3; ETL Stage 4 dedup `dim_product` theo `product_id` trước khi join.
- Nếu `fact_sales` rỗng thì revenue bằng `0.00`, purchase-related summary có thể bằng 0.

## Env Debug

| Env | Default |
| --- | --- |
| `MINIO_ENDPOINT` | `http://minio:9000` |
| `MINIO_ACCESS_KEY` | `admin` |
| `MINIO_SECRET_KEY` | `change_me` |
| `ICEBERG_CATALOG_NAME` | `iceberg_catalog` |
| `ICEBERG_NAMESPACE` | `gold` |
| `ICEBERG_WAREHOUSE` | `s3a://gold/warehouse/` |
| `ICEBERG_JDBC_URI` | `jdbc:postgresql://postgres-db:5432/agent4da` |
| `ICEBERG_JDBC_USER` | `bigdata` |
| `ICEBERG_JDBC_PASSWORD` | `change_me` |
| `ICEBERG_JDBC_SCHEMA` | `iceberg` |
| `RESET_GOLD_EXTENDED_SCHEMA` | `false` |
| `GOLD_EXTENDED_WRITE_MODE` | `overwrite_partitions` |
| `GOLD_EXTENDED_DRY_RUN` | `false` |
| `GOLD_EXTENDED_VALIDATE_TABLES` | `true` |
| `RESET_EXTENDED_DIMENSIONS` | `false` |
