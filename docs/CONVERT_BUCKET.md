# Gold Bucket Conversion

## 1. Muc Tieu Thay Doi

Default physical location cua Gold da duoc chuyen tu bucket `test` sang bucket `gold`.
Ten Iceberg catalog/schema/table khong doi, vi vay SQL identifier van giu nguyen:

- `iceberg_catalog.gold_staging.stg_events`
- `iceberg_catalog.gold.*`
- `iceberg_catalog.metadata.*`

Semantic metadata cho AI Agent khong doi noi dung nghiep vu; chi doi noi luu vat ly tren MinIO.

## 2. Truoc Va Sau

| Phan | Truoc | Sau |
| --- | --- | --- |
| Gold staging | `s3a://test/gold_staging` | `s3a://gold/gold_staging` |
| Gold tables | `s3a://test/gold` | `s3a://gold/gold` |
| Semantic metadata | `s3a://test/metadata` | `s3a://gold/metadata` |

## 3. File Da Chinh

- `code/spark/gold/config.py`: default bucket/base path va allowed prefixes.
- `code/spark/gold/ddl.py`: recreate catalog entry khi table dang tro toi location cu.
- `code/spark/gold/tasks/gold_prepare_events.py`: default staging output path.
- `code/spark/gold/tasks/gold_build_facts.py`: default fact output paths.
- `code/spark/gold/tasks/gold_build_dimensions.py`: default dimension output paths.
- `code/spark/gold/tasks/gold_build_summaries.py`: default summary output paths.
- `code/spark/gold/tasks/gold_build_metadata.py`: default metadata base path.
- `code/airflow/dags/gold_pipeline.py`: Airflow application args va warehouse defaults.
- `code/airflow/dags/gold_metadata_pipeline.py`: metadata warehouse/base path defaults.
- `notebook/gold_view.ipynb`: default warehouse cho notebook view.

## 4. Cach Doi Bucket Sau Nay

Default hien tai lay tu env truoc, sau do moi fallback ve bucket `gold`.
Neu muon doi bucket cho ca Gold pipeline, dat mot trong cac bien:

- `MINIO_BUCKET_GOLD=test`: doi bucket MinIO, tu sinh `s3a://test/...`.
- `GOLD_STORAGE_ROOT=s3a://test`: doi root S3A truc tiep.

Neu can chi tung vung storage rieng, dat cac base path cu the:

- `GOLD_STAGING_BASE_PATH=s3a://test/gold_staging`
- `GOLD_BASE_PATH=s3a://test/gold`
- `GOLD_METADATA_BASE_PATH=s3a://test/metadata`

Trong code Spark, cac default tuong ung nam tai `code/spark/gold/config.py`:

- `DEFAULT_GOLD_BUCKET`
- `DEFAULT_GOLD_STORAGE_ROOT`
- `DEFAULT_STAGING_BASE_PATH`
- `DEFAULT_GOLD_BASE_PATH`
- `DEFAULT_METADATA_BASE_PATH`

Neu can debug lai tren bucket `test`, dung:

- `s3a://test/gold_staging`
- `s3a://test/gold`
- `s3a://test/metadata`

Neu chay default hien tai, dung:

- `s3a://gold/gold_staging`
- `s3a://gold/gold`
- `s3a://gold/metadata`

Khong can doi table names hoac schema names.

## 5. MinIO Bucket

Bucket `gold` phai ton tai truoc khi chay Gold pipeline neu project chua tu tao bucket nay.
Khong xoa bucket `test`; bucket nay van co the dung de debug/backward-compatible.

## 6. Trino Sau Khi Doi Bucket

Trino doc location tu Iceberg table metadata trong PostgreSQL JDBC catalog. Neu bang da duoc tao tu truoc o bucket `test`, chi doi config se khong tu di chuyen data hay metadata cu sang bucket `gold`.

Can full refresh/recreate bang de Iceberg metadata tro sang:

- `s3a://gold/gold_staging/...`
- `s3a://gold/gold/...`
- `s3a://gold/metadata/...`

## 7. Kiem Tra Sau Khi Chay Lai

Co the kiem tra bang Spark SQL hoac Trino SQL:

```sql
SELECT * FROM iceberg.metadata.semantic_table_catalog LIMIT 10;
SELECT * FROM iceberg.gold.daily_event_summary LIMIT 10;
```

List semantic tables cho Agent:

```sql
SELECT table_name, display_name, purpose, grain
FROM iceberg.metadata.semantic_table_catalog
ORDER BY table_name;
```
