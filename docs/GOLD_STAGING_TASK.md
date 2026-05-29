# Gold Staging Task

## 1. Muc tieu task

Stage 1 cua Gold Layer tao staging table tu Silver valid events de build Gold
truoc. Day chua phai Gold production, chua build fact/dim/summary va chua
trigger Airflow DAG.

Task moi ghi vao Iceberg table tren MinIO bucket `gold`, mac dinh tai:

```text
s3a://gold/gold_staging/stg_events
```

## 2. File da tao/chinh sua

- `code/spark/gold/__init__.py`: khai bao package Gold layer.
- `code/spark/gold/tasks/__init__.py`: khai bao package cho runnable Gold tasks.
- `code/spark/gold/tasks/gold_prepare_events.py`: PySpark script doc lap, co
  `main()`, argparse, doc Silver Parquet, filter/dedup, tao namespace/table
  Iceberg va full refresh vao staging table test.
- `docs/GOLD_STAGING_TASK.md`: tai lieu task nay, cach compile va cach chay
  thu cong sau nay.

## 3. Input

Silver path mac dinh:

```text
s3a://silver/ecommerce_events/
```

Dieu kien loc:

- `is_valid = true`
- `event_fingerprint IS NOT NULL`

## 4. Logic xu ly

Script check cac required columns cua Silver truoc khi transform. Neu thieu cot,
script raise `ValueError` voi message liet ke ro cac cot thieu.

Required columns:

```text
event_fingerprint, source_event_id, event_ts, event_date, event_year,
event_month, event_day, event_hour, event_type, product_id, category_id,
category_code, category_l1, category_l2, category_l3, brand, price, user_id,
user_session, kafka_ts, kafka_partition, kafka_offset, silver_processed_at,
is_valid
```

Dedup theo `event_fingerprint`. Khi trung fingerprint, giu 1 record theo uu tien:

1. `bronze_ingested_at DESC` neu cot nay ton tai trong Silver.
2. `kafka_ts DESC`.
3. `kafka_partition DESC`.
4. `kafka_offset DESC`.

Mapping va cot moi:

- `user_session` duoc rename/cast thanh `session_id`.
- `time_id = date_format(event_ts, "yyyyMMddHH")`.
- `gold_processed_at = current_timestamp()`.
- `price` duoc cast sang `decimal(18,2)`.

## 5. Output

Iceberg table test mac dinh:

```text
iceberg_catalog.gold_staging.stg_events
```

Physical location mac dinh:

```text
s3a://gold/gold_staging/stg_events
```

Output schema:

```text
event_fingerprint string
source_event_id string
time_id string
event_ts timestamp
event_date date
event_year int
event_month int
event_day int
event_hour int
event_type string
product_id bigint
category_id bigint
category_code string
category_l1 string
category_l2 string
category_l3 string
brand string
price decimal(18,2)
user_id bigint
session_id string
kafka_ts timestamp
kafka_partition int
kafka_offset bigint
silver_processed_at timestamp
gold_processed_at timestamp
```

Table duoc tao bang Spark SQL:

- `CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.gold_staging`
- `CREATE TABLE IF NOT EXISTS ... USING iceberg`
- `LOCATION 's3a://gold/gold_staging/stg_events'`
- `PARTITIONED BY (event_date)`
- `TBLPROPERTIES ('format-version'='2')`

## 6. Database/Catalog ghi gi

PostgreSQL JDBC Catalog chi luu Iceberg metadata/catalog information, vi du:
namespace, table pointer, snapshot/catalog metadata. PostgreSQL khong luu full
event rows.

Event rows nam trong data files tren MinIO tai `s3a://gold/gold_staging/stg_events`
hoac `--output-path` duoc truyen vao. Metadata duoc tao/cap nhat khi Spark chay:

- `CREATE NAMESPACE`
- `CREATE TABLE`
- Iceberg write/`INSERT OVERWRITE`

## 7. Cach check compile

Chi check compile/syntax Python:

```bash
python -m py_compile code/spark/gold/tasks/gold_prepare_events.py
python -m compileall code/spark/gold
```

Khong can trigger DAG. Khong can chay `spark-submit` that trong task nay.

## 8. Cach chay thu cong sau nay

Day chi la tham khao, khong chay trong task implement nay. Jar da duoc mount san
trong `/opt/project/jars`; khong tai lai jar moi lan chay job.

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-class-path "/opt/project/jars/*" \
  --conf "spark.executor.extraClassPath=/opt/project/jars/*" \
  --conf "spark.pyspark.python=/usr/bin/python3" \
  --conf "spark.pyspark.driver.python=/usr/local/bin/python3" \
  /opt/project/code/spark/gold/tasks/gold_prepare_events.py \
  --silver-path s3a://silver/ecommerce_events/ \
  --catalog-name iceberg_catalog \
  --namespace gold_staging \
  --output-table stg_events \
  --output-path s3a://gold/gold_staging/stg_events \
  --refresh-mode full_refresh
```

Runtime env can co:

```text
MINIO_ENDPOINT
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
ICEBERG_JDBC_URI
ICEBERG_JDBC_USER
ICEBERG_JDBC_PASSWORD
ICEBERG_JDBC_SCHEMA
```

`GOLD_STAGING_ICEBERG_WAREHOUSE` co the duoc set neu muon override warehouse
staging. Neu khong set, script dung `s3a://gold/gold_staging/warehouse/`.

## 9. Xem data bang code/notebook

Co the dung notebook:

```text
/home/lyan/Project/BigData/Agent4DA/notebook/gold_view.ipynb
```

Trong notebook, tao SparkSession voi cung Iceberg JDBC catalog config va jar mount
san, sau do query ca row data that va metadata:

```python
table = "iceberg_catalog.gold_staging.stg_events"

spark.sql(f"SELECT * FROM {table} LIMIT 20").show(truncate=False)
spark.sql(f"SELECT COUNT(*) AS row_count FROM {table}").show(truncate=False)

spark.sql(f"SELECT * FROM {table}.snapshots").show(truncate=False)
spark.sql(f"SELECT * FROM {table}.files").show(truncate=False)
```

Hai cau query dau doc real event rows trong Iceberg table. Hai cau query sau doc
Iceberg metadata de xem snapshot/data files hien tai.

## 10. Gioi han hien tai

- Chi support `full_refresh`.
- Chua implement incremental refresh.
- Chua build fact/dim/summary.
- Chua trigger Airflow DAG.
- Chua validate bang query Trino.
- Bucket `test` can ton tai hoac duoc tao truoc trong MinIO.
