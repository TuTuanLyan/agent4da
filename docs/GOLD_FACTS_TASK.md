# Gold Facts Task

## 1. Muc tieu task

Stage 2 Gold build `fact_events` va `fact_sales` tu
`stg_events`. Day la ban test/debug tren bucket `test` cua MinIO, chua ghi
vao Gold production.

## 2. File da tao/chinh sua

- `code/spark/gold/tasks/gold_build_facts.py`: PySpark script doc lap doc
  staging Iceberg table, build fact_events/fact_sales, validate checks va full
  refresh vao Iceberg test tables.
- `code/airflow/dags/gold_pipeline.py`: DAG manual trigger cho Gold test, gom
  chain `gold_prepare_events >> gold_build_facts >> gold_build_dimensions`.
- `docs/GOLD_FACTS_TASK.md`: tai lieu Stage 2 facts.
- `notebook/gold_view.ipynb`: bo sung/cap nhat cell xem Gold test staging/facts
  bang Spark/Iceberg catalog va jar local.

## 3. Input

Staging table:

```text
iceberg_catalog.gold_staging.stg_events
```

Physical path staging:

```text
s3a://test/gold_staging/stg_events
```

Task fact chi doc tu Iceberg staging table, khong doc truc tiep Silver. Neu
staging table chua ton tai hoac khong doc duoc, task raise loi ro rang va nhac
chay `gold_prepare_events` truoc.

## 4. Logic fact_events

Khoa logic cua fact_events la `event_fingerprint`. Mapping:

```text
event_fingerprint = stg_events.event_fingerprint
source_event_id = stg_events.source_event_id
time_id = stg_events.time_id
event_ts = stg_events.event_ts
event_date = stg_events.event_date
event_type = stg_events.event_type
product_id = stg_events.product_id
user_id = stg_events.user_id
session_id = stg_events.session_id
price = cast(stg_events.price as decimal(18,2))
is_view = event_type == "view"
is_cart = event_type == "cart"
is_remove_from_cart = event_type == "remove_from_cart"
is_purchase = event_type == "purchase"
kafka_partition = stg_events.kafka_partition
kafka_offset = stg_events.kafka_offset
kafka_ts = stg_events.kafka_ts
silver_processed_at = stg_events.silver_processed_at
gold_processed_at = current_timestamp()
```

Output schema:

```text
event_fingerprint string
source_event_id string
time_id string
event_ts timestamp
event_date date
event_type string
product_id bigint
user_id bigint
session_id string
price decimal(18,2)
is_view boolean
is_cart boolean
is_remove_from_cart boolean
is_purchase boolean
kafka_partition int
kafka_offset bigint
kafka_ts timestamp
silver_processed_at timestamp
gold_processed_at timestamp
```

Required checks:

- `event_fingerprint` khong duoc null.
- `fact_events` phai unique theo `event_fingerprint`.

## 5. Logic fact_sales

`fact_sales` doc tu `fact_events` va chi lay `event_type = "purchase"`.
Dataset Kaggle khong co `order_id`/`quantity` that, nen moi purchase event tam
coi `quantity = 1`.

Mapping:

```text
sale_id = event_fingerprint
event_fingerprint = fact_events.event_fingerprint
source_event_id = fact_events.source_event_id
time_id = fact_events.time_id
sale_ts = fact_events.event_ts
sale_date = fact_events.event_date
product_id = fact_events.product_id
user_id = fact_events.user_id
session_id = fact_events.session_id
unit_price = fact_events.price
quantity = 1
gross_amount = unit_price * quantity
gold_processed_at = current_timestamp()
```

Output schema:

```text
sale_id string
event_fingerprint string
source_event_id string
time_id string
sale_ts timestamp
sale_date date
product_id bigint
user_id bigint
session_id string
unit_price decimal(18,2)
quantity int
gross_amount decimal(18,2)
gold_processed_at timestamp
```

Required check: `fact_sales` count phai bang count cua `fact_events` voi
`event_type = 'purchase'`.

## 6. Output

`fact_events`:

```text
iceberg_catalog.gold.fact_events
s3a://test/gold/fact_events
```

`fact_sales`:

```text
iceberg_catalog.gold.fact_sales
s3a://test/gold/fact_sales
```

Tables duoc tao bang `CREATE TABLE IF NOT EXISTS ... USING iceberg LOCATION ...`
tren namespace test `iceberg_catalog.gold`. Ban test hien chua partition de
giam rui ro runtime voi Spark/Iceberg config hien tai.

## 7. Database/Catalog ghi gi

PostgreSQL JDBC Catalog chi ghi metadata cua Iceberg table, vi du namespace,
table metadata, snapshot va catalog pointer tuy Iceberg implementation.
PostgreSQL khong chua full rows cua `fact_events` hoac `fact_sales`.

Real row data nam trong data files tren MinIO bucket `test`:

```text
s3a://test/gold/fact_events
s3a://test/gold/fact_sales
```

Cach phan biet:

- Query PostgreSQL catalog: thay metadata/cac pointer cua Iceberg.
- Query Iceberg table bang Spark: thay rows that cua facts.
- Query Iceberg metadata table nhu `.snapshots`/`.files`: thay snapshot va data
  files ma Iceberg dang quan ly.

## 8. Cach chay bang Airflow

DAG hien co dependency:

```text
gold_prepare_events >> gold_build_facts >> gold_build_dimensions
```

Chi trigger khi can test, khong trigger trong task implement nay:

```bash
docker exec -it airflow airflow dags list
docker exec -it airflow airflow dags trigger gold_pipeline
```

Xem task/log:

```bash
docker exec -it airflow airflow tasks list gold_pipeline
```

Hoac xem Airflow UI/logs theo convention hien co cua repo.

## 9. Cach chay tay bang spark-submit

Day chi la command tham khao. Jar da mount san, khong dung `--packages` va khong
tai lai jar moi lan chay.

Staging:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-class-path "/opt/project/jars/*" \
  --conf "spark.executor.extraClassPath=/opt/project/jars/*" \
  /opt/project/code/spark/gold/tasks/gold_prepare_events.py \
  --silver-path s3a://silver/ecommerce_events/ \
  --catalog-name iceberg_catalog \
  --namespace gold_staging \
  --output-table stg_events \
  --output-path s3a://test/gold_staging/stg_events \
  --refresh-mode full_refresh
```

Facts:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-class-path "/opt/project/jars/*" \
  --conf "spark.executor.extraClassPath=/opt/project/jars/*" \
  /opt/project/code/spark/gold/tasks/gold_build_facts.py \
  --catalog-name iceberg_catalog \
  --staging-namespace gold_staging \
  --staging-table stg_events \
  --target-namespace gold \
  --fact-events-table fact_events \
  --fact-sales-table fact_sales \
  --fact-events-path s3a://test/gold/fact_events \
  --fact-sales-path s3a://test/gold/fact_sales \
  --refresh-mode full_refresh
```

## 10. Cach kiem tra du lieu bang notebook

Notebook:

```text
/home/lyan/Project/BigData/Agent4DA/notebook/gold_view.ipynb
```

Notebook co cell tao SparkSession voi cung Iceberg JDBC catalog config va jar
local trong repo. Cac cell check can xem ca real rows va metadata:

```python
spark.sql("SHOW NAMESPACES IN iceberg_catalog").show(truncate=False)
spark.sql("SHOW TABLES IN iceberg_catalog.gold_staging").show(truncate=False)
spark.sql("SHOW TABLES IN iceberg_catalog.gold").show(truncate=False)

spark.table("iceberg_catalog.gold_staging.stg_events").show(20, truncate=False)
spark.table("iceberg_catalog.gold.fact_events").show(20, truncate=False)
spark.table("iceberg_catalog.gold.fact_sales").show(20, truncate=False)

spark.sql("SELECT count(*) FROM iceberg_catalog.gold.fact_events").show()
spark.sql("SELECT count(*) FROM iceberg_catalog.gold.fact_sales").show()

spark.sql("SELECT * FROM iceberg_catalog.gold.fact_events.snapshots").show(truncate=False)
spark.sql("SELECT * FROM iceberg_catalog.gold.fact_events.files").show(truncate=False)
spark.sql("SELECT * FROM iceberg_catalog.gold.fact_sales.snapshots").show(truncate=False)
spark.sql("SELECT * FROM iceberg_catalog.gold.fact_sales.files").show(truncate=False)
```

Nhap nho: cac query `spark.table(...)`/`SELECT count(*)` doc real rows. Cac query
`.snapshots` va `.files` doc Iceberg metadata.

## 11. Cach kiem tra trong PostgreSQL catalog

Doc `envs/postgre.env` va `envs/iceberg.env` de biet database/schema/user hien
tai. Khong hard-code password trong docs; su dung env cua container.

Command tham khao:

```bash
set -a
. envs/postgre.env
. envs/iceberg.env
set +a

docker exec -it postgres-db psql \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "\\dt ${ICEBERG_JDBC_SCHEMA}.*"
```

Muc tieu la kiem tra catalog metadata trong PostgreSQL, khong ky vong thay full
event/fact rows trong PostgreSQL.

## 12. Compile/check yeu cau

Chi chay compile/syntax:

```bash
python -m py_compile code/spark/gold/tasks/gold_build_facts.py
python -m py_compile code/spark/gold/tasks/gold_build_dimensions.py
python -m py_compile code/airflow/dags/gold_pipeline.py
python -m compileall code/spark/gold
```

Khong trigger DAG. Khong chay `spark-submit` that neu khong duoc yeu cau.

## 13. Gioi han hien tai

- Chi support `full_refresh`.
- Summary da co Stage 4 test/debug; chua build metadata catalog nghiep vu.
- Chua validate bang Trino.
- Bucket `test` phai ton tai tren MinIO hoac duoc tao truoc.
- `fact_sales` la purchase-event fact, khong phai order fact production.
