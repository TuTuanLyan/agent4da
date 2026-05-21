# Gold Test Refactor

## 1. Vi sao bo hau to `_test`

Gold table name nen giu semantic that cua bang: staging, fact, dimension. Moi
truong test/production nen duoc phan biet bang physical location hoac env/config,
khong phai bang ten semantic.

Sau refactor, khi chuyen test -> production, chi can doi base path/location sang
bucket production nhu `s3a://gold/...`; khong can doi namespace/table name va
khong can doi semantic schema.

## 2. Naming sau refactor

```text
iceberg_catalog.gold_staging.stg_events
iceberg_catalog.gold.fact_events
iceberg_catalog.gold.fact_sales
iceberg_catalog.gold.dim_time
iceberg_catalog.gold.dim_product
iceberg_catalog.gold.dim_user
iceberg_catalog.gold.dim_session
iceberg_catalog.gold.daily_event_summary
iceberg_catalog.gold.daily_product_summary
iceberg_catalog.gold.daily_category_summary
iceberg_catalog.gold.daily_brand_summary
```

## 3. Physical location hien tai

Du ten bang da theo production-style, data vat ly van nam trong bucket test:

```text
s3a://test/gold_staging/stg_events
s3a://test/gold/fact_events
s3a://test/gold/fact_sales
s3a://test/gold/dim_time
s3a://test/gold/dim_product
s3a://test/gold/dim_user
s3a://test/gold/dim_session
s3a://test/gold/daily_event_summary
s3a://test/gold/daily_product_summary
s3a://test/gold/daily_category_summary
s3a://test/gold/daily_brand_summary
```

## 4. File da tao/chinh sua

- `code/spark/gold/config.py`: constants naming/path va Spark/Iceberg runtime helper.
- `code/spark/gold/identifiers.py`: validate catalog/namespace/table va location.
- `code/spark/gold/validators.py`: common column/null/unique/table/count checks.
- `code/spark/gold/ddl.py`: common `CREATE NAMESPACE` va `CREATE TABLE USING iceberg`.
- `code/spark/gold/readers.py`: common Iceberg table readers.
- `code/spark/gold/writers.py`: common full refresh writer.
- `code/spark/gold/staging.py`: staging transform tu Silver valid events.
- `code/spark/gold/facts.py`: fact_events/fact_sales transforms.
- `code/spark/gold/dimensions.py`: dim_time/dim_product/dim_user/dim_session transforms.
- `code/spark/gold/summaries.py`: daily event/product/category/brand summary transforms.
- `code/spark/gold/tasks/gold_prepare_events.py`: task orchestration ngan cho staging.
- `code/spark/gold/tasks/gold_build_facts.py`: task orchestration ngan cho facts.
- `code/spark/gold/tasks/gold_build_dimensions.py`: task orchestration ngan cho dimensions.
- `code/spark/gold/tasks/gold_build_summaries.py`: task orchestration cho summaries.
- `code/airflow/dags/gold_pipeline.py`: DAG Gold chain moi.
- `notebook/gold_view.ipynb`: view/query names moi.
- `docs/GOLD_STAGING_TASK.md`: cap nhat naming staging moi, bo hau to `_test`.
- `docs/GOLD_FACTS_TASK.md`: cap nhat naming facts moi va DAG chain co dimensions.
- `docs/GOLD_SUMMARIES_TASK.md`: tai lieu Stage 4 summaries.
- `docs/GOLD_REFACTOR_TEST_ENV.md`: tai lieu refactor nay.

## 5. Helper dung chung

- `identifiers.py`: chan identifier rong/ky tu nguy hiem, tao full table name,
  assert location test phai nam duoi `s3a://test/`.
- `validators.py`: check required columns, non-null key, unique key, required table,
  count equality.
- `ddl.py`: tao namespace/table Iceberg voi location da validate.
- `readers.py` va `writers.py`: doc Iceberg table va ghi full refresh dung chung.

## 6. JAR/dependency

Khong dung `--packages`. Khong tai JAR tu Maven/Ivy. Khong co logic download JAR.

JAR duoc mount san trong container:

```text
/opt/project/jars
```

Gold DAG dung local mounted JAR tu `dag_common.BASE_JARS` va them:

```text
/opt/project/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
/opt/project/jars/postgresql-42.7.4.jar
```

Airflow va Spark worker deu mount `./jars:/opt/project/jars:ro`.

## 7. Cach chay bang Airflow

DAG:

```text
gold_pipeline
```

Nodes:

```text
gold_prepare_events
gold_build_facts
gold_build_dimensions
gold_build_daily_event_summary
gold_build_daily_product_summary
gold_build_daily_category_summary
gold_build_daily_brand_summary
```

Dependency:

```text
gold_prepare_events
>> gold_build_facts
>> gold_build_dimensions
>> [
  gold_build_daily_event_summary,
  gold_build_daily_product_summary,
  gold_build_daily_category_summary,
  gold_build_daily_brand_summary
]
```

Command tham khao:

```bash
docker exec -it airflow airflow dags list
docker exec -it airflow airflow tasks list gold_pipeline
docker exec -it airflow airflow dags trigger gold_pipeline
```

Khong trigger DAG trong task refactor nay.

## 8. Cach chay tay bang spark-submit

Command tham khao. Dung `--jars` voi JAR local mounted, khong dung `--packages`.

```bash
JARS="/opt/project/jars/org.apache.hadoop_hadoop-aws-3.4.2.jar,/opt/project/jars/org.apache.hadoop_hadoop-client-api-3.4.2.jar,/opt/project/jars/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar,/opt/project/jars/software.amazon.awssdk_bundle-2.29.52.jar,/opt/project/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar,/opt/project/jars/postgresql-42.7.4.jar"
CLASSPATH="${JARS//,/:}"
```

Staging:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars "$JARS" \
  --driver-class-path "$CLASSPATH" \
  --conf "spark.executor.extraClassPath=$CLASSPATH" \
  /opt/project/code/spark/gold/tasks/gold_prepare_events.py \
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
  --jars "$JARS" \
  --driver-class-path "$CLASSPATH" \
  --conf "spark.executor.extraClassPath=$CLASSPATH" \
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

Dimensions:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars "$JARS" \
  --driver-class-path "$CLASSPATH" \
  --conf "spark.executor.extraClassPath=$CLASSPATH" \
  /opt/project/code/spark/gold/tasks/gold_build_dimensions.py \
  --catalog-name iceberg_catalog \
  --source-namespace gold \
  --target-namespace gold \
  --staging-namespace gold_staging \
  --staging-table stg_events \
  --fact-events-table fact_events \
  --fact-sales-table fact_sales \
  --refresh-mode full_refresh
```

Summaries:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars "$JARS" \
  --driver-class-path "$CLASSPATH" \
  --conf "spark.executor.extraClassPath=$CLASSPATH" \
  /opt/project/code/spark/gold/tasks/gold_build_summaries.py \
  --catalog-name iceberg_catalog \
  --source-namespace gold \
  --target-namespace gold \
  --summary all \
  --refresh-mode full_refresh
```

## 9. Cach xem bang notebook

Notebook:

```text
/home/lyan/Project/BigData/Agent4DA/notebook/gold_view.ipynb
```

Real data:

```python
spark.table("iceberg_catalog.gold_staging.stg_events").show(20, truncate=False)
spark.table("iceberg_catalog.gold.fact_events").show(20, truncate=False)
spark.table("iceberg_catalog.gold.fact_sales").show(20, truncate=False)
spark.table("iceberg_catalog.gold.dim_time").show(20, truncate=False)
spark.table("iceberg_catalog.gold.dim_product").show(20, truncate=False)
spark.table("iceberg_catalog.gold.dim_user").show(20, truncate=False)
spark.table("iceberg_catalog.gold.dim_session").show(20, truncate=False)
spark.table("iceberg_catalog.gold.daily_event_summary").show(20, truncate=False)
spark.table("iceberg_catalog.gold.daily_product_summary").show(20, truncate=False)
spark.table("iceberg_catalog.gold.daily_category_summary").show(20, truncate=False)
spark.table("iceberg_catalog.gold.daily_brand_summary").show(20, truncate=False)
```

Metadata neu Iceberg ho tro:

```sql
SELECT * FROM iceberg_catalog.gold.fact_events.snapshots
SELECT * FROM iceberg_catalog.gold.fact_events.files
```

## 10. Luu y PostgreSQL catalog

PostgreSQL JDBC catalog chi luu Iceberg metadata nhu namespace, table metadata,
snapshot/catalog pointer. PostgreSQL khong chua full event rows. Real data files
nam trong MinIO bucket `test`.

## 11. Troubleshooting catalog pointer cu

Sau khi bo hau to `_test`, table name semantic nhu `iceberg_catalog.gold.fact_events`
co the trung voi entry cu trong JDBC catalog. Neu entry cu tro toi location production
hoac metadata da bi xoa, Spark co the fail ngay tai `CREATE TABLE IF NOT EXISTS`
voi loi dang:

```text
FileNotFoundException: s3a://gold/warehouse/gold/fact_events/metadata/...metadata.json
```

`ddl.py` hien co guard cho test full-refresh:

- Location dau vao cua table phai nam duoi `s3a://test/`.
- Truoc khi create table, task doc row trong JDBC catalog `iceberg.iceberg_tables`.
  Neu row hien tai cua dung table dang tro ra ngoai `s3a://test/`, task xoa catalog
  entry do de tao lai table test tai location moi.
- Neu Iceberg catalog pointer bi broken vi metadata file khong con ton tai, task se
  xoa truc tiep row catalog stale trong `iceberg.iceberg_tables`, sau do recreate
  table tai test location. Dung direct JDBC cleanup vi Spark SQL `DROP TABLE` cung
  co the fail khi metadata pointer da gay.
- Sau khi create, task doc location that cua table va fail neu location khong nam
  duoi `s3a://test/`. Diem nay giup tranh viec vo tinh ghi vao `s3a://gold/...`.

Neu automatic drop cung fail, can clean/drop catalog entry stale trong Iceberg JDBC
catalog roi chay lai DAG. PostgreSQL van chi la catalog metadata, khong phai noi
luu full event rows.

## 12. Compile/check

```bash
python -m py_compile code/spark/gold/tasks/gold_prepare_events.py
python -m py_compile code/spark/gold/tasks/gold_build_facts.py
python -m py_compile code/spark/gold/tasks/gold_build_dimensions.py
python -m py_compile code/spark/gold/tasks/gold_build_summaries.py
python -m py_compile code/airflow/dags/gold_pipeline.py
python -m compileall code/spark/gold
```

Khong trigger DAG. Khong chay `spark-submit`. Khong chay notebook.
