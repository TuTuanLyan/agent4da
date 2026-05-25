# Gold Summaries Task

## 1. Muc tieu task

Stage 4 Gold build summary tables phuc vu dashboard va AI Agent. Stage nay chay
sau dimensions, dung du lieu tu:

```text
iceberg_catalog.gold.fact_events
iceberg_catalog.gold.fact_sales
iceberg_catalog.gold.dim_product
```

Task khong doc Bronze, khong doc Silver va khong doc staging neu khong can.
Table name giu semantic that, physical data mac dinh nam trong bucket `gold` cua
MinIO. Neu can test/debug bang bucket khac, xem `docs/CONVERT_BUCKET.md`.

## 2. Vi sao summaries chay sau dimensions

Dimensions mo ta ngu nghia cho facts. Product/category/brand summaries can brand
va category hierarchy tu `dim_product`, nen chay sau dimensions giup logic chac
hon, de giai thich hon, va tranh lap lai enrichment logic tu staging.

## 3. Vi sao summary co the song song

Bon summary tables doc chung facts/dimensions da on dinh, nhung ghi ra bon output
Iceberg table khac nhau. Khong co bang nao ghi chung location, nen DAG tach thanh
4 node song song sau `gold_build_dimensions`:

```text
gold_build_daily_event_summary
gold_build_daily_product_summary
gold_build_daily_category_summary
gold_build_daily_brand_summary
```

Neu tai nguyen Spark worker khong du, co the giam parallelism trong Airflow/Spark
sau nay, nhung thiet ke node rieng giup debug va retry tung summary ro rang hon.

## 4. File da tao/chinh sua

- `code/spark/gold/summaries.py`: transform, schema SQL, column order va validation
  cho daily summaries.
- `code/spark/gold/tasks/gold_build_summaries.py`: PySpark task doc lap, co
  argparse va `--summary all|event|product|category|brand`.
- `code/spark/gold/config.py`: them constants table name cho summary tables.
- `code/airflow/dags/gold_pipeline.py`: them 4 summary nodes chay song song sau
  dimensions.
- `docs/GOLD_SUMMARIES_TASK.md`: tai lieu Stage 4 summaries.
- `docs/GOLD_REFACTOR_TEST_ENV.md`: cap nhat naming/DAG/notebook cho summaries.
- `notebook/gold_view.ipynb`: them cell xem real rows/count/top revenue/metadata
  cua summary tables.

## 5. Input

```text
iceberg_catalog.gold.fact_events
iceberg_catalog.gold.fact_sales
iceberg_catalog.gold.dim_product
```

## 6. Output

```text
iceberg_catalog.gold.daily_event_summary
s3a://gold/gold/daily_event_summary

iceberg_catalog.gold.daily_product_summary
s3a://gold/gold/daily_product_summary

iceberg_catalog.gold.daily_category_summary
s3a://gold/gold/daily_category_summary

iceberg_catalog.gold.daily_brand_summary
s3a://gold/gold/daily_brand_summary
```

## 7. Logic tung summary

`daily_event_summary`

- Grain: 1 row = 1 `event_date`.
- Source: `fact_events`, revenue aggregate tu `fact_sales`.
- Metrics: total events, views, carts, remove_from_cart, purchases, unique users,
  sessions, products, events, revenue, avg event price.
- Revenue: aggregate `fact_sales` truoc theo `sale_date`, sau do left join vao
  event aggregate theo date.

`daily_product_summary`

- Grain: 1 row = `event_date + product_id`.
- Source: `fact_events`, `fact_sales`, `dim_product`.
- `summary_id = yyyyMMdd + "_" + product_id`.
- Product attributes lay tu `dim_product`.
- Revenue aggregate truoc theo `sale_date + product_id`, sau do join vao event
  aggregate.

`daily_category_summary`

- Grain: 1 row = `event_date + category_l1 + category_l2 + category_l3`.
- Source: `fact_events`, `fact_sales`, `dim_product`.
- Null category duoc coalesce thanh `unknown`.
- Revenue aggregate truoc theo `sale_date + category_l1 + category_l2 + category_l3`.

`daily_brand_summary`

- Grain: 1 row = `event_date + brand`.
- Source: `fact_events`, `fact_sales`, `dim_product`.
- Null brand duoc coalesce thanh `unknown`.
- Revenue aggregate truoc theo `sale_date + brand`.

Rate formulas:

```text
conversion_rate = purchase_count / view_count
cart_to_purchase_rate = purchase_count / cart_count
```

Neu denominator bang 0 hoac null thi rate = `0.0` de dashboard de hien thi.
Boolean flags duoc cast qua integer khi sum. Revenue luon aggregate tu
`fact_sales` truoc theo dung grain de tranh fan-out khi join voi event rows.

## 8. Validation

- `daily_event_summary` unique theo `event_date`.
- `daily_product_summary`, `daily_category_summary`, `daily_brand_summary` unique
  va non-null theo `summary_id`.
- `daily_event_summary.total_events` khop count `fact_events` theo `event_date`.
- `daily_event_summary.total_purchases` khop purchase count theo `event_date`.
- `daily_event_summary.total_revenue` khop sum `fact_sales.gross_amount` theo
  `sale_date`.
- Validation fail thi raise exception de Airflow task fail.

## 9. Database/Catalog ghi gi

PostgreSQL JDBC Catalog chi ghi Iceberg metadata nhu namespace, table metadata,
snapshot/catalog pointer. PostgreSQL khong chua full summary rows.

Summary rows that nam trong data files tren MinIO bucket `gold`. Co the xem
Iceberg metadata qua Spark metadata tables nhu `.snapshots` va `.files` neu ho tro.

## 10. JAR/dependency

Khong dung `--packages`. Khong tai JAR tu Maven/Ivy. Khong co logic download JAR.

JAR duoc mount san trong container tai:

```text
/opt/project/jars
```

Gold DAG dung `dag_common.BASE_JARS` va them:

```text
/opt/project/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
/opt/project/jars/postgresql-42.7.4.jar
```

Airflow va Spark worker deu mount `./jars:/opt/project/jars:ro`.

## 11. Cach chay bang Airflow

DAG hien tai:

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

Khong tu trigger DAG trong task implement nay.

## 12. Cach chay tay bang spark-submit

Command tham khao. Dung JAR local mounted, khong dung `--packages`.

```bash
JARS="/opt/project/jars/org.apache.hadoop_hadoop-aws-3.4.2.jar,/opt/project/jars/org.apache.hadoop_hadoop-client-api-3.4.2.jar,/opt/project/jars/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar,/opt/project/jars/software.amazon.awssdk_bundle-2.29.52.jar,/opt/project/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar,/opt/project/jars/postgresql-42.7.4.jar"
CLASSPATH="${JARS//,/:}"
```

All summaries:

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

Tung summary:

```bash
/opt/spark/bin/spark-submit ... /opt/project/code/spark/gold/tasks/gold_build_summaries.py --summary event
/opt/spark/bin/spark-submit ... /opt/project/code/spark/gold/tasks/gold_build_summaries.py --summary product
/opt/spark/bin/spark-submit ... /opt/project/code/spark/gold/tasks/gold_build_summaries.py --summary category
/opt/spark/bin/spark-submit ... /opt/project/code/spark/gold/tasks/gold_build_summaries.py --summary brand
```

## 13. Cach xem bang notebook

Notebook:

```text
/home/lyan/Project/BigData/Agent4DA/notebook/gold_view.ipynb
```

Real data:

```python
spark.table("iceberg_catalog.gold.daily_event_summary").show(20, truncate=False)
spark.table("iceberg_catalog.gold.daily_product_summary").show(20, truncate=False)
spark.table("iceberg_catalog.gold.daily_category_summary").show(20, truncate=False)
spark.table("iceberg_catalog.gold.daily_brand_summary").show(20, truncate=False)
```

Counts:

```sql
SELECT count(*) FROM iceberg_catalog.gold.daily_event_summary
SELECT count(*) FROM iceberg_catalog.gold.daily_product_summary
SELECT count(*) FROM iceberg_catalog.gold.daily_category_summary
SELECT count(*) FROM iceberg_catalog.gold.daily_brand_summary
```

Samples:

```sql
SELECT * FROM iceberg_catalog.gold.daily_product_summary ORDER BY revenue DESC LIMIT 20
SELECT * FROM iceberg_catalog.gold.daily_category_summary ORDER BY revenue DESC LIMIT 20
SELECT * FROM iceberg_catalog.gold.daily_brand_summary ORDER BY revenue DESC LIMIT 20
```

Metadata neu Iceberg ho tro:

```sql
SELECT * FROM iceberg_catalog.gold.daily_event_summary.snapshots
SELECT * FROM iceberg_catalog.gold.daily_event_summary.files
```

## 14. Compile/check

```bash
python -m py_compile code/spark/gold/tasks/gold_build_summaries.py
python -m py_compile code/airflow/dags/gold_pipeline.py
python -m compileall code/spark/gold
```

Khong xoa `__pycache__`. Khong trigger DAG. Khong chay `spark-submit`. Khong
chay notebook.
