# Gold Metadata Pipeline

## 1. Metadata pipeline la gi

`gold_metadata_pipeline` publish semantic/business metadata cho AI Agent. Day
khong phai Iceberg technical metadata nhu snapshots/files. Metadata nay giup
Agent biet bang nao hop le, bang dung cho cau hoi gi, grain la gi, cot nao la
metric/dimension/time/join key, metric tinh nhu the nao, va bang nao join an toan.

Noi dung duoc viet ngan gon, uu tien English cho `business_name`/`description`
de SQL Agent on dinh. `agent_synonyms` co them mot so synonym tieng Viet hay dung.

## 2. Vi sao la pipeline rieng

Semantic metadata khong doi moi lan data refresh. Chi can chay lai khi Gold schema,
metric, join, description hoac recommended table thay doi. Tach DAG rieng giup
giam thoi gian xu ly, tranh ghi lap metadata moi lan Gold data refresh, va phu hop
voi manual trigger hon.

Metadata pipeline khong duoc noi vao `gold_pipeline` trong stage nay.

## 3. Hard-code vs metadata tables

Con nguoi van dinh nghia y nghia nghiep vu trong code, tap trung tai
`code/spark/gold/metadata_definitions.py`. `code/spark/gold/metadata.py` chi con
logic build/validate ngan gon, con output schema nam trong
`code/spark/gold/metadata_schema.py`.

Agent, backend hoac notebook co the query metadata tables thay vi doc code. Pipeline
con validate metadata voi schema that cua Gold tables de bat loi rename/xoa cot,
metric base table sai, hoac join key khong ton tai.

## 4. File da tao/chinh sua

- `code/spark/gold/config.py`: constants metadata namespace/base path/table names.
- `code/spark/gold/metadata_definitions.py`: dinh nghia business metadata cho table,
  column, metric va join.
- `code/spark/gold/metadata_schema.py`: schema cua cac metadata output tables.
- `code/spark/gold/metadata.py`: build DataFrame, tao/ghi Iceberg metadata tables,
  va validation.
- `code/spark/gold/tasks/gold_build_metadata.py`: Spark task build metadata.
- `code/spark/gold/tasks/gold_validate_metadata.py`: Spark task validate metadata.
- `code/airflow/dags/gold_metadata_pipeline.py`: DAG manual trigger rieng.
- `docs/GOLD_METADATA_PIPELINE.md`: tai lieu pipeline nay.
- `notebook/gold_view.ipynb`: them variables/cells xem metadata tables.

## 5. Output tables

Logical tables:

```text
iceberg_catalog.metadata.table_catalog
iceberg_catalog.metadata.column_catalog
iceberg_catalog.metadata.metric_catalog
iceberg_catalog.metadata.join_catalog
```

Physical locations:

```text
s3a://gold/metadata/table_catalog
s3a://gold/metadata/column_catalog
s3a://gold/metadata/metric_catalog
s3a://gold/metadata/join_catalog
```

Namespace duoc tao bang:

```sql
CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.metadata
```

Moi metadata table duoc tao `USING iceberg` voi explicit `LOCATION` ben tren.
Metadata tables khong partition.

Muon doi bucket/base path cho metadata, uu tien set
`GOLD_METADATA_BASE_PATH=s3a://<bucket>/metadata`. Neu muon doi ca Gold layer,
set `MINIO_BUCKET_GOLD` hoac `GOLD_STORAGE_ROOT`; chi tiet xem
`docs/CONVERT_BUCKET.md`.

## 6. Noi dung tung bang

`table_catalog` mo ta bang: layer, table type, business name, description, grain,
primary/unique key, visibility va recommendation cho Agent.

`column_catalog` mo ta cot theo schema that doc tu Spark/Iceberg, sau do merge
business overrides: business name, description, source, transformation logic,
flags metric/dimension/time/join/unique, allowed values va synonyms.

`metric_catalog` mo ta metric chuan: formula SQL ngan, base table mac dinh,
time column mac dinh, aggregation type, unit va example question tieng Viet.

`join_catalog` chi chua cac join an toan, ro, hay dung giua facts, dimensions va
summary tables.

## 7. Khi nao can trigger lai

Trigger lai `gold_metadata_pipeline` khi:

- them, xoa hoac doi ten bang Gold
- them, xoa hoac doi ten cot Gold
- doi cong thuc metric
- doi join relationship
- doi description hoac synonyms cho Agent
- doi bang recommended cho Agent

Khong can trigger lai chi vi Gold data rows duoc refresh.

## 8. Cach chay bang Airflow

DAG ID:

```text
gold_metadata_pipeline
```

Tasks:

```text
gold_build_metadata
gold_validate_metadata
```

Dependency:

```text
gold_build_metadata >> gold_validate_metadata
```

Command tham khao:

```bash
docker exec -it airflow airflow dags list
docker exec -it airflow airflow tasks list gold_metadata_pipeline
docker exec -it airflow airflow dags trigger gold_metadata_pipeline
```

Neu DAG dang pause tren Airflow UI, can unpause/active truoc khi manual trigger.

## 9. Cach chay tay bang spark-submit

Command tham khao. Dung mounted JAR tai `/opt/project/jars`, khong dung
`--packages`.

```bash
JARS="/opt/project/jars/org.apache.hadoop_hadoop-aws-3.4.2.jar,/opt/project/jars/org.apache.hadoop_hadoop-client-api-3.4.2.jar,/opt/project/jars/org.apache.hadoop_hadoop-client-runtime-3.4.2.jar,/opt/project/jars/software.amazon.awssdk_bundle-2.29.52.jar,/opt/project/jars/org.apache.spark_spark-sql-kafka-0-10_2.13-4.1.1.jar,/opt/project/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.1.1.jar,/opt/project/jars/org.apache.kafka_kafka-clients-3.9.1.jar,/opt/project/jars/org.apache.commons_commons-pool2-2.12.1.jar,/opt/project/jars/org.lz4_lz4-java-1.8.0.jar,/opt/project/jars/org.xerial.snappy_snappy-java-1.1.10.8.jar,/opt/project/jars/org.slf4j_slf4j-api-2.0.17.jar,/opt/project/jars/org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar,/opt/project/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar,/opt/project/jars/postgresql-42.7.4.jar"
CLASSPATH="${JARS//,/:}"
```

Build metadata:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars "$JARS" \
  --driver-class-path "$CLASSPATH" \
  --conf "spark.executor.extraClassPath=$CLASSPATH" \
  /opt/project/code/spark/gold/tasks/gold_build_metadata.py \
  --catalog-name iceberg_catalog \
  --metadata-namespace metadata \
  --gold-namespace gold \
  --staging-namespace gold_staging \
  --metadata-base-path s3a://gold/metadata \
  --refresh-mode full_refresh
```

Validate metadata:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars "$JARS" \
  --driver-class-path "$CLASSPATH" \
  --conf "spark.executor.extraClassPath=$CLASSPATH" \
  /opt/project/code/spark/gold/tasks/gold_validate_metadata.py \
  --catalog-name iceberg_catalog \
  --metadata-namespace metadata \
  --gold-namespace gold \
  --staging-namespace gold_staging
```

Runtime env can co `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`,
`ICEBERG_JDBC_URI`, `ICEBERG_JDBC_USER`, `ICEBERG_JDBC_PASSWORD`,
`ICEBERG_JDBC_SCHEMA`.

## 10. Cach xem metadata bang notebook

Notebook:

```text
/home/lyan/Project/BigData/Agent4DA/notebook/gold_view.ipynb
```

Da them/can co cac cell:

```python
spark.table("iceberg_catalog.metadata.table_catalog").show(100, truncate=False)
spark.table("iceberg_catalog.metadata.column_catalog").show(100, truncate=False)
spark.table("iceberg_catalog.metadata.metric_catalog").show(100, truncate=False)
spark.table("iceberg_catalog.metadata.join_catalog").show(100, truncate=False)
```

Vi du query:

```sql
SELECT table_name, description, grain
FROM iceberg_catalog.metadata.table_catalog
WHERE recommended_for_agent = true
```

```sql
SELECT column_name, business_name, description, agent_synonyms
FROM iceberg_catalog.metadata.column_catalog
WHERE table_name = 'gold.daily_product_summary'
```

```sql
SELECT metric_name, formula_sql, base_table, example_question
FROM iceberg_catalog.metadata.metric_catalog
```

## 11. JAR/dependency

Khong dung `--packages`. Khong tai JAR tu Maven/Ivy. Khong co logic download JAR.

JAR duoc mount san trong Airflow va Spark worker:

```text
./jars:/opt/project/jars:ro
```

Metadata DAG dung local mounted JAR tu `dag_common.BASE_JARS` va them:

```text
/opt/project/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
/opt/project/jars/postgresql-42.7.4.jar
```

## 12. Compile/check

Da chi chay syntax/compile checks, khong trigger DAG va khong chay Spark job that:

```bash
python -m py_compile code/spark/gold/tasks/gold_build_metadata.py
python -m py_compile code/spark/gold/tasks/gold_validate_metadata.py
python -m py_compile code/airflow/dags/gold_metadata_pipeline.py
python -m compileall code/spark/gold
```

Khong xoa `__pycache__`. Khong chay notebook.
