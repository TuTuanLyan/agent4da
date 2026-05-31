# Agent4DA

Agent4DA la project data engineering + AI analytics agent cho du lieu ecommerce.
Muc tieu cua project la demo mot pipeline du lieu phan tan theo Medallion
Architecture:

```text
CSV/sample data
  -> Kafka topic ecommerce_events
  -> Spark Bronze job
  -> MinIO bronze Parquet
  -> Spark Silver job
  -> MinIO silver Parquet
  -> Spark Gold jobs
  -> Iceberg Gold tables tren MinIO + PostgreSQL JDBC catalog
  -> Trino
  -> FastAPI/LangGraph Agent Text-to-SQL
```

README nay ghi lai tinh trang repo hien tai, doc tu source code va docs tai
ngay 2026-05-30. Muc dich la lam tai lieu tong quan de tiep tuc hoi/len ke
hoach deploy len Google Cloud Platform voi ngan sach Free Trial khoang $300.

## 1. Tinh trang hien tai

### Da co

- Docker Compose stack cho Kafka, Spark standalone, MinIO, PostgreSQL, Airflow
  va Trino.
- Spark standalone dang tach `spark-master` va `spark-worker` thanh cac
  container rieng.
- Bronze batch job doc Kafka theo offset da luu tren MinIO, parse JSON va ghi
  Parquet vao bucket `bronze`.
- Silver batch job doc Bronze Parquet, normalize schema, validate data quality,
  deduplicate theo `event_fingerprint`, tach valid/invalid va ghi Parquet vao
  bucket `silver`.
- Gold pipeline moi da duoc refactor thanh cac task doc lap:
  - `gold_prepare_events`
  - `gold_build_facts`
  - `gold_build_dimensions`
  - `gold_build_daily_event_summary`
  - `gold_build_daily_product_summary`
  - `gold_build_daily_category_summary`
  - `gold_build_daily_brand_summary`
- Gold tables dung Apache Iceberg tren MinIO. PostgreSQL duoc dung lam JDBC
  catalog metadata cho Iceberg, khong luu row data that cua Gold.
- Trino co connector Iceberg va PostgreSQL, duoc expose local port `8082`.
- Agent FastAPI da co API doc tai `/docs`, co LangGraph flow Text-to-SQL,
  doc metadata, guard question, guard SQL, query Trino, profile result, plan
  chart va sinh insight.
- Agent co fallback metadata tinh tu `code/spark/gold/metadata_definitions.py`
  neu chua doc duoc metadata tables tu Trino.

### Chua hoan thien hoac can kiem tra ky

- `docker-compose.spark.yml` hien chi co 1 service worker ten `spark-worker`.
  Vi co `container_name: spark-worker`, chua scale truc tiep len 2 worker bang
  `docker compose --scale spark-worker=2`. Muon demo 1 master + 2 worker can
  sua compose thanh `spark-worker-1` va `spark-worker-2`, hoac bo
  `container_name` de scale.
- Chua co `docker-compose.agent.yml` hoac Dockerfile rieng cho Agent FastAPI.
  Agent hien chay bang Python/conda env tren host:
  `code/agent/main_agent.py`.
- Metadata docs dang co diem lech voi code moi. File
  `docs/GOLD_METADATA_PIPELINE.md` con nhac cac bang
  `table_catalog`, `column_catalog`, `metric_catalog`, `join_catalog`, nhung
  code hien tai trong `code/spark/gold/config.py` va
  `code/spark/gold/metadata.py` build 2 bang:
  `semantic_table_catalog` va `semantic_column_catalog`. Agent cung dang query
  `iceberg.metadata.semantic_table_catalog` va
  `iceberg.metadata.semantic_column_catalog`.
- Gold metadata moi mo ta table/column cho Agent. Metric catalog va join catalog
  chua thay trong code runtime hien tai.
- Gold refresh mode hien tai la `full_refresh`; incremental/MERGE chua
  implement.
- `Makefile all-up` co the gay nham lan vi `docker-compose.airflow.yml` da gom
  PostgreSQL ben trong, trong khi repo cung co `docker-compose.postgre.yml`
  rieng. Khong nen start 2 PostgreSQL compose rieng cung luc neu cung
  `container_name: postgres-db`.
- `envs/`, `jars/`, `data/`, `log/` dang bi gitignore. Khi deploy sang may
  khac phai copy/generate lai cac file nay.
- Kafka compose dang advertise listener ngoai la `localhost:9092`; dung tot khi
  producer chay tren chinh VM/host, nhung neu producer tu may ngoai thi can doi
  sang external IP/DNS hoac dung SSH tunnel.

## 2. Kien truc service hien tai

```text
+------------------+       +--------------------+
| CSV producer     | ----> | Kafka KRaft        |
| code/kafka       |       | topic ecommerce    |
+------------------+       +---------+----------+
                                      |
                                      v
+------------------+       +--------------------+       +------------------+
| Airflow DAGs     | ----> | Spark standalone   | ----> | MinIO S3 buckets|
| scheduler/UI     |       | master + worker(s) |       | bronze/silver/  |
+------------------+       +---------+----------+       | gold            |
                                      |                  +--------+---------+
                                      v                           |
                            +--------------------+                |
                            | Iceberg tables     | <--------------+
                            | Gold layer         |
                            +---------+----------+
                                      |
                                      v
                            +--------------------+       +------------------+
                            | PostgreSQL         |       | Trino            |
                            | Iceberg catalog    | <---- | SQL engine       |
                            +--------------------+       +--------+---------+
                                                                  |
                                                                  v
                                                        +------------------+
                                                        | FastAPI Agent    |
                                                        | LangGraph SQL    |
                                                        +------------------+
```

## 3. Thu muc chinh

- `code/kafka/`: CSV producer va helper/tai lieu Kafka.
- `code/spark/bronze_job.py`: Spark Bronze batch job.
- `code/spark/silver_job.py`: Spark Silver batch job.
- `code/spark/gold/`: Gold layer modules, DDL, validators, readers/writers,
  metadata definitions va Spark task entrypoints.
- `code/airflow/dags/`: DAGs cho Bronze, Silver, Gold va Gold metadata.
- `code/agent/`: FastAPI + LangGraph Agent Text-to-SQL.
- `trino/`: Trino config va entrypoint generate catalog properties tu env.
- `dockerfile/`: Dockerfile Airflow va entrypoint.
- `script/`: helper script submit Spark job va thao tac Kafka.
- `notebook/`: notebook xem/debug data.
- `docs/`: tai lieu chi tiet tung phan. Mot so file co the lech nhe voi code
  moi, nen uu tien source code khi co conflict.
- `envs/`: local env files, bi gitignore.
- `jars/`: local Spark/Iceberg/Hadoop/Kafka/PostgreSQL jars, bi gitignore.
- `data/`: sample CSV, bi gitignore.

## 4. Docker services va ports

| Service | Compose file | Port host | Ghi chu |
| --- | --- | --- | --- |
| Kafka KRaft | `docker-compose.kafka.yml` | `9092` | 1 broker/controller, topic auto-create, 3 partitions mac dinh |
| Spark master | `docker-compose.spark.yml` | `8080`, `7077`, `4040` | Spark UI, master URL `spark://spark-master:7077` |
| Spark worker | `docker-compose.spark.yml` | none | Hien chi 1 worker, 2 cores, 2 GB memory |
| MinIO | `docker-compose.minio.yml` | `9000`, `9001` | S3 API va console UI |
| PostgreSQL | `docker-compose.airflow.yml` hoac `docker-compose.postgre.yml` | `5432` | Airflow metadata + Iceberg JDBC catalog schemas |
| Airflow | `docker-compose.airflow.yml` | `8081`, `8793` | LocalExecutor, SparkSubmitOperator |
| Trino | `docker-compose.trino.yml` | `8082` | Query Iceberg/Postgres |
| Agent API | chua co compose | `8001` | Chay bang Python host/conda env |

Tat ca compose file dang dung external Docker network:

```bash
docker network create data_network
```

## 5. Data pipeline

### 5.1 Kafka ingest

- Producer: `code/kafka/producer.py`.
- Input sample hien co trong repo local: `data/event_test.csv` va
  `data/event_test_1000.csv`.
- Topic mac dinh: `ecommerce_events`.
- Kafka container: `kafka-kraft`.
- Kafka internal bootstrap cho container: `kafka-kraft:29092`.
- Kafka external bootstrap tren host: `localhost:9092`.

Producer doc CSV, convert tung row thanh JSON va gui vao Kafka.

### 5.2 Bronze

Entry point:

```text
code/spark/bronze_job.py
code/airflow/dags/bronze_pipeline.py
```

Logic chinh:

- Doc Kafka batch voi `startingOffsets` lay tu file offset tren MinIO.
- Parse JSON theo schema ecommerce dang string.
- Them Kafka metadata: `kafka_ts`, `kafka_partition`, `kafka_offset`.
- Them `ingested_at` va `date_partition`.
- Ghi Parquet append vao:

```text
s3a://bronze/ecommerce_events/
```

- Luu offset tiep theo vao:

```text
s3a://bronze/_offsets/ecommerce_events.json
```

Airflow schedule:

```text
*/10 * * * *
```

`max_active_runs=1` de tranh race condition tren offset file.

### 5.3 Silver

Entry point:

```text
code/spark/silver_job.py
code/airflow/dags/silver_pipeline.py
```

Logic chinh:

- Doc Bronze Parquet tu:

```text
s3a://bronze/ecommerce_events/
```

- Parse/cast type:
  - timestamp/date/hour
  - bigint ids
  - decimal price
  - brand/category/session normalized
- Tao category levels `category_l1`, `category_l2`, `category_l3`.
- Tao `source_event_id` tu Kafka partition/offset.
- Tao `event_fingerprint` tu business event content.
- Validate records voi cac rule:
  - event timestamp bat buoc
  - event type thuoc `view`, `cart`, `remove_from_cart`, `purchase`
  - product/category/user/session/price hop le
- Tach:
  - valid output: `s3a://silver/ecommerce_events/`
  - invalid output: `s3a://silver/ecommerce_events_invalid/`
- Deduplicate valid events theo `event_fingerprint`.
- Khi write mode `append`, job doc existing fingerprints de skip duplicate.

Airflow schedule:

```text
*/10 * * * *
```

### 5.4 Gold

Entry point:

```text
code/airflow/dags/gold_pipeline.py
code/spark/gold/tasks/*.py
```

Gold dung Iceberg tables, catalog name mac dinh:

```text
iceberg_catalog
```

PostgreSQL chi luu Iceberg catalog metadata trong schema `iceberg`. Row data nam
tren MinIO bucket `gold`.

Gold DAG hien la manual trigger:

```text
gold_prepare_events
  -> gold_build_facts
  -> gold_build_dimensions
  -> [
       gold_build_daily_event_summary,
       gold_build_daily_product_summary,
       gold_build_daily_category_summary,
       gold_build_daily_brand_summary
     ]
```

Gold staging:

```text
iceberg_catalog.gold_staging.stg_events
s3a://gold/gold_staging/stg_events
```

Gold facts:

```text
iceberg_catalog.gold.fact_events
s3a://gold/gold/fact_events

iceberg_catalog.gold.fact_sales
s3a://gold/gold/fact_sales
```

Gold dimensions:

```text
iceberg_catalog.gold.dim_time
iceberg_catalog.gold.dim_product
iceberg_catalog.gold.dim_user
iceberg_catalog.gold.dim_session
```

Gold summaries phuc vu dashboard/Agent:

```text
iceberg_catalog.gold.daily_event_summary
iceberg_catalog.gold.daily_product_summary
iceberg_catalog.gold.daily_category_summary
iceberg_catalog.gold.daily_brand_summary
```

### 5.5 Gold metadata cho Agent

Entry point:

```text
code/airflow/dags/gold_metadata_pipeline.py
code/spark/gold/tasks/gold_build_metadata.py
code/spark/gold/tasks/gold_validate_metadata.py
```

Metadata DAG hien la manual trigger va nen chay sau khi Gold tables da co.

Code hien tai build:

```text
iceberg_catalog.metadata.semantic_table_catalog
iceberg_catalog.metadata.semantic_column_catalog
```

Metadata duoc khai bao bang tay trong:

```text
code/spark/gold/metadata_definitions.py
```

Agent dung metadata nay de biet bang/cot nao visible, grain, purpose,
business terms va query notes.

## 6. Trino

Trino compose:

```text
docker-compose.trino.yml
```

Trino image:

```text
trinodb/trino:481
```

Trino entrypoint generate runtime catalog configs:

```text
/etc/trino/catalog/iceberg.properties
/etc/trino/catalog/postgres.properties
```

Trino expose host port:

```text
http://localhost:8082
```

Query mau:

```sql
SELECT *
FROM iceberg.gold.daily_event_summary
LIMIT 10;

SELECT table_name, display_name, grain
FROM iceberg.metadata.semantic_table_catalog
ORDER BY table_name;
```

Luu y: Trino config dang set `iceberg.jdbc-catalog.schema-version=V0` de khop
voi Spark/Iceberg JDBC catalog hien tai.

## 7. Agent API

Entry point:

```text
code/agent/main_agent.py
```

Chay local:

```bash
cd code/agent
AGENT_API_PORT=8001 /opt/miniconda/envs/agent4daenv/bin/python -m uvicorn main_agent:app --host 0.0.0.0 --port 8001
```

Swagger UI:

```text
http://localhost:8001/docs
```

Endpoints chinh:

- `GET /health`
- `GET /api/v1/health`
- `GET /api/v1/metadata`
- `GET /api/v1/schema-context`
- `POST /api/v1/guard/question`
- `POST /api/v1/guard/sql`
- `POST /ask`
- `POST /api/v1/ask`

LangGraph flow:

```text
guard_question
  -> load_metadata
  -> build_prompt
  -> generate_sql
  -> guard_sql
  -> execute_sql
  -> profile_result
  -> plan_chart
  -> generate_insight
  -> build_final_response
```

Guardrail hien tai:

- Chan cau hoi co y dinh destructive/prompt injection don gian.
- SQL chi duoc la 1 statement `SELECT` hoac `WITH ... SELECT`.
- Chan cac keyword nhu `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`,
  `TRUNCATE`, `CREATE`, `MERGE`, `CALL`, `GRANT`, `REVOKE`, `EXECUTE`.

Env can co:

```text
GROQ_API_KEY
TRINO_HOST
TRINO_PORT
TRINO_USER
AGENT_MODEL
```

Mac dinh Agent doc env tu:

```text
envs/endpoint.env
envs/groq.env
envs/postgre.env
envs/iceberg.env
```

## 8. Cach chay local tham khao

Tao network:

```bash
docker network create data_network
```

Start cac service nen theo thu tu:

```bash
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.minio.yml up -d
docker compose -f docker-compose.spark.yml up -d
docker compose -f docker-compose.airflow.yml up -d
docker compose -f docker-compose.trino.yml up -d
```

Khong can chay `docker-compose.postgre.yml` rieng neu da chay
`docker-compose.airflow.yml`, vi Airflow compose da co PostgreSQL.

Submit Bronze/Silver thu cong:

```bash
bash script/spark/submit_bronze.sh
bash script/spark/submit_silver.sh
```

Trigger DAGs:

```bash
docker exec -it airflow airflow dags list
docker exec -it airflow airflow dags trigger gold_pipeline
docker exec -it airflow airflow dags trigger gold_metadata_pipeline
```

Mo UI local:

```text
Spark UI:   http://localhost:8080
Airflow:    http://localhost:8081
MinIO:      http://localhost:9001
Trino:      http://localhost:8082
Agent API:  http://localhost:8001/docs
```

## 9. Ke hoach deploy len GCP

### Khuyen nghi ngan han: 1 VM Compute Engine chay Docker Compose

Voi tinh trang project hien tai, cach hop ly nhat de demo la deploy len 1 VM
Compute Engine va chay Docker Compose. Cach nay khong phai phan tan theo nhieu
may vat ly, nhung van the hien duoc kien truc phan tan o muc service/container:

- Kafka la service rieng.
- Spark master la service rieng.
- Spark worker 1 va worker 2 la 2 executor node rieng.
- MinIO la object storage S3-compatible rieng.
- PostgreSQL la catalog DB rieng.
- Airflow la orchestrator rieng.
- Trino la query engine rieng.
- Agent API la service rieng.

Day la phu hop nhat de demo trong ngan sach Free Trial $300 vi:

- Khong phai doi code sang Dataproc/GKE ngay.
- Giu nguyen MinIO/Iceberg/PostgreSQL/Trino nhu local.
- De debug bang Docker logs va UI.
- Co the stop VM khi khong demo de tiet kiem credit.

Google Cloud Free Trial hien cho new customers $300 credit de thu va build proof
of concept. Compute Engine Always Free co e2-micro, nhung e2-micro qua yeu cho
stack nay. GCP docs mo ta e2-micro la shared-core voi 2 vCPU nhung chi sustain
khoang 25% CPU time va memory nho, khong phu hop chay Kafka + Spark + Airflow +
Trino + MinIO cung luc.

May VM nen can nhac cho demo:

- Toi thieu de thu nhe: `e2-standard-2` voi 2 vCPU, 8 GB RAM. Can giam memory
  worker, co the phai tat bot Airflow/Trino khi khong dung.
- De demo on hon: `e2-standard-4` voi 4 vCPU, 16 GB RAM. Phu hop hon voi 1
  Spark master + 2 worker moi worker 1-2 GB, cung Kafka/MinIO/Postgres/Trino.
- Disk: 50-100 GB persistent disk tuy data/jars/logs. Stack co jars hon 700 MB
  va MinIO data se tang theo pipeline.

### Can sua truoc khi deploy demo

1. Sua Spark compose de co 2 worker.

   Cach ro rang nhat la tao 2 service:

   ```text
   spark-worker-1
   spark-worker-2
   ```

   Ca hai cung command:

   ```text
   /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
   ```

   Moi worker nen set memory phu hop VM, vi du:

   ```text
   SPARK_WORKER_CORES=1
   SPARK_WORKER_MEMORY=1g
   ```

   Tren VM 16 GB co the dung:

   ```text
   SPARK_WORKER_CORES=2
   SPARK_WORKER_MEMORY=2g
   ```

2. Tao service/Dockerfile cho Agent API.

   Hien tai Agent chay bang conda env tren host. De deploy gon, nen tao
   `docker-compose.agent.yml` hoac them service `agent-api` dung Python image,
   install dependency va expose port `8001`.

3. Dieu chinh env cho cloud.

   - `TRINO_HOST=trino` neu Agent chay trong Docker network.
   - `TRINO_PORT=8080` neu Agent goi Trino noi bo container.
   - `MINIO_ENDPOINT=http://minio:9000` cho Spark/Airflow/Trino noi bo.
   - `AIRFLOW__WEBSERVER__BASE_URL` co the doi sang external IP/domain neu mo
     Airflow truc tiep.
   - Khong commit `GROQ_API_KEY` hay password len git.

4. Quyet dinh cach expose UI/API.

   Khuyen nghi an toan cho demo:

   - Public chi mo `8001` cho Agent API neu can nguoi khac truy cap.
   - Cac UI quan tri nhu Airflow `8081`, Spark `8080`, MinIO `9001`, Trino
     `8082` nen dung SSH tunnel hoac firewall source IP rieng.

5. Backup/restore data.

   Hien MinIO dung Docker named volume. Tren GCP VM neu xoa VM/disk thi mat
   data. Nen snapshot disk hoac backup MinIO volume neu can giu ket qua.

### Cac buoc deploy VM muc cao

1. Tao GCP project, enable billing, dat budget alert.
2. Tao Compute Engine VM Ubuntu o region gan ban.
3. Chon machine type `e2-standard-4` neu muon demo on, hoac `e2-standard-2` neu
   muon tiet kiem hon.
4. Gan persistent disk 50-100 GB.
5. Cai Docker Engine va Docker Compose plugin.
6. Clone repo len VM.
7. Tao/copy cac folder bi gitignore:

   ```text
   envs/
   jars/
   data/
   ```

8. Tao Docker network:

   ```bash
   docker network create data_network
   ```

9. Start stack theo thu tu:

   ```bash
   docker compose -f docker-compose.kafka.yml up -d
   docker compose -f docker-compose.minio.yml up -d
   docker compose -f docker-compose.spark.yml up -d
   docker compose -f docker-compose.airflow.yml up -d
   docker compose -f docker-compose.trino.yml up -d
   ```

10. Start Agent API bang Docker service moi hoac bang Python/conda tren VM.
11. Gui data vao Kafka bang producer.
12. Chay Bronze/Silver hoac bat DAG Airflow.
13. Trigger `gold_pipeline`.
14. Trigger `gold_metadata_pipeline`.
15. Test Trino query Gold va metadata.
16. Test Agent endpoint `/api/v1/ask`.

### Firewall/port goi y

Mo toi thieu:

```text
tcp:22    SSH
tcp:8001  Agent API neu can public
```

Chi mo khi can demo/debug, va nen restrict source IP:

```text
tcp:8081  Airflow UI
tcp:8080  Spark UI
tcp:9001  MinIO console
tcp:8082  Trino
```

Khong nen public:

```text
tcp:5432  PostgreSQL
tcp:9000  MinIO S3 API
tcp:7077  Spark master
tcp:9092  Kafka
```

Dung SSH tunnel neu chi ban truy cap:

```bash
ssh -L 8001:localhost:8001 -L 8081:localhost:8081 -L 8080:localhost:8080 -L 9001:localhost:9001 -L 8082:localhost:8082 <user>@<vm-external-ip>
```

## 10. Phuong an deploy that su phan tan hon

Sau khi demo Compose on, co the nang cap theo 2 huong:

### Huong A: nhieu VM

- VM 1: Kafka + Airflow + Agent.
- VM 2: Spark master + Trino.
- VM 3-4: Spark workers.
- VM 5 hoac managed DB: PostgreSQL.
- Object storage: chuyen MinIO sang Cloud Storage hoac giu MinIO rieng.

Huong nay the hien phan tan that hon, nhung tang chi phi va can network/security
nhieu hon.

### Huong B: dich vu managed GCP

- Kafka: Confluent Cloud hoac Pub/Sub.
- Spark: Dataproc.
- Object storage: Cloud Storage.
- Catalog/DB: Cloud SQL PostgreSQL.
- Orchestration: Cloud Composer hoac Airflow tren VM.
- SQL engine: BigQuery/Dataproc Trino tu quan ly.
- Agent: Cloud Run hoac Compute Engine.

Huong nay cloud-native hon nhung can refactor connector/path/env kha nhieu. Voi
tinh trang project hien tai, chua nen lam ngay neu muc tieu la demo nhanh trong
$300.

## 11. Checklist demo thanh cong

- Spark UI hien 1 master va 2 workers alive.
- Kafka co topic `ecommerce_events` va co message.
- Bronze bucket co Parquet files va offset JSON.
- Silver bucket co valid/invalid Parquet outputs.
- Airflow DAG Bronze/Silver/Gold co task success.
- PostgreSQL schema `iceberg` co Iceberg catalog metadata.
- MinIO bucket `gold` co data files cua Gold Iceberg tables.
- Trino query duoc:

  ```sql
  SELECT * FROM iceberg.gold.daily_event_summary LIMIT 10;
  SELECT * FROM iceberg.metadata.semantic_table_catalog LIMIT 10;
  ```

- Agent `/api/v1/metadata` tra ve source `trino` neu metadata da san sang, hoac
  `static_definitions` neu fallback.
- Agent `/api/v1/ask` sinh SQL read-only, query Trino va tra ve rows/insight.

## 12. Tai lieu lien quan

- `docs/ENV_SETUP.md`
- `docs/TRINO_ENV.md`
- `docs/AGENT_FASTAPI.md`
- `docs/GOLD_REFACTOR_TEST_ENV.md`
- `docs/GOLD_STAGING_TASK.md`
- `docs/GOLD_FACTS_TASK.md`
- `docs/GOLD_SUMMARIES_TASK.md`
- `docs/GOLD_METADATA_PIPELINE.md`

Khi co conflict giua docs va code, uu tien code hien tai.
