# Agent4DA

Agent4DA là nền tảng data engineering và AI analytics agent cho dữ liệu
e-commerce. Project mô phỏng một lakehouse local theo Medallion Architecture,
sau đó expose dữ liệu Gold qua Trino cho dashboard và AI Agent hỏi đáp bằng
ngôn ngữ tự nhiên.

Tài liệu này được cập nhật theo source code hiện tại ngày 2026-06-11. Khi có
xung đột giữa tài liệu cũ và code, ưu tiên code trong repo.

## Mục Lục

- [Tổng Quan](#tổng-quan)
- [Kiến Trúc](#kiến-trúc)
- [Các Thành Phần Chính](#các-thành-phần-chính)
- [Data Pipeline](#data-pipeline)
- [AI Agent Và Analytics Console](#ai-agent-và-analytics-console)
- [Observability](#observability)
- [Cách Chạy Local](#cách-chạy-local)
- [Ports](#ports)
- [Thư Mục Quan Trọng](#thư-mục-quan-trọng)
- [Ghi Chú Vận Hành](#ghi-chú-vận-hành)
- [Ảnh Nên Đưa Vào README](#ảnh-nên-đưa-vào-readme)

## Tổng Quan

Luồng chính của project:

```text
CSV sample data
  -> Kafka topic ecommerce_events
  -> Spark Bronze job
  -> MinIO bronze Parquet
  -> Spark Silver job
  -> MinIO silver Parquet
  -> Spark Gold jobs
  -> Iceberg Gold tables trên MinIO, metadata catalog trong PostgreSQL
  -> Trino
  -> FastAPI backend + LangGraph AI Agent
  -> Next.js Analytics Console
```

Điểm nổi bật hiện tại:

- Docker Compose stack đầy đủ cho Kafka, Spark, MinIO, PostgreSQL, Airflow,
  Trino, backend, frontend và monitoring.
- Bronze đọc Kafka theo batch, lưu offset trên MinIO để tránh đọc trùng.
- Silver chuẩn hóa schema, validate data quality, tách valid/invalid và
  deduplicate bằng `event_fingerprint`.
- Gold ghi Apache Iceberg tables qua JDBC catalog trên PostgreSQL, data nằm ở
  MinIO bucket `gold`.
- Metadata Gold cho Agent nằm ở `semantic_table_catalog` và
  `semantic_column_catalog`, có fallback về metadata tĩnh trong code.
- FastAPI backend có auth, chat sessions, query history, favorite runs, catalog,
  dashboard metrics, pipeline control, settings và Prometheus `/metrics`.
- LangGraph Agent có guardrail câu hỏi, metadata loading, answerability check,
  entity resolution, Text-to-SQL, SQL guard, Trino execution, result validation,
  chart planning và insight generation.
- Frontend Next.js có các màn hình Dashboard, Ask, History, Catalog, Pipelines
  và Settings.

## Kiến Trúc

```text
+------------------+       +--------------------+
| CSV producer     | ----> | Kafka KRaft        |
| code/kafka       |       | ecommerce_events   |
+------------------+       +---------+----------+
                                      |
                                      v
+------------------+       +--------------------+       +------------------+
| Airflow DAGs     | ----> | Spark standalone   | ----> | MinIO buckets    |
| scheduler/UI     |       | master + worker    |       | bronze/silver/   |
+------------------+       +---------+----------+       | gold             |
                                      |                  +--------+---------+
                                      v                           |
                            +--------------------+                |
                            | Iceberg Gold       | <--------------+
                            | tables             |
                            +---------+----------+
                                      |
                                      v
                            +--------------------+       +------------------+
                            | PostgreSQL         |       | Trino            |
                            | app DB + catalog   | <---- | SQL engine       |
                            +--------------------+       +--------+---------+
                                                                  |
                                                                  v
                                                        +------------------+
                                                        | FastAPI backend  |
                                                        | LangGraph Agent  |
                                                        +--------+---------+
                                                                 |
                                                                 v
                                                        +------------------+
                                                        | Next.js UI       |
                                                        | localhost:3000   |
                                                        +------------------+
```

Luồng được chia thành ba lớp:

- Data plane: Kafka, Spark, MinIO, Iceberg, PostgreSQL catalog, Trino.
- App plane: FastAPI backend, LangGraph Agent, Next.js frontend.
- Observability plane: Prometheus, Grafana, node-exporter, cAdvisor,
  postgres-exporter và backend metrics.

## Các Thành Phần Chính

### Ingestion Và Lakehouse

- Kafka KRaft nhận event JSON từ CSV producer.
- Spark Bronze đọc Kafka và ghi raw-ish Parquet vào `s3a://bronze`.
- Spark Silver đọc Bronze, chuẩn hóa dữ liệu và ghi clean Parquet vào
  `s3a://silver`.
- Spark Gold đọc Silver, ghi Iceberg tables vào `s3a://gold`.
- PostgreSQL lưu metadata catalog của Iceberg và dữ liệu ứng dụng backend.
- Trino query Gold Iceberg tables bằng catalog `iceberg`.

### Backend

Backend nằm ở `app/backend/api` và chạy trong service `agent-api`.

Nhóm API chính:

- Auth: `POST /auth/register`, `POST /auth/login`, `POST /auth/refresh`,
  `POST /auth/logout`, `GET /auth/me`.
- Agent: `GET /agent/stream`, `POST /agent/ask`, `POST /agent/stop`,
  `/agent/sessions`, `/agent/sample-questions`, feedback và CSV export.
- History: lọc run theo ngày, trạng thái, favorite và keyword.
- Catalog: browse/search semantic Gold tables và columns.
- Metrics: dashboard KPIs từ Gold summaries.
- Pipelines: đọc trạng thái Airflow DAGs, trigger DAG, xem tasks/logs.
- Settings: user preferences và trạng thái cấu hình hệ thống.
- Ops: health snapshot cho Trino, Spark, Airflow và LLM providers.
- Observability: `GET /metrics` cho Prometheus.

Auth dùng access token trong memory ở frontend và refresh token HttpOnly cookie.
Backend tự bootstrap admin nếu có `APP_BOOTSTRAP_ADMIN_EMAIL` và
`APP_BOOTSTRAP_ADMIN_PASSWORD` trong `envs/app.env`.

### Frontend

Frontend nằm ở `app/frontend` và chạy Next.js.

Màn hình chính:

- `/ask`: chat với AI Agent, session sidebar, pin/rename/delete session, stream
  tiến trình, kết quả dạng answer, SQL, chart và table.
- `/dashboard`: KPI, revenue trend, top brand/category/product từ Gold summary
  tables.
- `/history`: danh sách câu hỏi đã chạy, filter, favorite và re-run.
- `/catalog`: semantic catalog cho Gold tables/columns.
- `/pipelines`: trạng thái Bronze/Silver/Gold/Metadata DAGs, auto-refresh và
  trigger pipeline.
- `/settings`: theme, model/provider preference, chart default, language,
  export delimiter và system status.

Trang `/` redirect thẳng sang `/ask`.

## Data Pipeline

### Kafka Producer

Entry point:

```text
code/kafka/producer.py
```

Ví dụ gửi sample CSV vào Kafka:

```bash
python code/kafka/producer.py \
  --file data/event_test_1000.csv \
  --broker localhost:9092 \
  --topic ecommerce_events
```

Kafka internal bootstrap cho container là `kafka-kraft:29092`; từ host dùng
`localhost:9092`.

### Bronze

Entry points:

```text
code/spark/bronze_job.py
code/airflow/dags/bronze_pipeline.py
```

Bronze:

- Đọc Kafka batch với `startingOffsets` lấy từ MinIO.
- Parse JSON theo schema e-commerce, giữ type dạng string ở layer raw.
- Thêm Kafka metadata: `kafka_ts`, `kafka_partition`, `kafka_offset`.
- Thêm `ingested_at` và `date_partition`.
- Ghi append Parquet vào `s3a://bronze/ecommerce_events/`.
- Lưu offset kế tiếp vào `s3a://bronze/_offsets/ecommerce_events.json`.

Airflow schedule: `*/10 * * * *`, `max_active_runs=1`.

### Silver

Entry points:

```text
code/spark/silver_job.py
code/airflow/dags/silver_pipeline.py
```

Silver:

- Đọc Bronze Parquet từ `s3a://bronze/ecommerce_events/`.
- Cast timestamp, bigint ids, decimal price.
- Normalize brand/category/session.
- Tách `category_l1`, `category_l2`, `category_l3`.
- Tạo `source_event_id` từ Kafka partition/offset.
- Tạo `event_fingerprint` từ business content để deduplicate.
- Validate các field bắt buộc và event type hợp lệ:
  `view`, `cart`, `remove_from_cart`, `purchase`.
- Ghi valid Parquet vào `s3a://silver/ecommerce_events/`.
- Ghi invalid Parquet vào `s3a://silver/ecommerce_events_invalid/`.
- Ở mode append, đọc fingerprints đã có để skip duplicate valid events.

Airflow schedule: `*/10 * * * *`, `max_active_runs=1`.

### Gold

Entry points:

```text
code/airflow/dags/gold_pipeline.py
code/spark/gold/tasks/*.py
```

Gold chạy manual qua Airflow. DAG order:

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

Tables chính:

| Layer | Table |
| --- | --- |
| Staging | `iceberg.gold_staging.stg_events` |
| Facts | `iceberg.gold.fact_events`, `iceberg.gold.fact_sales` |
| Dimensions | `iceberg.gold.dim_time`, `iceberg.gold.dim_product`, `iceberg.gold.dim_user`, `iceberg.gold.dim_session` |
| Summaries | `iceberg.gold.daily_event_summary`, `iceberg.gold.daily_product_summary`, `iceberg.gold.daily_category_summary`, `iceberg.gold.daily_brand_summary` |

Gold hiện yêu cầu `full_refresh`; incremental/MERGE chưa được bật trong code.

### Gold Metadata Cho Agent

Entry points:

```text
code/airflow/dags/gold_metadata_pipeline.py
code/spark/gold/tasks/gold_build_metadata.py
code/spark/gold/tasks/gold_validate_metadata.py
code/spark/gold/metadata_definitions.py
```

Metadata DAG chạy manual sau khi Gold tables đã có. Tables metadata:

```text
iceberg.metadata.semantic_table_catalog
iceberg.metadata.semantic_column_catalog
```

Agent ưu tiên đọc metadata qua Trino. Nếu Trino hoặc metadata tables chưa sẵn
sàng, Agent fallback về metadata tĩnh trong
`code/spark/gold/metadata_definitions.py`.

## AI Agent Và Analytics Console

### LangGraph Flow

Luồng Agent hiện tại:

```text
guard_question
  -> load_metadata
  -> check_answerability
  -> resolve_entities
  -> build_prompt
  -> generate_sql
  -> guard_sql
  -> execute_sql
  -> profile_result
  -> validate_result
  -> plan_chart
  -> generate_insight
  -> build_final_response
```

Điểm đáng chú ý:

- Same-session memory được backend rebuild từ `app.query_runs` để hỗ trợ follow
  up questions.
- `check_answerability` chặn/giải thích các câu hỏi mà Gold không có dữ liệu,
  ví dụ hỏi `product_name` khi Gold chỉ có `product_id`.
- `guard_question` chặn destructive intent và prompt injection cơ bản.
- `guard_sql` chỉ cho phép một statement dạng `SELECT` hoặc `WITH ... SELECT`.
- SQL guard chặn keyword ghi/xóa/sửa như `INSERT`, `UPDATE`, `DELETE`, `DROP`,
  `ALTER`, `TRUNCATE`, `CREATE`, `MERGE`, `CALL`, `GRANT`, `REVOKE`.
- Nếu SQL không có `LIMIT`, backend thêm limit mặc định từ
  `AGENT_DEFAULT_SQL_LIMIT` hoặc `100`.
- `validate_result` có thể yêu cầu requery khi result rỗng do text filter chưa
  normalize hoặc thiếu field cần thiết.
- LLM provider hỗ trợ Gemini, Groq hoặc auto fallback. Gemini dùng endpoint
  OpenAI-compatible của Google Generative Language API.

### Data Người Dùng Trong PostgreSQL

Backend tự tạo schema `app` với các bảng:

- `app.users`
- `app.refresh_tokens`
- `app.user_preferences`
- `app.chat_sessions`
- `app.query_runs`
- `app.favorite_runs`
- `app.agent_feedback`

Dữ liệu này phục vụ auth, session memory, history, favorites, feedback và user
preferences.

## Observability

Monitoring chạy bằng `docker-compose.monitoring.yml` và là module tách biệt với
frontend.

UI:

- Prometheus: `http://localhost:19090`
- Grafana: `http://localhost:13000`

Prometheus scrape:

- Backend `/metrics`: HTTP latency/count, Agent `/ask`, retry/error metrics,
  ETL pipeline gauges và metrics từ agent services.
- Trino `/metrics`.
- PostgreSQL qua postgres-exporter.
- Host qua node-exporter.
- Container qua cAdvisor.

Grafana auto-provision dashboards trong folder Agent4DA:

- System Overview
- AI Agent Performance
- ETL Pipeline Monitoring
- Query / Data Layer

Nếu target backend trong Prometheus chưa `UP`, kiểm tra lại service/container
name đang chạy trong Docker network. Compose backend hiện expose internal port
`8000`; DNS thường dùng được qua service name `agent-api` hoặc container name
đang khai báo trong `docker-compose.agent.yml`.

## Cách Chạy Local

### 1. Chuẩn Bị

Yêu cầu:

- Docker và Docker Compose.
- `make`.
- Các file env trong `envs/`.
- Các Spark/Iceberg/Hadoop/Kafka/PostgreSQL jars trong `jars/`.
- Sample data trong `data/` nếu muốn chạy producer local.

Các thư mục `envs/`, `jars/`, `data/`, `log/` đang phục vụ local runtime và có
thể không được commit đầy đủ. Xem thêm [ENV_SETUP.md](ENV_SETUP.md).

### 2. Build Image Local

```bash
make all-build
```

Hoặc build riêng:

```bash
make airflow-build
make agent-build
make frontend-build
```

### 3. Start Stack

```bash
make all-up
```

Makefile tự tạo external network `data_network` nếu chưa có. `make all-up` dùng
`--no-build`, nên hãy build trước khi chạy lần đầu hoặc sau khi đổi Dockerfile
hoặc dependency.

Kiểm tra service:

```bash
make ps
```

Dừng toàn bộ stack:

```bash
make all-down
```

### 4. Nạp Dữ Liệu Và Chạy Pipeline

Gửi sample CSV vào Kafka:

```bash
python code/kafka/producer.py \
  --file data/event_test_1000.csv \
  --broker localhost:9092 \
  --topic ecommerce_events
```

Bronze/Silver có schedule 10 phút. Khi demo hoặc debug, có thể trigger thủ công:

```bash
docker exec airflow airflow dags trigger bronze_pipeline
docker exec airflow airflow dags trigger silver_pipeline
docker exec airflow airflow dags trigger gold_pipeline
docker exec airflow airflow dags trigger gold_metadata_pipeline
```

Gold metadata nên chạy sau Gold.

### 5. Mở UI

- Frontend: `http://localhost:3000`
- Backend Swagger: `http://localhost:8083/docs`
- Airflow: `http://localhost:8081`
- MinIO console: `http://localhost:9001`
- Trino: `http://localhost:8082`
- Spark master UI: `http://localhost:8080`
- Prometheus: `http://localhost:19090`
- Grafana: `http://localhost:13000`

Đăng nhập app bằng admin seed trong `envs/app.env`:

```text
APP_BOOTSTRAP_ADMIN_EMAIL
APP_BOOTSTRAP_ADMIN_PASSWORD
```

### 6. Query Trino Mẫu

```sql
SELECT *
FROM iceberg.gold.daily_event_summary
LIMIT 10;
```

```sql
SELECT table_name, display_name, grain
FROM iceberg.metadata.semantic_table_catalog
ORDER BY table_name;
```

## Ports

| Service | Host port | Ghi chú |
| --- | ---: | --- |
| Spark master UI | `8080` | Spark standalone web UI |
| Spark master RPC | `7077` | `spark://spark-master:7077` |
| Spark driver UI | `4040` | Khi job đang chạy |
| Airflow UI | `8081` | Airflow webserver |
| Airflow log server | `8793` | Task logs |
| Trino | `8082` | Host port map vào container `8080` |
| Backend API | `8083` | FastAPI, Swagger `/docs`, metrics `/metrics` |
| Frontend | `3000` | Next.js Analytics Console |
| Kafka | `9092` | External listener cho host |
| PostgreSQL | `5432` | Shared database |
| MinIO S3 API | `9000` | S3-compatible endpoint |
| MinIO Console | `9001` | Web console |
| Prometheus | `19090` | Monitoring module |
| Grafana | `13000` | Monitoring dashboards |

## Thư Mục Quan Trọng

| Path | Vai trò |
| --- | --- |
| `code/kafka/` | CSV producer và helper Kafka |
| `code/spark/bronze_job.py` | Bronze Spark job |
| `code/spark/silver_job.py` | Silver Spark job |
| `code/spark/gold/` | Gold Iceberg tasks, DDL, readers/writers, metadata definitions |
| `code/airflow/dags/` | Airflow DAGs cho Bronze/Silver/Gold/Metadata |
| `code/agent/` | LangGraph Agent engine |
| `app/backend/` | FastAPI backend cho UI và Agent |
| `app/frontend/` | Next.js Analytics Console |
| `monitoring/` | Prometheus/Grafana configs, dashboards, exporters docs |
| `trino/` | Trino config và runtime catalog entrypoint |
| `script/` | Helper scripts cho Spark/Kafka |
| `init/` | PostgreSQL schema bootstrap |
| `notebook/` | Notebook xem/debug data |
| `docs/` | Tài liệu chi tiết theo module |

## Ghi Chú Vận Hành

- `docker-compose.airflow.yml` đã có PostgreSQL shared instance, nên khi chạy
  `make all-up` không cần chạy riêng `docker-compose.postgre.yml`.
- Kafka external advertised listener hiện là `localhost:9092`; nếu producer
  chạy từ máy khác, cần đổi sang IP/DNS phù hợp hoặc dùng SSH tunnel.
- Spark compose hiện khai báo một worker `spark-worker` với container name cố
  định. Nếu muốn scale nhiều worker bằng `--scale`, cần bỏ `container_name` hoặc
  khai báo worker riêng.
- Trino Iceberg connector đang set `iceberg.security=read_only` và
  `iceberg.jdbc-catalog.schema-version=V0` để khớp Spark/Iceberg JDBC catalog.
- Gold refresh mode hiện là `full_refresh`.
- Backend cần LLM credential để Agent trả lời thật: `GEMINI_API_KEY` /
  `GEMINI_API_KEYS` hoặc `GROQ_API_KEY`.
- Khi đổi domain/host frontend, rebuild frontend với
  `NEXT_PUBLIC_API_BASE_URL` mới và cập nhật `APP_CORS_ORIGINS` cho backend.
- Không đưa production secrets vào README hoặc compose. Dùng `envs/*.env` cho
  local và secret manager phù hợp khi deploy.

## Ảnh Nên Đưa Vào README

Nên lưu ảnh vào `docs/assets/` và dùng đường dẫn tương đối, ví dụ:
`![Architecture](assets/architecture.png)`.

| Ảnh đề xuất | File gợi ý | Vì sao nên có |
| --- | --- | --- |
| Kiến trúc tổng thể | `assets/architecture.png` | Cho người đọc thấy ngay Kafka -> Spark -> MinIO/Iceberg -> Trino -> Agent/UI. |
| Airflow DAG graph | `assets/airflow-gold-dag.png` | Chứng minh orchestration Bronze/Silver/Gold/Metadata rõ ràng. |
| MinIO buckets | `assets/minio-buckets.png` | Minh họa lakehouse storage `bronze`, `silver`, `gold`. |
| Trino query Gold | `assets/trino-gold-query.png` | Cho thấy Gold Iceberg tables query được qua SQL. |
| Ask page | `assets/ui-ask.png` | Ảnh quan trọng nhất cho AI Agent: chat, stepper, SQL, chart, table. |
| Dashboard page | `assets/ui-dashboard.png` | Thể hiện giá trị BI/KPI ngoài chat. |
| Catalog page | `assets/ui-catalog.png` | Cho thấy semantic metadata phục vụ Text-to-SQL. |
| Pipelines page | `assets/ui-pipelines.png` | Thể hiện vận hành DAG từ UI app. |
| History page | `assets/ui-history.png` | Thể hiện persistence, favorite và audit trail của các câu hỏi. |
| Grafana AI Agent dashboard | `assets/grafana-agent-performance.png` | Thể hiện observability cho latency, error rate, retry và LLM/Trino performance. |

Thứ tự ảnh nên đặt trong README:

1. `architecture.png` ngay sau phần [Kiến Trúc](#kiến-trúc).
2. `ui-ask.png` và `ui-dashboard.png` ở phần [AI Agent Và Analytics Console](#ai-agent-và-analytics-console).
3. `airflow-gold-dag.png`, `minio-buckets.png`, `trino-gold-query.png` ở phần [Data Pipeline](#data-pipeline).
4. `grafana-agent-performance.png` ở phần [Observability](#observability).
