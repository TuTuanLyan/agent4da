# Agent4DA - Data Engineering & AI Analytics Agent

## 1. Tổng quan dự án

Agent4DA là một project data platform cho dữ liệu e-commerce, kết hợp pipeline
xử lý dữ liệu với một AI analytics agent ở lớp ứng dụng. Thay vì chỉ dừng ở
việc ETL dữ liệu, project đi trọn một vòng: đưa event vào Kafka, xử lý qua các
tầng Bronze/Silver/Gold, query Gold layer bằng Trino, rồi dùng giao diện web để
xem dashboard hoặc đặt câu hỏi bằng ngôn ngữ tự nhiên.

Điểm chính của project là mô phỏng một lakehouse local theo Medallion
Architecture. Dữ liệu thô được giữ ở Bronze, dữ liệu đã chuẩn hóa đi vào
Silver, còn Gold là nơi chứa các bảng phân tích được quản lý bằng Apache
Iceberg. Từ Gold layer, hệ thống phục vụ hai hướng sử dụng: dashboard BI truyền
thống và AI Agent có khả năng sinh SQL, chạy truy vấn, vẽ biểu đồ và diễn giải
kết quả.

Nói ngắn gọn, Agent4DA bao gồm đủ các mảnh quan trọng của một hệ thống data
platform: ingestion, storage, processing, orchestration, query engine, semantic
metadata, analytics application và observability.

## 2. Kiến trúc hệ thống

Luồng dữ liệu tổng quát:

```text
CSV sample data
   -> Kafka topic ecommerce_events
   -> Spark Bronze job
   -> MinIO bronze Parquet
   -> Spark Silver job
   -> MinIO silver Parquet
   -> Spark Gold jobs
   -> Apache Iceberg Gold tables on MinIO
   -> PostgreSQL JDBC catalog metadata
   -> Trino SQL engine
   -> FastAPI + LangGraph AI Agent
   -> Next.js Analytics Console
```

Sơ đồ logic của hệ thống:

```text
+------------------+       +--------------------+
| CSV Producer     | ----> | Kafka KRaft        |
| code/kafka       |       | ecommerce_events   |
+------------------+       +---------+----------+
                                      |
                                      v
+------------------+       +--------------------+       +------------------+
| Airflow DAGs     | ----> | Spark Standalone   | ----> | MinIO Buckets    |
| Orchestration    |       | master + worker    |       | bronze/silver/   |
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
                            | App DB + Catalog   | <---- | SQL Engine       |
                            +--------------------+       +--------+---------+
                                                                  |
                                                                  v
                                                        +------------------+
                                                        | FastAPI Backend  |
                                                        | LangGraph Agent  |
                                                        +--------+---------+
                                                                 |
                                                                 v
                                                        +------------------+
                                                        | Next.js UI       |
                                                        | Analytics App    |
                                                        +------------------+
```

![Sơ đồ kiến trúc tổng thể Agent4DA](./imgs/SystemFlow.png)

Hệ thống được tách thành ba nhóm chính:

**Data Platform**

- Kafka tiếp nhận dữ liệu sự kiện.
- Spark xử lý dữ liệu ở các tầng Bronze, Silver, Gold.
- MinIO đóng vai trò object storage tương thích S3.
- Apache Iceberg quản lý Gold tables.
- PostgreSQL lưu Iceberg catalog metadata và dữ liệu ứng dụng.
- Trino phục vụ truy vấn SQL tốc độ cao trên Gold layer.

**Application & AI Agent**

- FastAPI backend cung cấp API cho frontend, auth, history, catalog, metrics và
  tích hợp Agent.
- LangGraph Agent chuyển câu hỏi tự nhiên thành SQL an toàn, truy vấn Trino và
  sinh câu trả lời.
- Next.js frontend cung cấp giao diện Dashboard, Ask, History, Catalog,
  Pipelines và Settings.

**Monitoring & Observability**

- Prometheus scrape metrics từ backend, Trino, PostgreSQL và exporters.
- Grafana hiển thị dashboard hệ thống, pipeline và hiệu năng AI Agent.

## 3. Các công nghệ sử dụng chính

### Data Engineering & Lakehouse

* **Apache Kafka** nhận dòng sự kiện e-commerce từ CSV producer. Project dùng
  Kafka KRaft nên không cần Zookeeper.
* **Apache Spark** xử lý dữ liệu qua các job Bronze, Silver và Gold.
* **Apache Airflow** điều phối pipeline bằng DAGs. Bronze/Silver chạy theo
  lịch, còn Gold/Gold Metadata được trigger khi cần refresh lớp phân tích.
* **MinIO** đóng vai trò data lake local tương thích S3, với các bucket
  `bronze`, `silver`, `gold`.
* **Apache Iceberg** quản lý các bảng Gold để dữ liệu có schema, metadata và có
  thể query ổn định qua Trino.
* **PostgreSQL** lưu Iceberg JDBC catalog, metadata của Airflow và dữ liệu ứng
  dụng như users, sessions, query history.
* **Trino** là SQL engine dùng để truy vấn trực tiếp Gold layer.

### Backend, Frontend & AI

* **FastAPI** là backend chính cho Analytics Console: authentication, catalog,
  history, settings, pipeline control và Agent execution.
* **LangGraph** định nghĩa luồng AI Agent từ kiểm tra câu hỏi, đọc metadata,
  sinh SQL, validate SQL, chạy Trino, kiểm tra kết quả đến tạo insight.
* **Gemini / Groq** là LLM providers cho Text-to-SQL và phần diễn giải kết quả.
  Người dùng có thể chọn provider/model trong Settings.
* **Next.js + React + TailwindCSS** xây dựng giao diện web.
* **Recharts** hiển thị biểu đồ trong dashboard và kết quả trả về từ Agent.

### Monitoring

* **Prometheus** thu thập metrics của backend, Agent, ETL pipeline, Trino,
  PostgreSQL, host và container.
* **Grafana** trực quan hóa các metrics đó bằng dashboard đã provision sẵn.
* **node-exporter, cAdvisor, postgres-exporter** cung cấp metrics cho host,
  container và PostgreSQL.

## 4. Chức năng chính của hệ thống

### 4.1. Data Pipeline

Pipeline dữ liệu được tổ chức theo ba tầng để tách rõ dữ liệu thô, dữ liệu đã
làm sạch và dữ liệu phục vụ phân tích.

**Bronze**

Bronze là điểm vào của dữ liệu. Spark đọc message mới từ Kafka topic
`ecommerce_events`, parse JSON theo schema e-commerce, thêm metadata của Kafka
như partition, offset, timestamp rồi ghi Parquet vào:

```text
s3a://bronze/ecommerce_events/
```

Offset được lưu lại trên MinIO để lần chạy tiếp theo chỉ đọc phần dữ liệu mới.

**Silver**

Silver là nơi dữ liệu được chuẩn hóa. Job Silver cast lại timestamp, số,
decimal, chuẩn hóa brand/category/session, tách category hierarchy thành
`category_l1`, `category_l2`, `category_l3`, sau đó validate và tách dữ liệu
hợp lệ/không hợp lệ. Các event hợp lệ được deduplicate bằng
`event_fingerprint` trước khi ghi vào:

```text
s3a://silver/ecommerce_events/
```

**Gold**

Gold là lớp phục vụ phân tích. Từ Silver clean events, hệ thống build staging,
fact tables, dimension tables và summary tables bằng Apache Iceberg. Các bảng
Gold chính gồm:

- `iceberg.gold.fact_events`
- `iceberg.gold.fact_sales`
- `iceberg.gold.dim_time`
- `iceberg.gold.dim_product`
- `iceberg.gold.dim_user`
- `iceberg.gold.dim_session`
- `iceberg.gold.daily_event_summary`
- `iceberg.gold.daily_product_summary`
- `iceberg.gold.daily_category_summary`
- `iceberg.gold.daily_brand_summary`

![Airflow DAGs cho Bronze, Silver, Gold và Gold Metadata](./imgs/airflow-dags.png)

![MinIO buckets Bronze, Silver và Gold](./imgs/minio-buckets.png)

![Query Gold table thành công qua Trino](./imgs/trino-gold-query.png)

### 4.2. Semantic Metadata cho AI Agent

Để Agent sinh SQL tốt hơn, project có thêm một lớp semantic metadata cho Gold
tables. Metadata này được build thành hai bảng:

```text
iceberg.metadata.semantic_table_catalog
iceberg.metadata.semantic_column_catalog
```

Hai bảng này mô tả ý nghĩa business của từng bảng và từng cột: bảng nên dùng
cho loại câu hỏi nào, grain của bảng là gì, cột nào đại diện cho doanh thu,
lượt xem, conversion rate, brand, product hoặc category.

Khi nhận câu hỏi tự nhiên, Agent đọc metadata này trước để chọn bảng/cột phù
hợp, rồi mới sinh SQL. Bộ metadata chuẩn được khai báo trong:

```text
code/spark/gold/metadata_definitions.py
```

### 4.3. AI Analytics Agent

Với lớp Agent, người dùng không cần viết SQL trực tiếp. Họ có thể hỏi các câu
gần với ngôn ngữ phân tích kinh doanh, ví dụ:

```text
Top 10 brand theo doanh thu trong tháng gần nhất
Doanh thu theo ngày trong tháng gần nhất
Danh mục nào có tỷ lệ chuyển đổi cao nhất?
Sản phẩm nào được xem nhiều nhất?
```

Luồng xử lý bên trong Agent:

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

Để tránh sinh truy vấn nguy hiểm hoặc sai ngữ cảnh, Agent có một số guardrail:

- Chặn câu hỏi có ý định xóa, sửa, ghi dữ liệu hoặc prompt injection cơ bản.
- Chỉ cho phép SQL dạng `SELECT` hoặc `WITH ... SELECT`.
- Chặn các keyword như `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`,
  `TRUNCATE`, `CREATE`, `MERGE`, `CALL`, `GRANT`, `REVOKE`.
- Tự thêm `LIMIT` mặc định nếu SQL không có giới hạn.
- Có bước validate result và requery khi kết quả rỗng hoặc thiếu field quan
  trọng.

### 4.4. Analytics Console

Frontend là nơi gom toàn bộ trải nghiệm sử dụng của project. Các màn hình chính
bao gồm:

- **Ask**: Chat với AI Agent, xem SQL, bảng dữ liệu, biểu đồ và insight.
- **Dashboard**: KPI tổng quan, doanh thu theo ngày, ranking brand/category/
  product.
- **History**: Lưu lịch sử câu hỏi, trạng thái, thời gian chạy, favorite và
  re-run.
- **Catalog**: Xem semantic metadata của Gold tables và columns.
- **Pipelines**: Theo dõi trạng thái DAG Bronze, Silver, Gold, Metadata và
  trigger pipeline.
- **Settings**: Chọn theme, provider/model, chart mặc định, ngôn ngữ và xem
  trạng thái cấu hình hệ thống.

![Dashboard KPI và ranking](./imgs/ui-dashboard.png)

![Ask page với câu hỏi, SQL, chart và table](./imgs/ui-ask.png)

![Catalog semantic metadata](./imgs/ui-catalog.png)

![Pipelines page](./imgs/ui-pipelines.png)

## 5. Yêu cầu hệ thống

Yêu cầu khuyến nghị để chạy local:

* **Docker** và **Docker Compose**.
* **Make** để dùng các lệnh tiện ích trong `Makefile`.
* **Python 3.10+** cho Kafka producer chạy từ host.
* **RAM 16GB+** được khuyến nghị vì stack gồm Kafka, Spark, Airflow, Trino,
  PostgreSQL, MinIO, backend, frontend và monitoring.
* **CPU 4 cores+** cho stack cơ bản; 8 cores+ giúp chạy mượt hơn.
* **Dung lượng đĩa trống** cho Docker volumes, MinIO data và Spark logs.
* **Kết nối Internet** để pull Docker images và tải dependencies.

Các file runtime cần chuẩn bị:

- `envs/*.env`: biến môi trường và secret local.
- `jars/`: các JAR dependencies cho Spark, Kafka, Hadoop S3A, Iceberg,
  PostgreSQL.
- `data/`: sample CSV để nạp vào Kafka.

Chi tiết biến môi trường xem thêm: [`docs/ENV_SETUP.md`](docs/ENV_SETUP.md).

## 6. Cấu trúc thư mục dự án

```text
agent4da/
├── app/
│   ├── backend/                 # FastAPI backend cho UI và Agent
│   └── frontend/                # Next.js Analytics Console
├── code/
│   ├── agent/                   # LangGraph Agent engine
│   ├── airflow/dags/            # Airflow DAGs
│   ├── kafka/                   # CSV producer và Kafka helper
│   └── spark/                   # Bronze, Silver, Gold Spark jobs
├── docs/                        # Tài liệu chi tiết theo module
├── envs/                        # Local environment files
├── init/                        # PostgreSQL init scripts
├── jars/                        # Spark/Iceberg/Hadoop/Kafka/PostgreSQL jars
├── monitoring/                  # Prometheus, Grafana dashboards, exporters docs
├── notebook/                    # Notebook xem và debug dữ liệu
├── script/                      # Helper scripts cho Spark/Kafka/PostgreSQL
├── trino/                       # Trino config và entrypoint
├── docker-compose.*.yml         # Compose files theo từng service group
├── Makefile                     # Service manager cho local stack
├── README.md                    # Tài liệu tổng quan này
└── PROJECT.md                   # Mô tả ngắn về project
```

## 7. Hướng dẫn cài đặt và triển khai

### 7.1. Thiết lập ban đầu

1. **Chuẩn bị các file môi trường**

   Kiểm tra các file trong `envs/`, đặc biệt:

   - `envs/minio.env`
   - `envs/postgre.env`
   - `envs/airflow.env`
   - `envs/iceberg.env`
   - `envs/spark.env`
   - `envs/app.env`
   - `envs/gemini.env` hoặc `envs/groq.env` cho LLM provider của AI Agent.

   Không hardcode secret vào source code hoặc README. Với môi trường local, thay các
   giá trị `change_me` bằng giá trị phù hợp trên máy chạy.

2. **Chuẩn bị JARs cho Spark**

   Các job Spark dùng JARs trong thư mục `jars/` để đọc Kafka, truy cập MinIO
   qua S3A, ghi Iceberg và kết nối PostgreSQL.

3. **Dùng Makefile làm lối chạy chính**

   Project đã có `Makefile` để gom các lệnh Docker Compose dài thành các lệnh
   ngắn, thống nhất và ít nhầm service/port hơn. Khi chạy bằng `make`, network
   `data_network` được tạo tự động.

   | Lệnh | Mục đích |
   | --- | --- |
   | `make all-build` | Build các image local cần thiết |
   | `make all-up` | Khởi động toàn bộ stack |
   | `make ps` | Xem trạng thái containers |
   | `make all-down` | Dừng toàn bộ stack |
   | `make agent-build && make agent-up` | Build và chạy riêng backend |
   | `make frontend-build && make frontend-up` | Build và chạy riêng frontend |
   | `make monitoring-up` | Chạy riêng Prometheus/Grafana stack |

### 7.2. Build và khởi động hệ thống

1. **Build các image local**

   ```bash
   make all-build
   ```

   Có thể build riêng từng nhóm:

   ```bash
   make airflow-build
   make agent-build
   make frontend-build
   ```

2. **Khởi động toàn bộ stack**

   ```bash
   make all-up
   ```

   Lệnh này khởi động Kafka, MinIO, Spark, Airflow, Trino, backend, frontend và
   monitoring. `make all-up` dùng image đã build sẵn, nên quy trình chuẩn là
   build trước rồi start stack.

3. **Kiểm tra trạng thái containers**

   ```bash
   make ps
   ```

4. **Dừng toàn bộ stack**

   ```bash
   make all-down
   ```

### 7.3. Nạp dữ liệu vào Kafka

Sau khi Kafka và Airflow container đã chạy, đưa file CSV trong thư mục `data/`
vào Kafka bằng script có sẵn:

```bash
script/kafka/producer.sh <ten_file_csv>
```

Ví dụ:

```bash
script/kafka/producer.sh event_jan.csv
script/kafka/producer.sh event_feb.csv
script/kafka/producer.sh event_mar.csv
script/kafka/producer.sh event_apr.csv
```

Script này sẽ chạy producer bên trong container Airflow, đọc file tại
`/opt/project/data/<ten_file_csv>` và gửi từng dòng CSV dưới dạng JSON vào Kafka
topic `ecommerce_events`. Mặc định broker là `kafka-kraft:29092`; có thể override
bằng biến môi trường `KAFKA_BROKER` hoặc `KAFKA_TOPIC` nếu cần.

### 7.4. Chạy pipeline dữ liệu

Pipeline dữ liệu được điều phối bằng Airflow. Bronze và Silver có lịch chạy mỗi
10 phút; Gold và Gold Metadata được trigger khi cần build lại layer phân tích.

Cách chạy trực tiếp trên Airflow UI:

1. Mở Airflow tại `http://localhost:8081`.
2. Vào danh sách DAGs.
3. Trigger lần lượt các DAG:
   - `bronze_pipeline`
   - `silver_pipeline`
   - `gold_pipeline`
   - `gold_metadata_pipeline`

Cách chạy nhanh bằng CLI trong container Airflow:

```bash
docker exec airflow airflow dags trigger bronze_pipeline
docker exec airflow airflow dags trigger silver_pipeline
docker exec airflow airflow dags trigger gold_pipeline
docker exec airflow airflow dags trigger gold_metadata_pipeline
```

Thứ tự chạy end-to-end:

1. Gửi CSV vào Kafka bằng `script/kafka/producer.sh <ten_file_csv>`.
2. Trigger `bronze_pipeline`.
3. Trigger `silver_pipeline`.
4. Trigger `gold_pipeline`.
5. Trigger `gold_metadata_pipeline`.
6. Mở Trino hoặc frontend để kiểm tra Gold data.

Query mẫu trong Trino:

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

### 7.5. Sử dụng Analytics Console

1. Mở frontend:

   ```text
   http://localhost:3000
   ```

2. Đăng nhập bằng admin được seed từ `envs/app.env`:

   ```text
   APP_BOOTSTRAP_ADMIN_EMAIL
   APP_BOOTSTRAP_ADMIN_PASSWORD
   ```

3. Vào các màn hình chính:

   - **Dashboard** để xem KPI và ranking.
   - **Ask** để hỏi AI Agent.
   - **History** để xem lại các lần hỏi.
   - **Catalog** để kiểm tra semantic metadata.
   - **Pipelines** để xem và trigger DAGs.
   - **Settings** để chọn provider/model và kiểm tra cấu hình.

### 7.6. Giám sát bằng Prometheus và Grafana

Monitoring được chạy cùng `make all-up`. Có thể chạy riêng bằng:

```bash
make monitoring-up
```

Các UI monitoring:

- Prometheus: `http://localhost:19090`
- Grafana: `http://localhost:13000`

Grafana có các dashboard:

- System Overview
- AI Agent Performance
- ETL Pipeline Monitoring
- Query / Data Layer

Backend expose Prometheus metrics tại:

```text
http://localhost:8083/metrics
```

![Prometheus targets và metrics](./imgs/prometheus.png)

![Grafana dashboard AI Agent Performance](./imgs/grafana-agent-performance.png)

## 8. Các URLs quan trọng

| Dịch vụ | URL | Mô tả |
| --- | --- | --- |
| Frontend Analytics Console | `http://localhost:3000` | Giao diện chính cho người dùng |
| Backend Swagger UI | `http://localhost:8083/docs` | Tài liệu API tương tác |
| Backend Metrics | `http://localhost:8083/metrics` | Prometheus scrape endpoint |
| Airflow UI | `http://localhost:8081` | Quản lý DAGs và task logs |
| MinIO Console | `http://localhost:9001` | Xem buckets `bronze`, `silver`, `gold` |
| MinIO S3 API | `http://localhost:9000` | Endpoint S3-compatible |
| Spark Master UI | `http://localhost:8080` | Theo dõi Spark cluster |
| Trino | `http://localhost:8082` | SQL query engine |
| Kafka external bootstrap | `localhost:9092` | Producer chạy từ host |
| PostgreSQL | `localhost:5432` | Shared DB cho catalog/app/Airflow |
| Prometheus | `http://localhost:19090` | Metrics và targets |
| Grafana | `http://localhost:13000` | Dashboards |

## 9. Checklist kiểm thử end-to-end

Checklist dưới đây giúp kiểm tra nhanh toàn bộ hệ thống từ ingestion đến UI và
monitoring:

1. **Kiểm tra kiến trúc**

   Đối chiếu các service chính: Kafka, Spark, MinIO, Iceberg, PostgreSQL, Trino,
   FastAPI, Next.js và monitoring.

2. **Kiểm tra Airflow DAGs**

   Mở Airflow tại `http://localhost:8081`, kiểm tra các DAG:

   - `bronze_pipeline`
   - `silver_pipeline`
   - `gold_pipeline`
   - `gold_metadata_pipeline`

3. **Kiểm tra dữ liệu trên MinIO**

   Mở MinIO tại `http://localhost:9001`, kiểm tra buckets:

   - `bronze`
   - `silver`
   - `gold`

4. **Query Gold data bằng Trino**

   Chạy query trên `iceberg.gold.daily_event_summary` hoặc
   `iceberg.metadata.semantic_table_catalog` để chứng minh Gold layer đã query
   được bằng SQL.

5. **Kiểm tra Analytics Console**

   Truy cập `http://localhost:3000`, vào Dashboard để xem KPI và Ask để hỏi AI
   Agent. Câu hỏi kiểm thử gợi ý:

   ```text
   Top 10 brand theo doanh thu trong tháng gần nhất
   ```

6. **Kiểm tra guardrail của Agent**

   Kiểm tra Agent không cho phép SQL ghi/xóa/sửa dữ liệu, chỉ sinh truy vấn đọc,
   có semantic metadata và có bước validate result.

7. **Kiểm tra monitoring**

   Mở Grafana tại `http://localhost:13000`, xem dashboard AI Agent Performance
   hoặc ETL Pipeline Monitoring để chứng minh hệ thống có observability.

## 10. Tài liệu chi tiết

Các tài liệu sâu hơn nằm trong thư mục `docs/`:

- [`docs/README.md`](docs/README.md): tài liệu tổng quan kỹ thuật chi tiết hơn.
- [`docs/ENV_SETUP.md`](docs/ENV_SETUP.md): các biến môi trường cần chuẩn bị.
- [`docs/AGENT_API.md`](docs/AGENT_API.md): API liên quan Agent.
- [`docs/AGENT_DEBUG.md`](docs/AGENT_DEBUG.md): hướng dẫn debug Agent.
- [`docs/TRINO_ENV.md`](docs/TRINO_ENV.md): cấu hình Trino.
- [`docs/GOLD_METADATA_PIPELINE.md`](docs/GOLD_METADATA_PIPELINE.md): metadata
  pipeline cho Agent.
- [`monitoring/README.md`](monitoring/README.md): module Prometheus/Grafana.

## 11. Cấu hình triển khai

- `make all-up` là lệnh khởi động chuẩn cho toàn bộ project, bao gồm cả
  PostgreSQL shared instance trong `docker-compose.airflow.yml`.
- Kafka producer chạy từ host sử dụng bootstrap server `localhost:9092`.
- Trino query Gold layer qua Iceberg catalog ở chế độ đọc an toàn.
- AI Agent sử dụng Gemini hoặc Groq thông qua các biến môi trường trong
  `envs/gemini.env` hoặc `envs/groq.env`.
- Frontend gọi backend qua `NEXT_PUBLIC_API_BASE_URL`; backend kiểm soát CORS
  bằng `APP_CORS_ORIGINS`.

---

*Tài liệu này được cập nhật lần cuối vào: 11/06/2026.*
