# Iceberg Gold Layer - Stage 1

Stage 1 chỉ chuẩn bị hạ tầng để Spark tạo, ghi và đọc Apache Iceberg table trên MinIO qua PostgreSQL JDBC Catalog. Stage này chưa tạo Gold fact/dim nghiệp vụ, chưa thêm Trino, chưa đổi lịch Bronze/Silver.

## Phiên bản

`docker-compose.spark.yml` đang dùng:

```bash
spark:4.1.1-scala2.13-java17-python3-ubuntu
```

JAR Iceberg dùng cho Spark 4.x / Scala 2.13:

```text
org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1
org.postgresql:postgresql:42.7.4
```

## Tải JAR local

Project không dùng `--packages` trong DAG production. Tải JAR một lần vào `jars/`:

```bash
bash script/spark/download_iceberg_jars.sh
```

Kết quả cần có:

```text
jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
jars/postgresql-42.7.4.jar
```

DAG và script submit không dùng `--jars`; chúng truyền JAR qua
`--driver-class-path` và `spark.executor.extraClassPath`. Vì `/opt/project/jars`
đã được mount vào Airflow, spark-master và spark-worker, cách này tránh Spark
copy JAR vào `log/spark/app-*` sau mỗi lần chạy.

## Init PostgreSQL schema

Postgres init script chỉ chạy khi volume được tạo lần đầu. Với DB đang tồn tại, chạy:

```bash
bash script/postgres/init_iceberg_schema.sh
```

Schema catalog là `iceberg`. Spark cấu hình PostgreSQL JDBC `currentSchema=iceberg`, nên metadata tables của Iceberg JDBC Catalog sẽ nằm trong schema này.

Kiểm tra PostgreSQL:

```bash
docker exec -it postgres-db psql -U bigdata -d agent4da
\dn
\dt iceberg.*
SELECT catalog_name, namespace, property_key, property_value
FROM iceberg.iceberg_namespace_properties
ORDER BY namespace, property_key;

SELECT catalog_name, table_namespace, table_name, metadata_location
FROM iceberg.iceberg_tables
ORDER BY table_namespace, table_name;
```

Sau khi smoke test chạy thành công, thường sẽ thấy các bảng metadata như `iceberg_tables` và `iceberg_namespace_properties`.
Với Stage 1, bảng cần thấy là `iceberg_catalog.gold.iceberg_smoke_test`.

## Bucket Gold trên MinIO

Warehouse vật lý dùng:

```text
s3a://gold/warehouse/
```

Bucket `gold` cần tồn tại trước khi chạy smoke test. Repo hiện chưa có init bucket script, nên tạo bằng MinIO UI tại `http://localhost:9001` hoặc dùng `mc`, ví dụ:

```bash
docker run --rm --network data_network minio/mc sh -c \
  'mc alias set local http://minio:9000 admin change_me && mc mb --ignore-existing local/gold'
```

Sau smoke test, kiểm tra bucket `gold` có prefix `warehouse/`.

## Chạy Smoke Test

Chạy thủ công bằng `spark-submit` qua container Airflow:

```bash
bash script/spark/submit_iceberg_smoke_test.sh
```

Chạy bằng Airflow:

1. Mở Airflow UI: `http://localhost:8081`
2. Trigger DAG `iceberg_smoke_test_pipeline`
3. Kiểm tra log có dòng:

```text
[IcebergSmokeTest] SUCCESS
```

Nên trigger DAG thật từ UI hoặc `airflow dags trigger`. Lệnh `airflow dags test`
chạy task trong foreground để debug và có thể tạo DagRun trong UI nhưng không
lưu task log đầy đủ vào `/opt/airflow/logs`.

Nếu cần kiểm tra task log trong container Airflow:

```bash
docker exec airflow sh -lc \
  'find /opt/airflow/logs/dag_id=iceberg_smoke_test_pipeline -type f -print'
```

Nếu Airflow chưa nhận DAG/JAR mới, restart các service liên quan:

```bash
docker compose -f docker-compose.airflow.yml restart airflow
docker compose -f docker-compose.spark.yml restart spark-master spark-worker
```

## Lỗi thường gặp

`ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog`
: Thiếu `iceberg-spark-runtime-4.0_2.13-1.10.1.jar` trong driver hoặc executor classpath.

`No suitable driver for jdbc:postgresql`
: Thiếu `postgresql-42.7.4.jar`.

`ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`
: Thiếu Hadoop S3A, Hadoop client hoặc AWS SDK bundle JAR.

`Access denied` hoặc `bucket not found`
: Kiểm tra MinIO credential và bucket `gold`.

`Cannot initialize catalog`
: Kiểm tra JDBC URI, user/password, schema `iceberg` và quyền `USAGE, CREATE`.

## Tiêu chí nghiệm thu

- `bash script/spark/download_iceberg_jars.sh` chạy thành công.
- JAR Iceberg và PostgreSQL xuất hiện trong `jars/`.
- `bash script/postgres/init_iceberg_schema.sh` chạy thành công.
- Airflow DAG `iceberg_smoke_test_pipeline` chạy `SUCCESS`.
- Spark log có `[IcebergSmokeTest] SUCCESS`.
- Spark đọc lại được bảng `iceberg_catalog.gold.iceberg_smoke_test`.
- MinIO bucket `gold` có dữ liệu dưới `warehouse/`.
- PostgreSQL có metadata catalog Iceberg trong schema `iceberg`.
- Bronze/Silver không bị thay đổi logic.
