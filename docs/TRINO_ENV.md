# Trino Environment

## 1. Vi Sao Can Env

Trino catalog properties can PostgreSQL password va MinIO access key/secret key. Cac secret nay khong duoc hard-code vao config tracked trong git.

`docker-compose.trino.yml` load env tu file hien co, sau do `trino/entrypoint.sh` generate catalog properties ben trong container.

## 2. Env Source

- `envs/postgre.env`: `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `envs/minio.env`: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`

## 3. Catalog Properties Duoc Tao Tu Env

Container tao cac file runtime nay:

- `/etc/trino/catalog/iceberg.properties`
- `/etc/trino/catalog/postgres.properties`

File tracked trong repo chi gom config khong chua secret:

- `trino/etc/config.properties`
- `trino/etc/node.properties`
- `trino/etc/jvm.config`
- `trino/etc/log.properties`
- `trino/entrypoint.sh`

## 4. Cach Chay Trino

```bash
docker compose -f docker-compose.trino.yml up -d
docker exec -it trino trino
```

Trino UI/API duoc expose tren host port `8082`.

## 5. Test Queries

Trong Trino CLI, catalog Iceberg la `iceberg` vi file runtime ten `iceberg.properties`.
Iceberg JDBC catalog name van la `iceberg_catalog` de khop voi Spark.

```sql
SELECT table_name, table_type, description, grain
FROM iceberg.metadata.table_catalog
ORDER BY table_name;

SELECT * FROM iceberg.gold.daily_event_summary LIMIT 10;

SELECT *
FROM postgres.public.some_table
LIMIT 10;
```

## 6. Luu Y Schema-Version V0

Trino image dang dung: `trinodb/trino:481`.

Trino 453+ default JDBC catalog schema-version la `V1`, trong khi Spark/Iceberg JDBC catalog hien tai cua project dung schema cu voi `iceberg_tables` va `iceberg_namespace_properties`. Vi vay Trino config set:

```properties
iceberg.jdbc-catalog.schema-version=V0
```

Voi Trino 481 va schema-version `V0`, `SELECT` co the chay binh thuong, nhung `SHOW TABLES FROM iceberg.gold` co the gap loi view support. De list table cho Agent, dung `iceberg.metadata.table_catalog`.
