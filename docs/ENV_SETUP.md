# Environment Setup

`envs/*.env` chứa biến chạy local dev. Không đưa production secrets vào code,
DAG, compose hoặc docs. Thay các giá trị `change_me` bằng secret thật trên máy
triển khai.

## Secret Variables

```bash
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=change_me
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=change_me

# PostgreSQL
POSTGRES_USER=bigdata
POSTGRES_PASSWORD=change_me
POSTGRES_DB=agent4da

# Airflow database user created by init/01_init_schemas.sh
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=change_me

# Iceberg JDBC
ICEBERG_JDBC_USER=bigdata
ICEBERG_JDBC_PASSWORD=change_me

# Airflow UI and webserver
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=change_me
AIRFLOW__WEBSERVER__SECRET_KEY=change_me
AIRFLOW__CORE__FERNET_KEY=change_me

# External APIs
GROQ_API_KEY=change_me

# Analytics Console (Phase 1+ - see docs/WEB_APP_PLAN.md)
APP_JWT_SECRET=change_me
APP_BOOTSTRAP_ADMIN_PASSWORD=change_me
APP_DB_URL=postgresql+psycopg://bigdata:change_me@postgres-db:5432/agent4da
APP_AIRFLOW_PASSWORD=change_me
```

## Non-secret Variables

```bash
KAFKA_BOOTSTRAP=kafka-kraft:29092
KAFKA_TOPIC=ecommerce_events

MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET_BRONZE=bronze
MINIO_BUCKET_SILVER=silver
MINIO_BUCKET_GOLD=gold

SPARK_MASTER_URL=spark://spark-master:7077
SPARK_SHUFFLE_PARTITIONS=4
SPARK_DRIVER_PYTHON=/usr/local/bin/python3
SPARK_EXECUTOR_PYTHON=/usr/bin/python3

ICEBERG_CATALOG_NAME=iceberg_catalog
ICEBERG_WAREHOUSE=s3a://gold/warehouse/
ICEBERG_JDBC_URI=jdbc:postgresql://postgres-db:5432/agent4da
ICEBERG_JDBC_SCHEMA=iceberg

GOLD_NAMESPACE=gold
METADATA_NAMESPACE=metadata
SILVER_EVENTS_PATH=s3a://silver/ecommerce_events/
GOLD_RUN_MODE=all
GOLD_REFRESH_MODE=full_refresh
GOLD_DRY_RUN=false
GOLD_VALIDATE_TABLES=true

# Analytics Console (Phase 1+)
APP_ENV=local
APP_CORS_ORIGINS=http://localhost:3000
APP_JWT_ALG=HS256
APP_ACCESS_TOKEN_TTL_MIN=60
APP_REFRESH_TOKEN_TTL_DAYS=14
APP_BOOTSTRAP_ADMIN_EMAIL=admin@example.com
APP_TRINO_HOST=trino
APP_TRINO_PORT=8080
APP_TRINO_USER=agent4da_app
APP_AIRFLOW_BASE_URL=http://airflow:8080
APP_AIRFLOW_USER=admin
APP_AIRFLOW_AUTH=basic
APP_SPARK_MASTER_URL=http://spark-master:8080
APP_MINIO_ENDPOINT=http://minio:9000
APP_GROQ_MODEL_WHITELIST=llama-3.3-70b-versatile,llama-3.1-8b-instant
APP_ALLOW_TEMPERATURE_OVERRIDE=false
APP_AGENT_SQL_REPAIR=false
```

## Files

- `envs/minio.env`: MinIO account, access key, bucket names.
- `envs/postgre.env`: Postgres database and Airflow DB role secrets.
- `envs/airflow.env`: Airflow DB connection, admin user, Kafka runtime vars.
- `envs/iceberg.env`: Iceberg catalog/JDBC vars and Gold run options.
- `envs/spark.env`: Spark submit defaults.
- `envs/groq.env`: Groq API key for the agent (text-to-SQL) and the Answer-tab
  summary. Template at `envs/groq.env.example`. The populated file is gitignored.
- `envs/app.env`: Analytics Console secrets (JWT, admin bootstrap, DB URL).
  Template at `envs/app.env.example` (tracked); the populated file is
  gitignored.

## Groq API key — fixes "Groq is not configured"

The Console shows **"Groq is not configured. Add GROQ_API_KEY and restart the
backend."** when `GROQ_API_KEY` is empty or `envs/groq.env` is missing. The
backend reads it via `app/backend/api/settings.py` (env files
`/envs/app.env` + `/envs/groq.env`), and the agent reads it in
`code/agent/services/llm_service.py`. Get a key and add it:

1. Go to https://console.groq.com and sign in (Google, GitHub, or email — no
   credit card needed for the free tier).
2. Open **API Keys** (https://console.groq.com/keys) → **Create API Key**, give
   it a name (e.g. `agent4da-local`), and copy the value. It starts with `gsk_`.
   You can only see the full key once, so copy it now.
3. Put it in `envs/groq.env` (create from the template if needed):

   ```bash
   cp envs/groq.env.example envs/groq.env   # only if envs/groq.env does not exist
   # then edit envs/groq.env and set:
   GROQ_API_KEY=gsk_...your_real_key...
   ```

4. Restart the backend so it re-reads the env file:

   ```bash
   make app-down && make app-up
   # or just the API container:
   docker compose -f docker-compose.app.yml up -d --force-recreate app-api
   ```

5. Verify: open **Settings → Connection status** (Groq should flip to a green
   "configured"), or curl the redacted system view:

   ```bash
   curl -s http://localhost:8083/settings/system | grep -o '"groq":"[a-z]*"'
   # expect: "groq":"configured"
   ```

The model defaults to `llama-3.3-70b-versatile` (in
`APP_GROQ_MODEL_WHITELIST`). To use a different whitelisted model or skip the
summary call, set `AGENT_MODEL` / `AGENT_SUMMARIZE` in `envs/groq.env`.

## Airflow CLI error: "cannot use SQLite with the LocalExecutor"

If `docker exec -it airflow airflow <cmd>` fails with
`AirflowConfigException: error: cannot use SQLite with the LocalExecutor` or
`sqlite3.OperationalError: no such table: dag/job/task_instance`, the exec
shell is not seeing the Postgres connection. The scheduler/webserver get it
from `entrypoint.sh`, but a fresh `docker exec` shell does not inherit those
in-script `export`s, so the CLI falls back to the default SQLite DB.

Fix: `envs/airflow.env` defines `AIRFLOW__CORE__EXECUTOR` and
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` so every process in the container
(including exec shells) uses Postgres. After creating/editing it:

```bash
make airflow-down && make airflow-up      # recreate so the env_file is loaded
docker exec -it airflow airflow db check  # should connect to Postgres, no SQLite error
```

Keep the DB password in `envs/airflow.env` in sync with `AIRFLOW_DB_PASSWORD`
in `envs/postgre.env` (env files cannot reference each other).
