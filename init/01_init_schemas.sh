#!/usr/bin/env bash
# Initialize project schemas and the dedicated Airflow database user.

set -euo pipefail

PROJECT_DB="${POSTGRES_DB:-agent4da}"
PROJECT_DB_USER="${POSTGRES_USER:-bigdata}"
AIRFLOW_DB_USER="${AIRFLOW_DB_USER:-airflow_user}"
AIRFLOW_DB_PASSWORD="${AIRFLOW_DB_PASSWORD:?Missing AIRFLOW_DB_PASSWORD}"

psql -v ON_ERROR_STOP=1 \
  --username "${PROJECT_DB_USER}" \
  --dbname "${PROJECT_DB}" \
  -v db_name="${PROJECT_DB}" \
  -v project_user="${PROJECT_DB_USER}" \
  -v airflow_user="${AIRFLOW_DB_USER}" \
  -v airflow_password="${AIRFLOW_DB_PASSWORD}" <<'SQL'
SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
WHERE NOT EXISTS (
    SELECT FROM pg_catalog.pg_roles WHERE rolname = :'airflow_user'
)
\gexec

SELECT format('ALTER ROLE %I WITH PASSWORD %L', :'airflow_user', :'airflow_password')
\gexec

CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS iceberg;
CREATE SCHEMA IF NOT EXISTS bronze_meta;
CREATE SCHEMA IF NOT EXISTS silver_meta;

GRANT CONNECT ON DATABASE :"db_name" TO :"airflow_user";
GRANT USAGE, CREATE ON SCHEMA airflow TO :"airflow_user";
ALTER ROLE :"airflow_user" SET search_path = airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA airflow
    GRANT ALL ON TABLES TO :"airflow_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow
    GRANT ALL ON SEQUENCES TO :"airflow_user";

GRANT ALL ON SCHEMA airflow, iceberg, bronze_meta, silver_meta, public TO :"project_user";
GRANT USAGE, CREATE ON SCHEMA iceberg TO :"project_user";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iceberg TO :"project_user";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA iceberg TO :"project_user";

ALTER DEFAULT PRIVILEGES IN SCHEMA airflow     GRANT ALL ON TABLES TO :"project_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA iceberg     GRANT ALL ON TABLES TO :"project_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA iceberg     GRANT ALL ON SEQUENCES TO :"project_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze_meta GRANT ALL ON TABLES TO :"project_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA silver_meta GRANT ALL ON TABLES TO :"project_user";
SQL
