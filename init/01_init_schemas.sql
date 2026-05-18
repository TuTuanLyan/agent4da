-- =============================================================================
-- 01_init_schemas.sql
-- Chạy 1 lần duy nhất khi postgres volume được tạo lần đầu.
--
-- Schema layout — dùng chung 1 Postgres instance:
--   airflow     → Airflow metadata (DAG runs, task instances, XCom, ...)
--   iceberg     → Iceberg JDBC Catalog metadata
--   bronze_meta → Optional: catalog/stats cho bronze layer
--   silver_meta → Optional: catalog/stats cho silver layer
--   public      → App backend, general queries (mặc định)
--
-- User layout:
--   bigdata     → superuser của project, quyền mọi nơi
--   airflow_user → chỉ có quyền trên schema airflow (principle of least privilege)
-- =============================================================================

-- Tạo user riêng cho Airflow — idempotent
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow_user'
    ) THEN
        CREATE ROLE airflow_user WITH LOGIN PASSWORD 'Airflow123!';
    END IF;
END
$$;

-- Tạo các schema
CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS iceberg;
CREATE SCHEMA IF NOT EXISTS bronze_meta;
CREATE SCHEMA IF NOT EXISTS silver_meta;
-- public đã tồn tại mặc định

-- Phân quyền cho airflow_user — chỉ schema airflow
GRANT CONNECT ON DATABASE agent4da TO airflow_user;
GRANT USAGE, CREATE ON SCHEMA airflow TO airflow_user;
ALTER ROLE airflow_user SET search_path = airflow;

-- Default privileges: tables tạo sau trong schema airflow tự cấp quyền
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow
    GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow
    GRANT ALL ON SEQUENCES TO airflow_user;

-- bigdata: full quyền mọi schema
GRANT ALL ON SCHEMA airflow, iceberg, bronze_meta, silver_meta, public TO bigdata;
GRANT USAGE, CREATE ON SCHEMA iceberg TO bigdata;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iceberg TO bigdata;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA iceberg TO bigdata;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow     GRANT ALL ON TABLES TO bigdata;
ALTER DEFAULT PRIVILEGES IN SCHEMA iceberg     GRANT ALL ON TABLES TO bigdata;
ALTER DEFAULT PRIVILEGES IN SCHEMA iceberg     GRANT ALL ON SEQUENCES TO bigdata;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze_meta GRANT ALL ON TABLES TO bigdata;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver_meta GRANT ALL ON TABLES TO bigdata;
