#!/bin/sh
set -eu

require_env() {
  name="$1"
  eval "value=\${$name:-}"
  if [ -z "$value" ]; then
    echo "Missing required environment variable: $name" >&2
    exit 1
  fi
}

require_env POSTGRES_DB
require_env POSTGRES_USER
require_env POSTGRES_PASSWORD
require_env MINIO_ROOT_USER
require_env MINIO_ROOT_PASSWORD

mkdir -p /etc/trino/catalog

cat > /etc/trino/catalog/iceberg.properties <<EOF
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.catalog-name=iceberg_catalog
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres-db:5432/${POSTGRES_DB}?currentSchema=iceberg
iceberg.jdbc-catalog.connection-user=${POSTGRES_USER}
iceberg.jdbc-catalog.connection-password=${POSTGRES_PASSWORD}
iceberg.jdbc-catalog.default-warehouse-dir=s3://gold
iceberg.jdbc-catalog.schema-version=V0
iceberg.security=read_only
fs.s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=${MINIO_ROOT_USER}
s3.aws-secret-key=${MINIO_ROOT_PASSWORD}
EOF

cat > /etc/trino/catalog/postgres.properties <<EOF
connector.name=postgresql
connection-url=jdbc:postgresql://postgres-db:5432/${POSTGRES_DB}
connection-user=${POSTGRES_USER}
connection-password=${POSTGRES_PASSWORD}
EOF

echo "Generated Trino catalog configs from container environment."
echo "Iceberg catalog uses JDBC schema-version=V0 for Spark Iceberg JDBC metadata."

exec /usr/lib/trino/bin/run-trino
