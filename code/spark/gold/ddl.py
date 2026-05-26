"""Shared Iceberg DDL helpers for Gold tasks."""

from gold.config import DEFAULT_ALLOWED_LOCATION_PREFIXES
from gold.identifiers import (
    assert_safe_table_location,
    quote_sql_string,
    validate_identifier_part,
)


def create_namespace_if_not_exists(spark, catalog, namespace):
    catalog = validate_identifier_part(catalog, "catalog")
    namespace = validate_identifier_part(namespace, "namespace")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def _split_table_identifier(full_table_name):
    parts = str(full_table_name).split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Expected full table name as catalog.namespace.table, got {full_table_name!r}."
        )

    catalog, namespace, table = parts
    return (
        validate_identifier_part(catalog, "catalog"),
        validate_identifier_part(namespace, "namespace"),
        validate_identifier_part(table, "table"),
    )


def _is_missing_metadata_error(exc):
    message = f"{type(exc).__name__}: {exc}"
    return (
        "FileNotFoundException" in message
        and "/metadata/" in message
        and ".metadata.json" in message
    )


def _is_allowed_location(location):
    if location is None:
        return False
    return any(str(location).startswith(prefix) for prefix in DEFAULT_ALLOWED_LOCATION_PREFIXES)


def _catalog_conf(spark, catalog):
    prefix = f"spark.sql.catalog.{catalog}."
    uri = spark.conf.get(f"{prefix}uri", "")
    user = spark.conf.get(f"{prefix}jdbc.user", "")
    password = spark.conf.get(f"{prefix}jdbc.password", "")
    schema = spark.conf.get(f"{prefix}jdbc.currentSchema", "iceberg")

    missing = [
        name
        for name, value in [
            ("uri", uri),
            ("jdbc.user", user),
            ("jdbc.password", password),
            ("jdbc.currentSchema", schema),
        ]
        if value is None or str(value).strip() == ""
    ]
    if missing:
        raise RuntimeError(
            f"Missing JDBC catalog config for {catalog}: {', '.join(missing)}."
        )

    return uri, user, password, validate_identifier_part(schema, "jdbc schema")


def _jdbc_table_name(schema, table):
    schema = validate_identifier_part(schema, "jdbc schema")
    table = validate_identifier_part(table, "jdbc table")
    return f"{schema}.{table}"


def _read_jdbc_metadata_location(spark, catalog, namespace, table):
    uri, user, password, schema = _catalog_conf(spark, catalog)
    conn = spark._jvm.java.sql.DriverManager.getConnection(uri, user, password)
    statement = None
    result = None
    try:
        statement = conn.prepareStatement(
            f"""
            SELECT metadata_location
            FROM {_jdbc_table_name(schema, "iceberg_tables")}
            WHERE catalog_name = ?
              AND table_namespace = ?
              AND table_name = ?
            """
        )
        statement.setString(1, catalog)
        statement.setString(2, namespace)
        statement.setString(3, table)
        result = statement.executeQuery()
        if result.next():
            return result.getString(1)
        return None
    finally:
        if result is not None:
            result.close()
        if statement is not None:
            statement.close()
        conn.close()


def _delete_jdbc_catalog_entry(spark, catalog, namespace, table, reason):
    uri, user, password, schema = _catalog_conf(spark, catalog)
    conn = spark._jvm.java.sql.DriverManager.getConnection(uri, user, password)
    statement = None
    try:
        statement = conn.prepareStatement(
            f"""
            DELETE FROM {_jdbc_table_name(schema, "iceberg_tables")}
            WHERE catalog_name = ?
              AND table_namespace = ?
              AND table_name = ?
            """
        )
        statement.setString(1, catalog)
        statement.setString(2, namespace)
        statement.setString(3, table)
        deleted = statement.executeUpdate()
        print(
            "[GoldDDL] Deleted stale JDBC catalog entry "
            f"{catalog}.{namespace}.{table}; rows={deleted}; reason={reason}.",
            flush=True,
        )
        return deleted
    finally:
        if statement is not None:
            statement.close()
        conn.close()


def _remove_catalog_entry_if_location_is_not_allowed(spark, full_table_name):
    catalog, namespace, table = _split_table_identifier(full_table_name)
    metadata_location = _read_jdbc_metadata_location(spark, catalog, namespace, table)
    if metadata_location is None or _is_allowed_location(metadata_location):
        return

    allowed = ", ".join(DEFAULT_ALLOWED_LOCATION_PREFIXES)
    print(
        "[GoldDDL] Existing JDBC catalog entry for "
        f"{full_table_name} points outside allowed Gold locations: "
        f"{metadata_location}. Removing it so this pipeline can recreate the "
        f"table under one of: {allowed}.",
        flush=True,
    )
    _delete_jdbc_catalog_entry(
        spark,
        catalog,
        namespace,
        table,
        "existing metadata_location is outside allowed Gold location prefixes",
    )


def _remove_broken_catalog_entry_directly(spark, full_table_name):
    catalog, namespace, table = _split_table_identifier(full_table_name)
    deleted = _delete_jdbc_catalog_entry(
        spark,
        catalog,
        namespace,
        table,
        "metadata.json file referenced by JDBC catalog is missing",
    )
    if deleted < 1:
        raise RuntimeError(
            f"Broken Iceberg catalog entry for {full_table_name} points to a missing "
            "metadata.json file, but no JDBC catalog row was deleted."
        )


def _table_location(spark, full_table_name):
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").collect()
    for row in rows:
        key = str(row.col_name).strip().lower()
        if key == "location":
            return str(row.data_type).strip()
    return None


def _assert_table_location_is_allowed(spark, full_table_name):
    location = _table_location(spark, full_table_name)
    if location is None:
        raise RuntimeError(f"Could not resolve location for Iceberg table {full_table_name}.")

    assert_safe_table_location(location, DEFAULT_ALLOWED_LOCATION_PREFIXES)


def _create_iceberg_table_sql(full_table_name, schema_sql, location, partition_clause):
    partition_sql = f"\nPARTITIONED BY ({partition_clause})" if partition_clause else ""
    return f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
          {schema_sql}
        )
        USING iceberg{partition_sql}
        LOCATION {quote_sql_string(location)}
        TBLPROPERTIES (
          'format-version'='2'
        )
        """


def create_iceberg_table_if_not_exists(
    spark,
    full_table_name,
    schema_sql,
    location,
    partition_clause=None,
):
    assert_safe_table_location(location, DEFAULT_ALLOWED_LOCATION_PREFIXES)
    _remove_catalog_entry_if_location_is_not_allowed(spark, full_table_name)
    create_sql = _create_iceberg_table_sql(
        full_table_name,
        schema_sql,
        location,
        partition_clause,
    )

    try:
        spark.sql(create_sql)
    except Exception as exc:
        if not _is_missing_metadata_error(exc):
            raise

        print(
            "[GoldDDL] Broken Iceberg metadata pointer found for "
            f"{full_table_name}; deleting catalog entry and recreating at "
            f"{location}.",
            flush=True,
        )
        _remove_broken_catalog_entry_directly(spark, full_table_name)

        spark.sql(create_sql)

    _assert_table_location_is_allowed(spark, full_table_name)
