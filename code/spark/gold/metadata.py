"""Build and validate semantic metadata for Gold tables.

This module keeps only executable logic. Business descriptions live in
gold.metadata_definitions and output table schemas live in gold.metadata_schema.
"""

from datetime import datetime, timezone

from pyspark.sql.functions import col

from gold.config import (
    COLUMN_CATALOG,
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_REFRESH_MODE,
    JOIN_CATALOG,
    METRIC_CATALOG,
    TABLE_CATALOG,
    table_location,
)
from gold.ddl import create_iceberg_table_if_not_exists, create_namespace_if_not_exists
from gold.identifiers import (
    assert_safe_table_location,
    table_identifier,
    validate_identifier_part,
)
from gold.metadata_definitions import (
    COLUMN_METADATA_BY_NAME,
    JOIN_DEFINITIONS,
    METRIC_DEFINITIONS,
    TABLE_DEFINITIONS,
    TABLE_SPECIFIC_COLUMN_METADATA,
)
from gold.metadata_schema import (
    COLUMN_CATALOG_SCHEMA,
    JOIN_CATALOG_SCHEMA,
    METADATA_TABLE_SPECS,
    METRIC_CATALOG_SCHEMA,
    TABLE_CATALOG_SCHEMA,
)
from gold.writers import write_full_refresh


def log(message):
    print(f"[GoldMetadata] {message}", flush=True)


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _default_business_name(column_name):
    return str(column_name).replace("_", " ").title()


def _table_namespace(spec, gold_namespace, staging_namespace):
    if spec["namespace_role"] == "staging":
        return staging_namespace
    return gold_namespace


def _table_keys():
    return [spec["table"] for spec in TABLE_DEFINITIONS]


def resolve_table_name(table_key, gold_namespace, staging_namespace):
    for spec in TABLE_DEFINITIONS:
        if spec["table"] == table_key:
            namespace = _table_namespace(spec, gold_namespace, staging_namespace)
            return f"{namespace}.{table_key}"
    raise ValueError(f"Unknown Gold metadata table key: {table_key}")


def expected_table_names(gold_namespace, staging_namespace):
    return {
        resolve_table_name(table_key, gold_namespace, staging_namespace)
        for table_key in _table_keys()
    }


def semantic_table_identifier(catalog_name, table_name):
    parts = str(table_name).split(".")
    if len(parts) != 2:
        raise ValueError(
            f"Expected semantic table name as namespace.table, got {table_name!r}."
        )
    return table_identifier(catalog_name, parts[0], parts[1])


def _validate_common_args(catalog_name, metadata_namespace, gold_namespace, staging_namespace):
    validate_identifier_part(catalog_name, "catalog")
    validate_identifier_part(metadata_namespace, "metadata namespace")
    validate_identifier_part(gold_namespace, "gold namespace")
    validate_identifier_part(staging_namespace, "staging namespace")


def _metadata_table_location(metadata_base_path, metadata_table):
    location = table_location(metadata_base_path, metadata_table)
    assert_safe_table_location(location, DEFAULT_ALLOWED_LOCATION_PREFIXES)
    return location


def _field_names(schema):
    return {field.name for field in schema.fields}


def _read_schema(spark, catalog_name, table_name, required):
    full_name = semantic_table_identifier(catalog_name, table_name)
    try:
        return spark.table(full_name).schema
    except Exception as exc:
        if not required:
            return None
        raise RuntimeError(
            "Required Gold table not found or unreadable: "
            f"{full_name}. Run gold_pipeline before gold_metadata_pipeline."
        ) from exc


def _load_required_gold_schemas(spark, catalog_name, gold_namespace, staging_namespace):
    schemas = {}
    for spec in TABLE_DEFINITIONS:
        table_name = resolve_table_name(spec["table"], gold_namespace, staging_namespace)
        schemas[table_name] = _read_schema(
            spark,
            catalog_name,
            table_name,
            required=True,
        )
    return schemas


def _validate_declared_column_metadata(schemas, gold_namespace, staging_namespace):
    errors = []
    for spec in TABLE_DEFINITIONS:
        table_key = spec["table"]
        table_name = resolve_table_name(table_key, gold_namespace, staging_namespace)
        schema = schemas.get(table_name)
        if schema is None:
            continue

        real_columns = _field_names(schema)
        for column_name in TABLE_SPECIFIC_COLUMN_METADATA.get(table_key, {}):
            if column_name not in real_columns:
                errors.append(
                    f"TABLE_SPECIFIC_COLUMN_METADATA[{table_key!r}] declares "
                    f"missing column {column_name!r} for {table_name}."
                )

    if errors:
        raise RuntimeError("Invalid metadata column definitions:\n- " + "\n- ".join(errors))


def _column_metadata(table_key, column_name):
    metadata = dict(COLUMN_METADATA_BY_NAME.get(column_name, {}))
    metadata.update(TABLE_SPECIFIC_COLUMN_METADATA.get(table_key, {}).get(column_name, {}))
    return metadata


def build_table_catalog_df(spark, gold_namespace, staging_namespace):
    now = _utc_now()
    rows = []
    for spec in TABLE_DEFINITIONS:
        table_name = resolve_table_name(spec["table"], gold_namespace, staging_namespace)
        rows.append(
            {
                "table_name": table_name,
                "layer": spec["layer"],
                "table_type": spec["table_type"],
                "business_name": spec["business_name"],
                "description": spec["description"],
                "grain": spec["grain"],
                "primary_key": spec["primary_key"],
                "unique_key": spec["unique_key"],
                "storage_format": "iceberg",
                "query_engine": "spark_sql",
                "is_agent_visible": spec["is_agent_visible"],
                "recommended_for_agent": spec["recommended_for_agent"],
                "refresh_frequency": "manual when Gold semantics change",
                "owner": "agent4da",
                "created_at": now,
                "updated_at": now,
            }
        )
    return spark.createDataFrame(rows, schema=TABLE_CATALOG_SCHEMA)


def build_column_catalog_df(spark, schemas, gold_namespace, staging_namespace):
    rows = []
    for spec in TABLE_DEFINITIONS:
        table_key = spec["table"]
        table_name = resolve_table_name(table_key, gold_namespace, staging_namespace)

        for field in schemas[table_name].fields:
            column_name = field.name
            metadata = _column_metadata(table_key, column_name)
            business_name = metadata.get("business_name") or _default_business_name(
                column_name
            )
            rows.append(
                {
                    "column_id": f"{table_name}.{column_name}",
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": field.dataType.simpleString(),
                    "business_name": business_name,
                    "description": metadata.get("description")
                    or f"{business_name} in {table_name}.",
                    "source_table": metadata.get("source_table") or table_name,
                    "source_column": metadata.get("source_column") or column_name,
                    "transformation_logic": metadata.get("transformation_logic")
                    or "Selected or derived by the Gold pipeline.",
                    "is_nullable": bool(field.nullable),
                    "is_dimension": bool(metadata.get("is_dimension", False)),
                    "is_metric": bool(metadata.get("is_metric", False)),
                    "is_time_column": bool(metadata.get("is_time_column", False)),
                    "is_join_key": bool(metadata.get("is_join_key", False)),
                    "is_unique_key": bool(metadata.get("is_unique_key", False)),
                    "example_values": metadata.get("example_values"),
                    "allowed_values": metadata.get("allowed_values"),
                    "agent_synonyms": metadata.get("agent_synonyms"),
                }
            )
    return spark.createDataFrame(rows, schema=COLUMN_CATALOG_SCHEMA)


def build_metric_catalog_df(spark, gold_namespace, staging_namespace):
    rows = []
    for metric in METRIC_DEFINITIONS:
        row = dict(metric)
        row["base_table"] = resolve_table_name(
            row["base_table"],
            gold_namespace,
            staging_namespace,
        )
        rows.append(row)
    return spark.createDataFrame(rows, schema=METRIC_CATALOG_SCHEMA)


def build_join_catalog_df(spark, gold_namespace, staging_namespace):
    rows = []
    for join in JOIN_DEFINITIONS:
        left_table = resolve_table_name(
            join["left_table"],
            gold_namespace,
            staging_namespace,
        )
        right_table = resolve_table_name(
            join["right_table"],
            gold_namespace,
            staging_namespace,
        )
        rows.append(
            {
                "join_id": (
                    f"{left_table}.{join['left_key']}__"
                    f"{right_table}.{join['right_key']}"
                ),
                "left_table": left_table,
                "left_key": join["left_key"],
                "right_table": right_table,
                "right_key": join["right_key"],
                "relationship_type": join["relationship_type"],
                "description": join["description"],
            }
        )
    return spark.createDataFrame(rows, schema=JOIN_CATALOG_SCHEMA)


def create_metadata_tables(spark, catalog_name, metadata_namespace, metadata_base_path):
    create_namespace_if_not_exists(spark, catalog_name, metadata_namespace)
    for metadata_table, spec in METADATA_TABLE_SPECS.items():
        full_name = table_identifier(catalog_name, metadata_namespace, metadata_table)
        location = _metadata_table_location(metadata_base_path, metadata_table)
        log(f"Ensuring metadata table: {full_name} at {location}")
        create_iceberg_table_if_not_exists(
            spark,
            full_name,
            spec.schema_sql,
            location,
        )


def build_metadata_catalogs(
    spark,
    catalog_name,
    metadata_namespace,
    gold_namespace,
    staging_namespace,
    metadata_base_path,
    refresh_mode=DEFAULT_REFRESH_MODE,
):
    mode = str(refresh_mode).strip().lower()
    if mode != DEFAULT_REFRESH_MODE:
        raise NotImplementedError(
            "Incremental metadata refresh is not implemented; "
            "use --refresh-mode full_refresh."
        )

    _validate_common_args(catalog_name, metadata_namespace, gold_namespace, staging_namespace)
    for metadata_table in METADATA_TABLE_SPECS:
        _metadata_table_location(metadata_base_path, metadata_table)

    log(f"Catalog name        : {catalog_name}")
    log(f"Metadata namespace  : {metadata_namespace}")
    log(f"Gold namespace      : {gold_namespace}")
    log(f"Staging namespace   : {staging_namespace}")
    log(f"Metadata base path  : {metadata_base_path}")
    log(f"Refresh mode        : {mode}")

    schemas = _load_required_gold_schemas(
        spark,
        catalog_name,
        gold_namespace,
        staging_namespace,
    )
    _validate_declared_column_metadata(schemas, gold_namespace, staging_namespace)
    create_metadata_tables(spark, catalog_name, metadata_namespace, metadata_base_path)

    outputs = {
        TABLE_CATALOG: build_table_catalog_df(
            spark,
            gold_namespace,
            staging_namespace,
        ),
        COLUMN_CATALOG: build_column_catalog_df(
            spark,
            schemas,
            gold_namespace,
            staging_namespace,
        ),
        METRIC_CATALOG: build_metric_catalog_df(
            spark,
            gold_namespace,
            staging_namespace,
        ),
        JOIN_CATALOG: build_join_catalog_df(
            spark,
            gold_namespace,
            staging_namespace,
        ),
    }

    row_counts = {}
    for metadata_table, df in outputs.items():
        full_name = table_identifier(catalog_name, metadata_namespace, metadata_table)
        write_full_refresh(
            df,
            full_name,
            METADATA_TABLE_SPECS[metadata_table].columns,
            mode=mode,
        )
        row_counts[metadata_table] = df.count()

    log("Metadata catalog full refresh completed.")
    return row_counts


def _metadata_full_name(catalog_name, metadata_namespace, metadata_table):
    return table_identifier(catalog_name, metadata_namespace, metadata_table)


def _assert_metadata_table_exists(spark, catalog_name, metadata_namespace, metadata_table):
    full_name = _metadata_full_name(catalog_name, metadata_namespace, metadata_table)
    try:
        spark.table(full_name).schema
    except Exception as exc:
        raise RuntimeError(
            f"Required metadata table not found or unreadable: {full_name}"
        ) from exc
    return full_name


def _duplicate_values(df, key_column):
    return [
        row[key_column]
        for row in (
            df.groupBy(key_column)
            .count()
            .where(col("count") > 1)
            .select(key_column)
            .collect()
        )
    ]


def _collect_metadata_tables(spark, catalog_name, metadata_namespace):
    tables = {}
    for metadata_table in METADATA_TABLE_SPECS:
        full_name = _assert_metadata_table_exists(
            spark,
            catalog_name,
            metadata_namespace,
            metadata_table,
        )
        tables[metadata_table] = spark.table(full_name).cache()
    return tables


def _read_known_gold_schemas(spark, catalog_name, table_names):
    schemas = {}
    for table_name in sorted(table_names):
        schemas[table_name] = _read_schema(
            spark,
            catalog_name,
            table_name,
            required=False,
        )
    return schemas


def _validate_duplicate_keys(metadata_tables, errors):
    checks = [
        (metadata_tables[TABLE_CATALOG], "table_name", TABLE_CATALOG),
        (metadata_tables[COLUMN_CATALOG], "column_id", COLUMN_CATALOG),
        (metadata_tables[METRIC_CATALOG], "metric_name", METRIC_CATALOG),
        (metadata_tables[JOIN_CATALOG], "join_id", JOIN_CATALOG),
    ]
    for df, key_column, table_name in checks:
        duplicates = _duplicate_values(df, key_column)
        if duplicates:
            errors.append(
                f"{table_name} has duplicate {key_column}: {', '.join(duplicates)}"
            )


def _validate_table_catalog(table_rows, gold_namespace, staging_namespace, errors):
    table_names = {row["table_name"] for row in table_rows}
    missing_tables = sorted(expected_table_names(gold_namespace, staging_namespace) - table_names)
    if missing_tables:
        errors.append(
            "table_catalog is missing required tables: " + ", ".join(missing_tables)
        )
    return table_names


def _validate_visible_table_schemas(table_rows, schema_cache, catalog_name, errors):
    for row in table_rows:
        table_name = row["table_name"]
        if bool(row["is_agent_visible"]) and schema_cache.get(table_name) is None:
            errors.append(
                "Agent-visible table does not exist or is unreadable: "
                f"{semantic_table_identifier(catalog_name, table_name)}"
            )


def _validate_column_catalog(column_df, table_names, schema_cache, errors):
    for row in column_df.select("column_id", "table_name", "column_name").collect():
        column_id = row["column_id"]
        table_name = row["table_name"]
        column_name = row["column_name"]
        expected_column_id = f"{table_name}.{column_name}"

        if table_name not in table_names:
            errors.append(f"column_catalog references unknown table: {table_name}")
        if column_id != expected_column_id:
            errors.append(
                f"column_catalog column_id mismatch: {column_id} != {expected_column_id}"
            )

        schema = schema_cache.get(table_name)
        if schema is not None and column_name not in _field_names(schema):
            errors.append(
                f"column_catalog references missing column: {table_name}.{column_name}"
            )


def _validate_metric_catalog(metric_df, table_names, errors):
    for row in metric_df.select("metric_name", "base_table").collect():
        if row["base_table"] not in table_names:
            errors.append(
                f"metric_catalog.{row['metric_name']} references unknown base_table: "
                f"{row['base_table']}"
            )


def _validate_join_catalog(join_df, table_names, schema_cache, errors):
    join_columns = ["join_id", "left_table", "left_key", "right_table", "right_key"]
    for row in join_df.select(*join_columns).collect():
        join_id = row["join_id"]
        left_schema = schema_cache.get(row["left_table"])
        right_schema = schema_cache.get(row["right_table"])

        _validate_join_side(
            join_id,
            "left",
            row["left_table"],
            row["left_key"],
            table_names,
            left_schema,
            errors,
        )
        _validate_join_side(
            join_id,
            "right",
            row["right_table"],
            row["right_key"],
            table_names,
            right_schema,
            errors,
        )


def _validate_join_side(join_id, side, table_name, key, table_names, schema, errors):
    if table_name not in table_names:
        errors.append(f"join_catalog.{join_id} references unknown {side}_table: {table_name}")
        return
    if schema is None:
        errors.append(f"join_catalog.{join_id} {side}_table is unreadable: {table_name}")
        return
    if key not in _field_names(schema):
        errors.append(f"join_catalog.{join_id} {side}_key missing: {table_name}.{key}")


def validate_metadata_catalogs(
    spark,
    catalog_name,
    metadata_namespace,
    gold_namespace,
    staging_namespace,
):
    _validate_common_args(catalog_name, metadata_namespace, gold_namespace, staging_namespace)
    metadata_tables = _collect_metadata_tables(spark, catalog_name, metadata_namespace)
    errors = []

    try:
        _validate_duplicate_keys(metadata_tables, errors)

        table_df = metadata_tables[TABLE_CATALOG]
        column_df = metadata_tables[COLUMN_CATALOG]
        metric_df = metadata_tables[METRIC_CATALOG]
        join_df = metadata_tables[JOIN_CATALOG]

        table_rows = table_df.select(
            "table_name",
            "is_agent_visible",
            "recommended_for_agent",
        ).collect()
        table_names = _validate_table_catalog(
            table_rows,
            gold_namespace,
            staging_namespace,
            errors,
        )
        schema_cache = _read_known_gold_schemas(spark, catalog_name, table_names)
        _validate_visible_table_schemas(table_rows, schema_cache, catalog_name, errors)

        existing_schemas = {
            table_name: schema
            for table_name, schema in schema_cache.items()
            if schema is not None
        }
        try:
            _validate_declared_column_metadata(
                existing_schemas,
                gold_namespace,
                staging_namespace,
            )
        except Exception as exc:
            errors.append(str(exc))

        _validate_column_catalog(column_df, table_names, schema_cache, errors)
        _validate_metric_catalog(metric_df, table_names, errors)
        _validate_join_catalog(join_df, table_names, schema_cache, errors)

        row_counts = {
            TABLE_CATALOG: table_df.count(),
            COLUMN_CATALOG: column_df.count(),
            METRIC_CATALOG: metric_df.count(),
            JOIN_CATALOG: join_df.count(),
        }

        if errors:
            raise RuntimeError("Metadata validation failed:\n- " + "\n- ".join(errors))

        log("Metadata validation passed.")
        return row_counts
    finally:
        for df in metadata_tables.values():
            df.unpersist()
