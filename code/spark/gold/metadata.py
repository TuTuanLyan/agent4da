"""Build and validate simple semantic metadata for Gold Text-to-SQL."""

from pyspark.sql.functions import col

from gold.config import (
    DEFAULT_ALLOWED_LOCATION_PREFIXES,
    DEFAULT_REFRESH_MODE,
    SEMANTIC_COLUMN_CATALOG,
    SEMANTIC_TABLE_CATALOG,
    table_location,
)
from gold.ddl import create_iceberg_table_if_not_exists, create_namespace_if_not_exists
from gold.identifiers import (
    assert_safe_table_location,
    table_identifier,
    validate_identifier_part,
)
from gold.metadata_definitions import (
    AGENT_VISIBLE_COLUMNS,
    COMMON_COLUMN_DEFINITIONS,
    TABLE_COLUMN_OVERRIDES,
    TABLE_DEFINITIONS,
)
from gold.metadata_schema import (
    METADATA_TABLE_SPECS,
    SEMANTIC_COLUMN_CATALOG_SCHEMA,
    SEMANTIC_TABLE_CATALOG_SCHEMA,
)
from gold.writers import write_full_refresh


def log(message):
    print(f"[GoldAgentMetadata] {message}", flush=True)


def _table_keys():
    return [spec["table"] for spec in TABLE_DEFINITIONS]


def resolve_table_name(table_key, gold_namespace):
    if table_key not in _table_keys():
        raise ValueError(f"Unknown Gold metadata table key: {table_key}")
    return f"{gold_namespace}.{table_key}"


def expected_table_names(gold_namespace):
    return {resolve_table_name(table_key, gold_namespace) for table_key in _table_keys()}


def semantic_table_identifier(catalog_name, table_name):
    parts = str(table_name).split(".")
    if len(parts) != 2:
        raise ValueError(
            f"Expected semantic table name as namespace.table, got {table_name!r}."
        )
    return table_identifier(catalog_name, parts[0], parts[1])


def _validate_common_args(
    catalog_name,
    metadata_namespace,
    gold_namespace,
):
    validate_identifier_part(catalog_name, "catalog")
    validate_identifier_part(metadata_namespace, "metadata namespace")
    validate_identifier_part(gold_namespace, "gold namespace")


def _metadata_table_location(metadata_base_path, metadata_table):
    location = table_location(metadata_base_path, metadata_table)
    assert_safe_table_location(location, DEFAULT_ALLOWED_LOCATION_PREFIXES)
    return location


def _field_names(schema):
    return {field.name for field in schema.fields}


def _read_schema(spark, catalog_name, table_name, required):
    try:
        full_name = semantic_table_identifier(catalog_name, table_name)
        return spark.table(full_name).schema
    except Exception as exc:
        if not required:
            return None
        raise RuntimeError(
            "Required Gold table not found or unreadable: "
            f"{catalog_name}.{table_name}. Run gold_pipeline before "
            "gold_metadata_pipeline."
        ) from exc


def _load_required_gold_schemas(spark, catalog_name, gold_namespace):
    schemas = {}
    for table_key in _table_keys():
        table_name = resolve_table_name(table_key, gold_namespace)
        schemas[table_name] = _read_schema(
            spark,
            catalog_name,
            table_name,
            required=True,
        )
    return schemas


def _column_definition(table_key, column_name):
    definition = dict(COMMON_COLUMN_DEFINITIONS.get(column_name, {}))
    definition.update(TABLE_COLUMN_OVERRIDES.get(table_key, {}).get(column_name, {}))
    return definition


def _is_blank(value):
    return value is None or str(value).strip() == ""


def _validate_table_definitions(errors):
    seen = set()
    required_fields = [
        "table",
        "display_name",
        "purpose",
        "grain",
        "use_for",
        "query_notes",
        "is_agent_visible",
    ]
    for spec in TABLE_DEFINITIONS:
        table_key = spec.get("table")
        if table_key in seen:
            errors.append(f"Duplicate table definition: {table_key}")
        seen.add(table_key)

        for field_name in required_fields:
            if field_name == "is_agent_visible":
                if not isinstance(spec.get(field_name), bool):
                    errors.append(f"{table_key}.{field_name} must be boolean.")
            elif _is_blank(spec.get(field_name)):
                errors.append(f"{table_key}.{field_name} must not be blank.")


def _validate_declared_column_metadata(schemas, gold_namespace):
    errors = []
    _validate_table_definitions(errors)

    for table_key in _table_keys():
        table_name = resolve_table_name(table_key, gold_namespace)
        schema = schemas.get(table_name)
        if schema is None:
            errors.append(f"Missing schema for {table_name}.")
            continue

        real_columns = _field_names(schema)
        visible_columns = AGENT_VISIBLE_COLUMNS.get(table_key, [])
        duplicate_columns = sorted(
            {name for name in visible_columns if visible_columns.count(name) > 1}
        )
        if duplicate_columns:
            errors.append(
                f"Duplicate metadata columns for {table_name}: "
                + ", ".join(duplicate_columns)
            )
        if not visible_columns:
            errors.append(f"No visible metadata columns declared for {table_name}.")

        for column_name in visible_columns:
            if column_name not in real_columns:
                errors.append(
                    f"Metadata declares missing Gold column: {table_name}.{column_name}"
                )
                continue

            definition = _column_definition(table_key, column_name)
            for field_name in ["meaning", "business_terms", "example_usage"]:
                if _is_blank(definition.get(field_name)):
                    errors.append(
                        f"Missing {field_name} for {table_name}.{column_name}"
                    )

    unknown_tables = sorted(set(AGENT_VISIBLE_COLUMNS) - set(_table_keys()))
    if unknown_tables:
        errors.append(
            "AGENT_VISIBLE_COLUMNS references unknown tables: "
            + ", ".join(unknown_tables)
        )

    if errors:
        raise RuntimeError(
            "Invalid Gold agent metadata definitions:\n- " + "\n- ".join(errors)
        )


def build_semantic_table_catalog_df(spark, gold_namespace):
    rows = []
    for spec in TABLE_DEFINITIONS:
        rows.append(
            {
                "table_name": resolve_table_name(spec["table"], gold_namespace),
                "display_name": spec["display_name"],
                "purpose": spec["purpose"],
                "grain": spec["grain"],
                "use_for": spec["use_for"],
                "query_notes": spec["query_notes"],
                "is_agent_visible": bool(spec["is_agent_visible"]),
            }
        )
    return spark.createDataFrame(rows, schema=SEMANTIC_TABLE_CATALOG_SCHEMA)


def build_semantic_column_catalog_df(spark, schemas, gold_namespace):
    rows = []
    for table_key in _table_keys():
        table_name = resolve_table_name(table_key, gold_namespace)
        fields_by_name = {field.name: field for field in schemas[table_name].fields}

        for column_name in AGENT_VISIBLE_COLUMNS[table_key]:
            field = fields_by_name[column_name]
            definition = _column_definition(table_key, column_name)
            rows.append(
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": field.dataType.simpleString(),
                    "meaning": definition["meaning"],
                    "business_terms": definition["business_terms"],
                    "example_usage": definition["example_usage"],
                    "is_agent_visible": True,
                }
            )

    return spark.createDataFrame(rows, schema=SEMANTIC_COLUMN_CATALOG_SCHEMA)


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
    metadata_base_path,
    refresh_mode=DEFAULT_REFRESH_MODE,
):
    mode = str(refresh_mode).strip().lower()
    if mode != DEFAULT_REFRESH_MODE:
        raise NotImplementedError(
            "Incremental metadata refresh is not implemented; "
            "use --refresh-mode full_refresh."
        )

    _validate_common_args(
        catalog_name,
        metadata_namespace,
        gold_namespace,
    )
    for metadata_table in METADATA_TABLE_SPECS:
        _metadata_table_location(metadata_base_path, metadata_table)

    log(f"Catalog name        : {catalog_name}")
    log(f"Metadata namespace  : {metadata_namespace}")
    log(f"Gold namespace      : {gold_namespace}")
    log(f"Metadata base path  : {metadata_base_path}")
    log(f"Refresh mode        : {mode}")

    schemas = _load_required_gold_schemas(spark, catalog_name, gold_namespace)
    _validate_declared_column_metadata(schemas, gold_namespace)
    create_metadata_tables(spark, catalog_name, metadata_namespace, metadata_base_path)

    outputs = {
        SEMANTIC_TABLE_CATALOG: build_semantic_table_catalog_df(
            spark,
            gold_namespace,
        ),
        SEMANTIC_COLUMN_CATALOG: build_semantic_column_catalog_df(
            spark,
            schemas,
            gold_namespace,
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

    log("Gold agent metadata full refresh completed.")
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


def _duplicate_values(df, key_columns):
    rows = (
        df.groupBy(*key_columns)
        .count()
        .where(col("count") > 1)
        .select(*key_columns)
        .collect()
    )
    return [".".join(str(row[column]) for column in key_columns) for row in rows]


def _validate_duplicate_keys(metadata_tables, errors):
    checks = [
        (metadata_tables[SEMANTIC_TABLE_CATALOG], ["table_name"], SEMANTIC_TABLE_CATALOG),
        (
            metadata_tables[SEMANTIC_COLUMN_CATALOG],
            ["table_name", "column_name"],
            SEMANTIC_COLUMN_CATALOG,
        ),
    ]
    for df, key_columns, table_name in checks:
        duplicates = _duplicate_values(df, key_columns)
        if duplicates:
            errors.append(
                f"{table_name} has duplicate key values: {', '.join(duplicates)}"
            )


def _validate_table_catalog(table_rows, catalog_name, spark, errors):
    table_names = set()
    schema_cache = {}

    for row in table_rows:
        table_name = row["table_name"]
        table_names.add(table_name)
        schema = _read_schema(
            spark,
            catalog_name,
            table_name,
            required=False,
        )
        schema_cache[table_name] = schema
        if schema is None:
            errors.append(
                "semantic_table_catalog references missing Gold table: "
                f"{catalog_name}.{table_name}"
            )

    return table_names, schema_cache


def _validate_column_catalog(column_rows, table_names, schema_cache, errors):
    for row in column_rows:
        table_name = row["table_name"]
        column_name = row["column_name"]

        if table_name not in table_names:
            errors.append(
                "semantic_column_catalog references unknown table: "
                f"{table_name}"
            )
            continue

        schema = schema_cache.get(table_name)
        if schema is None:
            continue

        if column_name not in _field_names(schema):
            errors.append(
                "semantic_column_catalog references missing Gold column: "
                f"{table_name}.{column_name}"
            )


def validate_metadata_catalogs(
    spark,
    catalog_name,
    metadata_namespace,
    gold_namespace,
):
    _validate_common_args(
        catalog_name,
        metadata_namespace,
        gold_namespace,
    )
    metadata_tables = _collect_metadata_tables(spark, catalog_name, metadata_namespace)
    errors = []

    try:
        table_df = metadata_tables[SEMANTIC_TABLE_CATALOG]
        column_df = metadata_tables[SEMANTIC_COLUMN_CATALOG]

        row_counts = {
            SEMANTIC_TABLE_CATALOG: table_df.count(),
            SEMANTIC_COLUMN_CATALOG: column_df.count(),
        }
        log(f"{SEMANTIC_TABLE_CATALOG} rows: {row_counts[SEMANTIC_TABLE_CATALOG]}")
        log(f"{SEMANTIC_COLUMN_CATALOG} rows: {row_counts[SEMANTIC_COLUMN_CATALOG]}")

        _validate_duplicate_keys(metadata_tables, errors)

        table_rows = table_df.select("table_name", "is_agent_visible").collect()
        column_rows = column_df.select(
            "table_name",
            "column_name",
            "is_agent_visible",
        ).collect()

        table_names, schema_cache = _validate_table_catalog(
            table_rows,
            catalog_name,
            spark,
            errors,
        )
        _validate_column_catalog(column_rows, table_names, schema_cache, errors)

        if errors:
            log("Metadata validation errors:")
            for error in errors:
                log(f"- {error}")
            raise RuntimeError("Metadata validation failed:\n- " + "\n- ".join(errors))

        log("Metadata validation success.")
        return row_counts
    finally:
        for df in metadata_tables.values():
            df.unpersist()
