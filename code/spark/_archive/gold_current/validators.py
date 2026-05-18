"""Gold validation helpers."""

from common.iceberg import run_sql, table_name
from common.logging_utils import log
from gold.schemas import GOLD_TABLE_NAMES, METADATA_TABLE_NAMES


def validate_namespaces(spark, config):
    run_sql(
        spark,
        f"SHOW NAMESPACES IN {config.catalog_name}",
        "Iceberg namespaces",
    ).show(200, truncate=False)


def validate_table_exists(spark, full_name):
    run_sql(spark, f"DESCRIBE TABLE {full_name}", f"Schema for {full_name}").show(
        200,
        truncate=False,
    )


def validate_output_counts(spark, table_names, config, namespace):
    for short_name in table_names:
        full_name = table_name(config.catalog_name, namespace, short_name)
        validate_table_exists(spark, full_name)
        row = run_sql(spark, f"SELECT COUNT(*) AS row_count FROM {full_name}").collect()[0]
        log("GoldJob", f"Count {full_name}: {row['row_count']}")


def validate_sample_queries(spark, config):
    sample_queries = [
        (
            "Sample daily_event_summary",
            f"""
            SELECT *
            FROM {table_name(config.catalog_name, config.gold_namespace, 'daily_event_summary')}
            ORDER BY event_date
            LIMIT 10
            """,
            10,
        ),
        (
            "Sample daily_product_summary",
            f"""
            SELECT *
            FROM {table_name(config.catalog_name, config.gold_namespace, 'daily_product_summary')}
            LIMIT 10
            """,
            10,
        ),
        (
            "Metric catalog",
            f"SELECT * FROM {table_name(config.catalog_name, config.metadata_namespace, 'metric_catalog')}",
            200,
        ),
    ]

    for description, sql, limit in sample_queries:
        try:
            run_sql(spark, sql, description).show(limit, truncate=False)
        except Exception as exc:
            log(
                "GoldJob",
                f"Optional sample query failed: {description}: {type(exc).__name__}: {exc}",
            )


def validate_outputs(spark, config):
    if not config.validate_tables:
        log("GoldJob", "GOLD_VALIDATE_TABLES=false. Skipping validation.")
        return

    validate_namespaces(spark, config)
    run_sql(
        spark,
        f"SHOW TABLES IN {config.catalog_name}.{config.gold_namespace}",
        "Gold tables",
    ).show(200, truncate=False)
    run_sql(
        spark,
        f"SHOW TABLES IN {config.catalog_name}.{config.metadata_namespace}",
        "Metadata tables",
    ).show(200, truncate=False)

    validate_output_counts(spark, GOLD_TABLE_NAMES, config, config.gold_namespace)
    validate_output_counts(spark, METADATA_TABLE_NAMES, config, config.metadata_namespace)
    validate_sample_queries(spark, config)

