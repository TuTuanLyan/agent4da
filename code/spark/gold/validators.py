"""Validation helpers for Gold Spark tasks."""

from pyspark.sql.functions import col


def require_columns(df, required_columns, table_name):
    missing = [name for name in required_columns if name not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {table_name}: {', '.join(missing)}")


def require_non_null(df, column_name, table_name):
    null_count = df.where(col(column_name).isNull()).count()
    if null_count > 0:
        raise RuntimeError(
            f"{table_name}.{column_name} has {null_count} null rows."
        )


def assert_unique_key(df, key_column, table_name):
    total_count = df.count()
    distinct_count = df.select(key_column).distinct().count()
    if total_count != distinct_count:
        raise RuntimeError(
            f"{table_name} is not unique by {key_column}; "
            f"rows={total_count}, distinct={distinct_count}."
        )


def assert_no_duplicate(df, key_column, table_name):
    assert_unique_key(df, key_column, table_name)


def assert_table_exists(spark, full_table_name):
    try:
        spark.table(full_table_name).schema
    except Exception as exc:
        raise RuntimeError(
            f"Required Iceberg table not found or unreadable: {full_table_name}"
        ) from exc


def assert_count_equal(left_count, right_count, message):
    if left_count != right_count:
        raise RuntimeError(f"{message}: left={left_count}, right={right_count}")
