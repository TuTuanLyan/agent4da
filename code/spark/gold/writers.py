"""Shared Iceberg writers for Gold tasks."""

import re
from datetime import date, datetime


def log(message):
    print(f"[GoldWriter] {message}", flush=True)


def quote_sql_string(value):
    value = str(value)
    if "\x00" in value:
        raise ValueError("SQL string contains a null byte.")
    return "'" + value.replace("'", "''") + "'"


def sql_date_literal(value):
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        value = value.isoformat()
    value = str(value)[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"Invalid date literal: {value!r}")
    return f"DATE {quote_sql_string(value)}"


def date_in_condition(column_name, dates):
    values = sorted({str(value)[:10] for value in dates if value is not None})
    if not values:
        return "1 = 0"
    literals = ", ".join(sql_date_literal(value) for value in values)
    return f"`{column_name}` IN ({literals})"


def _temp_name(prefix, full_table_name):
    return re.sub(r"[^A-Za-z0-9_]", "_", f"{prefix}_{full_table_name}")


def _select_output(df, output_columns):
    return df.select(*output_columns) if output_columns else df


def write_full_refresh(df, full_table_name, output_columns=None, mode="full_refresh"):
    mode = str(mode).strip().lower()
    if mode != "full_refresh":
        raise NotImplementedError(
            "Only full_refresh is implemented for Gold Iceberg writes."
        )

    output_df = _select_output(df, output_columns)
    row_count = output_df.count()
    temp_view = _temp_name("_tmp", full_table_name)
    columns_sql = ", ".join(f"`{column}`" for column in output_df.columns)
    spark = output_df.sparkSession

    output_df.createOrReplaceTempView(temp_view)

    try:
        log(f"Full refresh via INSERT OVERWRITE: {full_table_name}")
        spark.sql(f"INSERT OVERWRITE {full_table_name} SELECT {columns_sql} FROM {temp_view}")
    except Exception as exc:
        log(f"INSERT OVERWRITE failed for {full_table_name}: {type(exc).__name__}: {exc}")
        log(f"Fallback full refresh: DELETE all rows, then append to {full_table_name}.")
        spark.sql(f"DELETE FROM {full_table_name} WHERE 1 = 1")
        if row_count > 0:
            output_df.writeTo(full_table_name).append()

    log(f"Rows written to {full_table_name}: {row_count}")
    return row_count


def write_replace_where(df, full_table_name, replace_condition_sql, output_columns=None):
    output_df = _select_output(df, output_columns)
    row_count = output_df.count()
    spark = output_df.sparkSession

    log(f"Replacing rows in {full_table_name} where {replace_condition_sql}")
    spark.sql(f"DELETE FROM {full_table_name} WHERE {replace_condition_sql}")
    if row_count > 0:
        output_df.writeTo(full_table_name).append()

    log(f"Rows written to {full_table_name}: {row_count}")
    return row_count


def write_replace_keys(df, full_table_name, key_column, output_columns=None):
    output_df = _select_output(df, output_columns)
    row_count = output_df.count()
    spark = output_df.sparkSession
    key_view = _temp_name("_keys", full_table_name)
    keys_df = output_df.select(key_column).where(f"`{key_column}` IS NOT NULL").distinct()
    key_count = keys_df.count()

    if key_count < 1:
        log(f"No keys to replace for {full_table_name}; rows={row_count}.")
        return row_count

    keys_df.createOrReplaceTempView(key_view)
    log(f"Replacing {key_count} keys in {full_table_name} by {key_column}")
    spark.sql(
        f"DELETE FROM {full_table_name} "
        f"WHERE `{key_column}` IN (SELECT `{key_column}` FROM {key_view})"
    )
    if row_count > 0:
        output_df.writeTo(full_table_name).append()

    log(f"Rows written to {full_table_name}: {row_count}")
    return row_count
