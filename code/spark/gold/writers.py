"""Shared Iceberg writers for Gold tasks."""

import re


def log(message):
    print(f"[GoldWriter] {message}", flush=True)


def write_full_refresh(df, full_table_name, output_columns=None, mode="full_refresh"):
    mode = str(mode).strip().lower()
    if mode != "full_refresh":
        raise NotImplementedError(
            "Only full_refresh is implemented for Gold Iceberg writes."
        )

    output_df = df.select(*output_columns) if output_columns else df
    row_count = output_df.count()
    temp_view = re.sub(r"[^A-Za-z0-9_]", "_", f"_tmp_{full_table_name}")
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
