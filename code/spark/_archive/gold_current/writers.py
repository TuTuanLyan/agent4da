"""Iceberg writers for Gold outputs."""

import re

from common.iceberg import run_sql, table_name
from common.logging_utils import log
from gold.schemas import EXTENDED_TABLE_NAMES, METADATA_TABLE_NAMES, MVP_TABLE_NAMES


def log_df_info(df, name):
    row_count = df.count()
    log("GoldJob", f"{name} rows: {row_count}")
    df.printSchema()
    return row_count


def write_table(df, full_name, refresh_mode, dry_run):
    spark = df.sparkSession
    row_count = log_df_info(df, full_name)

    if dry_run:
        log("GoldJob", f"GOLD_DRY_RUN=true. Skipping write for {full_name}.")
        return row_count

    if refresh_mode == "append":
        log("GoldJob", f"Appending to {full_name}. Re-runs can duplicate data.")
        if row_count > 0:
            df.writeTo(full_name).append()
        else:
            log("GoldJob", f"No rows to append for {full_name}.")
        return row_count

    if refresh_mode != "full_refresh":
        raise ValueError(f"Unsupported GOLD_REFRESH_MODE={refresh_mode!r}")

    temp_view = re.sub(r"[^A-Za-z0-9_]", "_", f"tmp_{full_name}")
    columns = ", ".join(f"`{column}`" for column in df.columns)
    df.createOrReplaceTempView(temp_view)

    try:
        log("GoldJob", f"Full refresh via INSERT OVERWRITE for {full_name}")
        run_sql(spark, f"INSERT OVERWRITE {full_name} SELECT {columns} FROM {temp_view}")
    except Exception as exc:
        log("GoldJob", f"INSERT OVERWRITE failed for {full_name}: {type(exc).__name__}: {exc}")
        log("GoldJob", f"Fallback full refresh for {full_name}: DELETE all rows, then append if non-empty.")
        run_sql(spark, f"DELETE FROM {full_name} WHERE 1 = 1")
        if row_count > 0:
            df.writeTo(full_name).append()

    return row_count


def write_named_tables(config, outputs, table_names, namespace):
    for short_name in table_names:
        cached_df = outputs[short_name].cache()
        full_name = table_name(config.catalog_name, namespace, short_name)
        try:
            write_table(cached_df, full_name, config.refresh_mode, config.dry_run)
        finally:
            cached_df.unpersist()


def write_all_mvp_tables(config, outputs):
    write_named_tables(config, outputs, MVP_TABLE_NAMES, config.gold_namespace)


def write_all_extended_tables(config, outputs):
    write_named_tables(config, outputs, EXTENDED_TABLE_NAMES, config.gold_namespace)


def write_all_metadata_tables(config, outputs):
    write_named_tables(config, outputs, METADATA_TABLE_NAMES, config.metadata_namespace)

