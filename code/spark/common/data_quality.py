"""Small reusable data quality helpers."""

from pyspark.sql.functions import col, lit, lower, row_number, sum as spark_sum, trim, when
from pyspark.sql.window import Window


def safe_divide(numerator_col, denominator_col):
    return when(denominator_col == lit(0), lit(0.0)).otherwise(
        numerator_col.cast("double") / denominator_col.cast("double")
    )


def normalize_empty_to_null(value_col):
    cleaned = lower(trim(value_col))
    return (
        when(value_col.isNull(), lit(None).cast("string"))
        .when(cleaned.isin("", "null", "none", "nan"), lit(None).cast("string"))
        .otherwise(cleaned)
    )


def bool_sum(value):
    value_col = col(value) if isinstance(value, str) else value
    return spark_sum(when(value_col, 1).otherwise(0)).cast("long")


def required_not_null_filter(df, columns):
    filtered_df = df
    for column_name in columns:
        filtered_df = filtered_df.where(col(column_name).isNotNull())
    return filtered_df


def deduplicate_by_key(df, key_columns, order_columns):
    window = Window.partitionBy(*key_columns).orderBy(*order_columns)
    return (
        df
        .withColumn("_row_number", row_number().over(window))
        .where(col("_row_number") == 1)
        .drop("_row_number")
    )

