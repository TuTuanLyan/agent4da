"""Shared Iceberg readers for Gold tasks."""

from gold.validators import assert_table_exists


def read_table(spark, full_table_name):
    return spark.table(full_table_name)


def read_required_table(spark, full_table_name):
    assert_table_exists(spark, full_table_name)
    return read_table(spark, full_table_name)
