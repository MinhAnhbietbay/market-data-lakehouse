from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import bucket, col, days, hours, months, years

from pipelines.raw.table_config import get_partition_spec, get_table_properties


def namespace_from_table(table_name: str) -> str:
    parts = table_name.split(".")
    return ".".join(parts[:-1])


def ensure_namespace(spark: SparkSession, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def build_partition_field(field: dict[str, Any]) -> str | Column:
    transform = field["transform"]
    column_name = field["column"]

    if transform == "identity":
        return column_name
    if transform == "years":
        return years(col(column_name))
    if transform == "months":
        return months(col(column_name))
    if transform == "days":
        return days(col(column_name))
    if transform == "hours":
        return hours(col(column_name))
    if transform == "bucket":
        return bucket(int(field["buckets"]), col(column_name))

    raise ValueError(f"Unsupported runtime partition transform: {transform}")


def write_iceberg_table(
    df: DataFrame,
    table_name: str,
    write_mode: str = "overwrite-partitions",
    table_config: dict[str, Any] | None = None,
) -> None:
    namespace = namespace_from_table(table_name)
    ensure_namespace(df.sparkSession, namespace)

    writer = df.writeTo(table_name).using("iceberg")
    for key, value in get_table_properties(table_config).items():
        writer = writer.tableProperty(key, str(value))

    partition_fields = [build_partition_field(field) for field in get_partition_spec(table_config)]
    writer = writer.partitionedBy(*partition_fields)

    if write_mode == "replace-table":
        writer.createOrReplace()
        return

    if write_mode == "append":
        if table_exists(df.sparkSession, table_name):
            writer.append()
        else:
            writer.create()
        return

    if write_mode == "overwrite-partitions":
        if table_exists(df.sparkSession, table_name):
            writer.overwritePartitions()
        else:
            writer.create()
        return

    raise ValueError(f"Unsupported Iceberg write_mode: {write_mode}")
