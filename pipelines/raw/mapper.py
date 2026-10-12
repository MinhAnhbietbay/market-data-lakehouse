from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, explode, input_file_name, lit, to_date
from pyspark.sql.types import DecimalType, StructType


def parse_type(type_name: str):
    normalized = type_name.lower()

    if normalized.startswith("decimal("):
        inside = normalized.removeprefix("decimal(").removesuffix(")")
        precision, scale = inside.split(",")
        return DecimalType(int(precision), int(scale))

    return normalized


def root_column(root: str):
    parts = root.split(".")
    result = col(parts[0])

    for part in parts[1:]:
        result = result[part]

    return result


def has_nested_field(schema: StructType, path: str) -> bool:
    current = schema
    for part in path.split("."):
        if not isinstance(current, StructType):
            return False
        field = next((field for field in current.fields if field.name == part), None)
        if field is None:
            return False
        current = field.dataType
    return True


def source_expression(source_field: str, payload_df: DataFrame):
    metadata_sources = {
        "@source": col("source"),
        "@endpoint": col("endpoint"),
        "@run_date": col("run_date"),
        "@partition_key": col("partition_key"),
        "@requested_at": col("requested_at"),
        "@response_status": col("response_status"),
    }

    if source_field in metadata_sources:
        return metadata_sources[source_field]

    payload_path = f"_payload_row.{source_field}"
    if has_nested_field(payload_df.schema, payload_path):
        return col(payload_path)

    return lit(None)


def apply_payload_shape(df: DataFrame, contract: dict[str, Any]) -> DataFrame:
    payload_config = contract.get("payload", {"root": "payload", "explode": True})
    root = payload_config.get("root", "payload")
    should_explode = payload_config.get("explode", True)
    payload_col = root_column(root)

    if should_explode:
        return df.withColumn("_payload_row", explode(payload_col))

    return df.withColumn("_payload_row", payload_col)


def build_raw_dataframe(landing_df: DataFrame, contract: dict[str, Any]) -> DataFrame:
    filtered_df = landing_df.filter(col("response_status") == 200)
    payload_df = apply_payload_shape(filtered_df, contract)
    select_exprs = []

    for column in contract["columns"]:
        target_name = column["name"]
        target_type = parse_type(column["type"])

        if "value" in column:
            expr = lit(column["value"])
        else:
            expr = source_expression(column["source"], payload_df)

        select_exprs.append(expr.cast(target_type).alias(target_name))

    metadata_exprs = {
        "source": col("source").cast("string"),
        "source_endpoint": col("endpoint").cast("string"),
        "run_date": to_date(col("run_date")),
        "partition_key": col("partition_key").cast("string"),
        "requested_at": col("requested_at").cast("timestamp"),
        "response_status": col("response_status").cast("int"),
        "landing_key": input_file_name(),
        "ingested_at": current_timestamp(),
    }

    for column in contract.get("metadata_columns", []):
        name = column["name"]
        target_type = parse_type(column["type"])
        expr = metadata_exprs.get(name, lit(None))
        select_exprs.append(expr.cast(target_type).alias(name))

    return payload_df.select(*select_exprs)
