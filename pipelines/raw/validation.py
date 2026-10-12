from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SUPPORTED_TYPES = {
    "string",
    "date",
    "timestamp",
    "integer",
    "int",
    "bigint",
    "double",
    "float",
    "boolean",
}


def all_contract_columns(contract: dict[str, Any]) -> list[dict[str, Any]]:
    return contract.get("columns", []) + contract.get("metadata_columns", [])


def non_nullable_column_names(contract: dict[str, Any]) -> list[str]:
    return [column["name"] for column in all_contract_columns(contract) if column.get("nullable") is False]


def validate_contract(contract: dict[str, Any]) -> None:
    required_top_level = [
        "dataset",
        "layer",
        "source",
        "source_endpoint",
        "target_table",
        "payload",
        "columns",
        "metadata_columns",
    ]
    missing = [key for key in required_top_level if key not in contract]
    if missing:
        raise ValueError(f"Contract {contract.get('dataset', '<unknown>')} is missing required key(s): {missing}")

    payload = contract["payload"]
    if "root" not in payload or "explode" not in payload:
        raise ValueError(f"Contract {contract['dataset']} payload must define root and explode")

    seen_names: set[str] = set()
    metadata_column_names = {column["name"] for column in contract.get("metadata_columns", [])}
    for column in all_contract_columns(contract):
        for required_key in ["name", "type"]:
            if required_key not in column:
                raise ValueError(f"Contract {contract['dataset']} column is missing {required_key}: {column}")

        if column["name"] not in metadata_column_names and "source" not in column and "value" not in column:
            raise ValueError(f"Contract {contract['dataset']} column must define source or value: {column}")

        if column["name"] in seen_names:
            raise ValueError(f"Contract {contract['dataset']} has duplicate column: {column['name']}")
        seen_names.add(column["name"])

        normalized_type = str(column["type"]).lower()
        if not (normalized_type in SUPPORTED_TYPES or normalized_type.startswith("decimal(")):
            raise ValueError(f"Contract {contract['dataset']} has unsupported type {column['type']} for column {column['name']}")


def validate_raw_dataframe(df: DataFrame, contract: dict[str, Any], fail_on_empty: bool = True) -> int:
    from pyspark.sql.functions import col

    expected_columns = [column["name"] for column in all_contract_columns(contract)]
    missing_columns = [column_name for column_name in expected_columns if column_name not in df.columns]
    if missing_columns:
        raise ValueError(f"Raw dataframe is missing contract column(s): {missing_columns}")

    row_count = df.count()
    if fail_on_empty and row_count == 0:
        raise ValueError(f"Raw dataframe for {contract['dataset']} is empty")

    for column_name in non_nullable_column_names(contract):
        null_count = df.filter(col(column_name).isNull()).limit(1).count()
        if null_count > 0:
            raise ValueError(f"Required raw column contains null values: {column_name}")

    return row_count
