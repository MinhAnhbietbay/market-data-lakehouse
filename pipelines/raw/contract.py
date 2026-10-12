from pathlib import Path
from typing import Any

import yaml


def load_contract(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_all_columns(contract: dict[str, Any]) -> list[dict[str, Any]]:
    return contract.get("columns", []) + contract.get("metadata_columns", [])


def contract_schema_ddl(contract: dict[str, Any]) -> str:
    """Sinh DDL Spark từ contract, để tạo được bảng raw khi CHƯA có dữ liệu landing.

    Endpoint bị khoá theo gói (402) chỉ thiếu *dữ liệu*; contract đã khai đủ *schema*, nên
    bảng dựng được ngay. Làm vậy để model hạ nguồn tham chiếu không gãy, và để schema được
    chốt trong metastore từ trước -- lúc mở gói chỉ việc đổ dữ liệu vào, không phải tạo lại.

    Tên cột luôn được quote: `range`, `event`, `unit` đều là tên cột thật trong repo và là
    từ khoá của Spark SQL.
    """
    columns = get_all_columns(contract)
    if not columns:
        raise ValueError(
            f"contract {contract.get('dataset', '<không rõ>')} không có cột nào -- "
            "đây là contract hỏng, không phải bảng rỗng hợp lệ"
        )
    return ", ".join(
        f"`{column['name']}` {column['type']}" + ("" if column.get("nullable", True) else " NOT NULL")
        for column in columns
    )


def get_target_table(contract: dict[str, Any]) -> str:
    return contract["target_table"]


def get_source(contract: dict[str, Any]) -> str:
    return contract["source"]


def get_source_endpoint(contract: dict[str, Any]) -> str:
    return contract["source_endpoint"]


def contract_is_selected(contract: dict[str, Any], sources: list[str], endpoints: list[str]) -> bool:
    if sources and get_source(contract) not in sources:
        return False
    if endpoints and get_source_endpoint(contract) not in endpoints:
        return False
    return True


def select_contract_paths(
    contract_paths: list[str],
    sources: list[str],
    endpoints: list[str],
) -> list[str]:
    selected = [
        contract_path
        for contract_path in contract_paths
        if contract_is_selected(load_contract(contract_path), sources, endpoints)
    ]
    if (sources or endpoints) and not selected:
        filters = [f"sources={sources}" if sources else "", f"endpoints={endpoints}" if endpoints else ""]
        raise ValueError(f"no raw contracts match {' '.join(value for value in filters if value)}")
    return selected
