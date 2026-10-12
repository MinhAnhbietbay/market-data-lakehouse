from pathlib import Path
from typing import Any
import yaml


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)

def get_source_config(config: dict[str, Any], source_name: str) -> dict[str, Any]:
    return config["sources"][source_name]

def get_endpoint_config(
    config: dict[str, Any],
    source_name: str,
    endpoint_name: str,
) -> dict[str, Any]:
    source = get_source_config(config, source_name=source_name)
    return source["endpoints"][endpoint_name]

def get_landing_defaults(config: dict[str, Any]) -> dict[str, Any]:
    return config["defaults"]["landing"]

def get_request_defaults(config: dict[str, Any]) -> dict[str, Any]:
    return config["defaults"]["request"]

def get_rate_limit_delay(source_config: dict[str, Any]) -> float:
    """Giây tối thiểu giữa 2 request cho source này; 0.0 khi không cấu hình.

    Dùng `or {}` chứ không phải `.get(key, {})`: YAML `rate_limit:` để trống parse ra
    None, và None.get() sẽ nổ giữa đợt pull.
    """
    rate_limit = source_config.get("rate_limit") or {}
    return float(rate_limit.get("request_delay_seconds") or 0.0)
