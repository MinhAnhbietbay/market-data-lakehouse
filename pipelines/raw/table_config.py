from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


DEFAULT_PARTITION_SPEC = [
    {
        "transform": "identity",
        "column": "run_date",
    }
]

DEFAULT_TABLE_PROPERTIES = {
    "format-version": "2",
}


def load_table_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if not isinstance(config, dict):
        raise ValueError(f"Table config must be a mapping: {path}")
    return config


def discover_table_configs(root: str | Path) -> dict[str, str]:
    configs: dict[str, str] = {}
    for path in sorted(Path(root).glob("**/*.yaml")):
        config = load_table_config(path)
        table_name = config.get("table")
        if table_name:
            if table_name in configs:
                raise ValueError(
                    f"Duplicate table config for {table_name}: {configs[table_name]} and {path}"
                )
            configs[table_name] = str(path)
    return configs


def get_partition_spec(table_config: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not table_config:
        return DEFAULT_PARTITION_SPEC
    return table_config.get("partition_strategy", {}).get("spec", DEFAULT_PARTITION_SPEC)


def get_table_properties(table_config: dict[str, Any] | None) -> dict[str, str]:
    if not table_config:
        return DEFAULT_TABLE_PROPERTIES
    return {
        **DEFAULT_TABLE_PROPERTIES,
        **table_config.get("table_properties", {}),
    }


def get_write_mode(table_config: dict[str, Any] | None, override: str | None = None) -> str:
    if override:
        return override
    if table_config:
        configured_mode = table_config.get("write", {}).get("mode")
        if configured_mode:
            return configured_mode
    return "overwrite-partitions"


def ensure_table_config_matches(table_config: dict[str, Any], table_name: str) -> None:
    configured_table = table_config.get("table")
    if configured_table != table_name:
        raise ValueError(
            f"Table config targets {configured_table}, but raw contract targets {table_name}"
        )
