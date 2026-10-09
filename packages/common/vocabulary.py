from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class RawField:
    api: str
    endpoint: str
    field: str
    type: str
    meaning: str


def _load_yaml(path: Path) -> dict:
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_raw_fields(path: str | Path) -> list[RawField]:
    payload = _load_yaml(path)
    return [RawField(**entry) for entry in payload["fields"]]


def load_excluded_endpoints(path: str | Path) -> dict[str, str]:
    payload = _load_yaml(path)
    return {entry["endpoint"]: entry["reason"] for entry in payload["excluded_endpoints"]}


VALID_GRAINS = frozenset({"dimension", "daily_fact", "period_fact", "event", "series"})

VALID_ROLES = frozenset(
    {"identity", "event_date", "known_from_date", "attribute", "reference", "unknown"}
)


@dataclass(frozen=True)
class Area:
    code: str
    name: str
    status: str
    empty_reason: str = ""


@dataclass(frozen=True)
class Concept:
    name: str
    area_code: str
    grain: str
    description: str
    source_endpoints: tuple[str, ...]
    no_source: bool = False


@dataclass(frozen=True)
class Mapping:
    endpoint: str
    field: str
    concept: str
    role: str
    raw_name: str


def load_areas(path: str | Path) -> list[Area]:
    payload = _load_yaml(path)
    return [Area(**entry) for entry in payload["areas"]]


def load_concepts(directory: str | Path) -> list[Concept]:
    concepts: list[Concept] = []
    for file_path in sorted(Path(directory).glob("*.yaml")):
        payload = _load_yaml(file_path)
        area_code = payload["area_code"]
        for entry in payload["concepts"]:
            concepts.append(
                Concept(
                    name=entry["name"],
                    area_code=area_code,
                    grain=entry["grain"],
                    description=entry["description"],
                    source_endpoints=tuple(entry["source_endpoints"]),
                    no_source=entry.get("no_source", False),
                )
            )
    return concepts


def load_field_concept_map(path: str | Path) -> list[Mapping]:
    payload = _load_yaml(path)
    return [Mapping(**entry) for entry in payload["mappings"]]
