from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

SCHEMA_VERSION = 1
FINAL_STATUSES = {"success", "failed"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass
class RunManifest:
    run_type: str
    run_date: str
    inputs: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION
    run_id: str = field(default_factory=lambda: str(uuid4()))
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str | None = None
    duration_seconds: float | None = None
    status: str = "running"
    outputs: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, int | float | str] = field(default_factory=dict)
    failures: list[dict[str, Any]] = field(default_factory=list)

    def add_output(self, **output: Any) -> None:
        self.outputs.append(output)

    def add_failure(self, **failure: Any) -> None:
        self.failures.append(failure)

    def finish(self) -> None:
        self.finished_at = utc_now_iso()
        self.duration_seconds = round(
            (parse_timestamp(self.finished_at) - parse_timestamp(self.started_at)).total_seconds(),
            3,
        )
        self.status = "failed" if self.failures else "success"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def write(self, path: str | Path) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
        temporary_path.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary_path.replace(output_path)
        return output_path


def manifest_path(root: str | Path, manifest: RunManifest) -> Path:
    return Path(root) / manifest.run_type / manifest.run_date / f"{manifest.run_id}.json"


def resolve_manifest_path(
    manifest: RunManifest,
    explicit_path: str | Path | None,
    manifest_root: str | Path,
) -> Path:
    return Path(explicit_path) if explicit_path else manifest_path(manifest_root, manifest)


def load_manifest(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError("manifest root must be an object")
    return data


def validate_manifest(data: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = {
        "schema_version", "run_id", "run_type", "run_date", "started_at",
        "finished_at", "duration_seconds", "status", "inputs", "outputs",
        "metrics", "failures",
    }
    missing = sorted(required - set(data))
    if missing:
        errors.append(f"missing required field(s): {', '.join(missing)}")

    if data.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"unsupported schema_version: {data.get('schema_version')}")
    try:
        UUID(str(data.get("run_id")))
    except ValueError:
        errors.append("run_id must be a UUID")
    if not isinstance(data.get("run_type"), str) or not data.get("run_type"):
        errors.append("run_type must be a non-empty string")
    try:
        date.fromisoformat(str(data.get("run_date")))
    except ValueError:
        errors.append("run_date must be an ISO-8601 date")
    if data.get("status") not in FINAL_STATUSES:
        errors.append(f"status must be one of: {', '.join(sorted(FINAL_STATUSES))}")
    for field_name in ("inputs", "metrics"):
        if not isinstance(data.get(field_name), dict):
            errors.append(f"{field_name} must be an object")
    for field_name in ("outputs", "failures"):
        if not isinstance(data.get(field_name), list):
            errors.append(f"{field_name} must be an array")

    try:
        started = parse_timestamp(str(data.get("started_at")))
        finished = parse_timestamp(str(data.get("finished_at")))
        if started.tzinfo is None or finished.tzinfo is None:
            errors.append("started_at and finished_at must include a timezone")
        elif finished < started:
            errors.append("finished_at must not be before started_at")
    except ValueError:
        errors.append("started_at and finished_at must be ISO-8601 timestamps")

    duration = data.get("duration_seconds")
    if not isinstance(duration, (int, float)) or isinstance(duration, bool) or duration < 0:
        errors.append("duration_seconds must be a non-negative number")
    failures = data.get("failures")
    if isinstance(failures, list):
        if data.get("status") == "success" and failures:
            errors.append("success manifest must not contain failures")
        if data.get("status") == "failed" and not failures:
            errors.append("failed manifest must contain at least one failure")
    return errors


def discover_manifest_paths(root: str | Path) -> list[Path]:
    return sorted(Path(root).glob("**/*.json"))
