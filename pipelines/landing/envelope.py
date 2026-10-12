from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pipelines.landing.templating import render_template

def mask_secret_query_params(url: str, secret_param_names: set[str]) -> str:
    parts = urlsplit(url)
    query_items = parse_qsl(parts.query, keep_blank_values=True)

    safe_query_items = []
    for key, value in query_items:
        if key in secret_param_names:
            safe_query_items.append((key, "***"))
        else:
            safe_query_items.append((key, value))

    safe_query = urlencode(safe_query_items)

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            safe_query,
            parts.fragment,
        )
    )


def build_partition_key(endpoint_config: dict[str, Any], values: dict[str, Any]) -> str:
    partition_template = endpoint_config["landing"]["partition_key"]
    return render_template(partition_template, values=values)


def build_landing_envelope(
    source_name: str,
    endpoint_name: str,
    run_date: str,
    partition_key: str,
    requested_at: str,
    request_url: str,
    request_params: dict[str, Any],
    response_status: int,
    payload: Any,
) -> dict[str, Any]:
    return {
        "source": source_name,
        "endpoint": endpoint_name,
        "run_date": run_date,
        "partition_key": partition_key,
        "requested_at": requested_at,
        "request_url": request_url,
        "request_params": request_params,
        "response_status": response_status,
        "payload": payload,
    }

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def current_run_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def build_object_key(
    landing_defaults: dict[str, Any],
    source_name: str,
    endpoint_name: str,
    run_date: str,
    partition_key: str,
) -> str:
    return render_template(
        landing_defaults["object_template"],
        {
            "prefix": landing_defaults["prefix"],
            "source": source_name,
            "endpoint": endpoint_name,
            "run_date": run_date,
            "partition_key": partition_key,
        },
    )

def validate_landing_envelope(envelope: dict[str, Any]) -> None:
    required_fields = [
        "source",
        "endpoint",
        "run_date",
        "partition_key",
        "requested_at",
        "request_url",
        "request_params",
        "response_status",
        "payload",
    ]
    missing_fields = [field for field in required_fields if field not in envelope]
    if missing_fields:
        raise ValueError(f"Landing envelope is missing required field(s): {missing_fields}")

    if not isinstance(envelope["response_status"], int):
        raise ValueError("Landing envelope field response_status must be an integer")

    if envelope["payload"] is None:
        raise ValueError("Landing envelope field payload must not be null")
