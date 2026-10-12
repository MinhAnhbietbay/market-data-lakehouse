import argparse
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any

from dotenv import load_dotenv

from pipelines.landing.config import (
    get_endpoint_config,
    get_landing_defaults,
    get_rate_limit_delay,
    get_request_defaults,
    get_source_config,
    load_yaml,
)
from pipelines.landing.http_client import ApiResponse, apply_header_auth, fetch_json, prepare_request, retry_decision
from pipelines.landing.envelope import (
    build_landing_envelope,
    build_object_key,
    build_partition_key,
    mask_secret_query_params,
    validate_landing_envelope,
)
from pipelines.landing.object_store import write_local_json, write_minio_json
from pipelines.landing.rate_limiter import RateLimiter

_RATE_LIMITERS: dict[str, RateLimiter] = {}
# Tách ra biến module để test thay bằng no-op (conftest.py) -- retry 429 chờ 60 giây.
_sleep = time.sleep


def fetch_with_retry(
    url: str,
    timeout_seconds: int,
    request_defaults: dict[str, Any],
    before_attempt: Callable[[], None] | None = None,
    **fetch_kwargs: Any,
) -> ApiResponse:
    """Gọi `fetch_json`, thử lại theo `retry_decision` tối đa `max_retries` lần.

    `before_attempt` là `RateLimiter.wait` của source: MỖI lần thử (kể cả thử lại) đều là một
    request FMP đếm vào trần 750/phút, nên đều phải qua limiter. Trả response của lần thử cuối
    -- caller (record_output/classify_error) vẫn là nơi duy nhất quyết định dừng run.
    """
    max_retries = int(request_defaults.get("max_retries", 0))
    backoff_factor = float(request_defaults.get("retry_backoff_factor", 2))
    rate_limit_backoff = float(request_defaults.get("rate_limit_backoff_seconds", 60))
    attempt = 0
    while True:
        if before_attempt is not None:
            before_attempt()
        response = fetch_json(url=url, timeout_seconds=timeout_seconds, **fetch_kwargs)
        attempt += 1
        decision = retry_decision(response, attempt, backoff_factor, rate_limit_backoff)
        if not decision.retry or attempt > max_retries:
            return response
        print(
            f"retry {attempt}/{max_retries} after HTTP {response.status}: "
            f"sleeping {decision.wait_seconds:.0f}s"
        )
        _sleep(decision.wait_seconds)



def get_rate_limiter(source_name: str, source_config: dict[str, Any]) -> RateLimiter:
    """Một limiter cho mỗi source, dùng chung toàn process.

    Hạn mức là của SOURCE (mọi endpoint FMP chung một quota), nên limiter phải sống
    ngoài phạm vi một lần gọi ingest_one_with_payload -- nếu không, mỗi call tự khởi tạo
    lại và không có tiết chế nào cả.
    """
    if source_name not in _RATE_LIMITERS:
        _RATE_LIMITERS[source_name] = RateLimiter(get_rate_limit_delay(source_config))
    return _RATE_LIMITERS[source_name]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw API payloads into the landing zone")

    parser.add_argument("--config", default="configs/sources/data_sources.yaml")
    parser.add_argument("--source", required=True)
    parser.add_argument("--endpoint", required=True)

    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--run-date")
    parser.add_argument("--write-target", choices=["local", "minio", "both"], default="local")
    parser.add_argument("--landing-root", default="/tmp/lakehouse-landing")

    parser.add_argument("--include-disabled", action="store_true")
    parser.add_argument("--series-id", action="append", default=[])

    return parser.parse_args()


def build_runtime_values(
    endpoint_config: dict[str, Any],
    symbol: str | None = None,
    series_id: str | None = None,
    group: str | None = None,
) -> list[dict[str, Any]]:
    scope = endpoint_config["scope"]

    if scope == "per_symbol":
        if not symbol:
            raise ValueError("Endpoint scope per_symbol requires --symbol")
        return [{"symbol": symbol}]

    if scope == "macro_series":
        if not series_id:
            raise ValueError("Endpoint scope macro_series requires --series-id")
        return [{"series_id": series_id}]

    if scope == "per_group":
        # A10 (sector/industry aggregates) lặp theo NHÓM, không theo symbol. Dùng khoá
        # trung tính "group" thay vì "sector"/"industry" để một nhánh phục vụ cả 4
        # endpoint; tên param thật của FMP nằm ở endpoint_config["group_param"] và được
        # render qua params, không hard-code ở đây.
        if not group:
            raise ValueError("Endpoint scope per_group requires a group value")
        return [{"group": group}]

    if scope in {"reference", "market_wide"}:
        return [{}]

    if scope == "paged_reference":
        # Always start at page 0 for a single-page fetch. Do NOT read this from
        # endpoint_config["params"]["page"] — that config value is a template
        # placeholder (e.g. "{page}") meant to be filled in by runtime `values`,
        # not a literal default (see pipelines/landing/run_batch.py::ingest_paged_reference
        # for the real multi-page loop, which fills in successive page numbers).
        return [{"page": 0}]

    raise ValueError(f"Unsupported endpoint scope: {scope}")


def write_envelope(
    write_target: str,
    landing_root: str,
    bucket: str,
    object_key: str,
    envelope: dict[str, Any],
) -> str:
    if write_target == "local":
        return write_local_json(
            root=landing_root,
            object_key=object_key,
            data=envelope,
        )

    if write_target == "minio":
        return write_minio_json(
            bucket=bucket,
            object_key=object_key,
            data=envelope,
        )

    if write_target == "both":
        # Bản cục bộ là bản gốc (spec 2026-09-30 §1): ghi nó TRƯỚC, để đứt giữa chừng thì bản
        # gốc vẫn còn; object_exists("both") coi thiếu MinIO là chưa xong.
        write_local_json(root=landing_root, object_key=object_key, data=envelope)
        return write_minio_json(bucket=bucket, object_key=object_key, data=envelope)

    raise ValueError(f"Unsupported write target: {write_target}")


def _build_and_write_envelope(
    source_name: str,
    endpoint_name: str,
    landing_defaults: dict[str, Any],
    run_date: str,
    partition_key: str,
    request_url: str,
    request_params: dict[str, Any],
    response: ApiResponse,
    payload: Any,
    write_target: str,
    landing_root: str,
    quarantine_non_2xx: bool = False,
) -> str:
    """Build+validate the landing envelope, compute its object key, and write it.

    The tail shared by every `ingest_one*_with_payload` variant, regardless of how the
    request itself was built (GET+query-auth vs POST+header+body) -- factored out so a
    second request-building path doesn't have to re-duplicate the envelope/write
    mechanics (fix round 1 finding (a)).

    `payload` is accepted SEPARATELY from `response.payload`: `response` still carries
    the real `status`/`size_bytes` for the envelope's `response_status` field and for
    bandwidth accounting, but the caller may want to write something other than the
    raw wire payload into the envelope body -- e.g. `ingest_one_batch_with_payload`
    zips OpenFIGI's positional per-job result list against the jobs that produced it
    before writing, rather than writing the raw (position-only, job-less) response.
    """
    requested_at = datetime.now(timezone.utc).isoformat()

    envelope = build_landing_envelope(
        source_name=source_name,
        endpoint_name=endpoint_name,
        run_date=run_date,
        partition_key=partition_key,
        requested_at=requested_at,
        request_url=request_url,
        request_params=request_params,
        response_status=response.status,
        payload=payload,
    )
    validate_landing_envelope(envelope)

    is_ok = 200 <= response.status < 300
    object_key_run_date = run_date if is_ok or not quarantine_non_2xx else f"{run_date}__non_2xx"
    object_key = build_object_key(
        landing_defaults=landing_defaults,
        source_name=source_name,
        endpoint_name=endpoint_name,
        run_date=object_key_run_date,
        partition_key=partition_key,
    )

    return write_envelope(
        write_target=write_target,
        landing_root=landing_root,
        bucket=landing_defaults["bucket"],
        object_key=object_key,
        envelope=envelope,
    )


def ingest_one_with_payload(
    config: dict[str, Any],
    source_name: str,
    endpoint_name: str,
    values: dict[str, Any],
    run_date: str,
    write_target: str,
    landing_root: str,
    quarantine_non_2xx: bool = False,
) -> tuple[str, ApiResponse]:
    """Fetch+validate+write one landing object, returning (written_path, response).

    Exists so callers that need to inspect the raw API response after writing it
    (e.g. pagination, which must know whether a page was "short" to decide whether
    to fetch the next one; or run_batch, which must know the HTTP status to avoid
    silently counting a non-2xx response as a successful write) don't have to
    fetch the same page twice. `ingest_one` below is a thin wrapper over this for
    callers that only need the path. Returning the full `ApiResponse` (not just
    its payload) lets callers see `status` and `size_bytes` without re-fetching.

    `quarantine_non_2xx`: when True and the response is not 2xx, the object is
    written under a `dt={run_date}__non_2xx/` sibling directory instead of the
    normal `dt={run_date}/` one, so it never lands inside the glob that
    `pipelines/raw/landing_reader.py` scans for good pages. Debug value is
    kept (the object is still written, still has its real status/payload, still
    gets recorded in the run manifest via the caller's `record_output`) -- it is
    just no longer a sibling file next to a 2xx page under the same directory
    Spark infers one schema for. This exists for `ingest_paged_reference`
    (multiple pages of the SAME endpoint/run_date can land in the same
    directory, so one non-2xx page can poison schema inference for every good
    page); it defaults to False so every other caller's on-disk layout is
    unchanged.
    """
    source_config = get_source_config(config=config, source_name=source_name)
    endpoint_config = get_endpoint_config(config=config, source_name=source_name, endpoint_name=endpoint_name)
    request_defaults = get_request_defaults(config=config)
    landing_defaults = get_landing_defaults(config=config)

    prepared_request = prepare_request(
        source_config=source_config,
        endpoint_config=endpoint_config,
        values=values,
    )

    response = fetch_with_retry(
        url=prepared_request.url,
        timeout_seconds=int(request_defaults.get("timeout_seconds", request_defaults.get("timeout", 30))),
        request_defaults=request_defaults,
        before_attempt=get_rate_limiter(source_name, source_config).wait,
    )

    partition_key = build_partition_key(endpoint_config=endpoint_config, values=values)

    safe_url = mask_secret_query_params(
        url=prepared_request.url,
        secret_param_names={source_config.get("auth", {}).get("param_name", "")},
    )

    written_path = _build_and_write_envelope(
        source_name=source_name,
        endpoint_name=endpoint_name,
        landing_defaults=landing_defaults,
        run_date=run_date,
        partition_key=partition_key,
        request_url=safe_url,
        request_params=prepared_request.safe_params,
        response=response,
        payload=response.payload,
        write_target=write_target,
        landing_root=landing_root,
        quarantine_non_2xx=quarantine_non_2xx,
    )

    return written_path, response


def _build_batch_records(batch: list[dict[str, Any]], response: ApiResponse) -> Any:
    """Turn OpenFIGI's positional per-job result list into a flat list of records, each
    carrying the `_job` that produced it (fix round 1 finding (c), controller ruling).

    OpenFIGI's `/mapping` response does not echo which job produced which result --
    the correspondence between `batch` (the request body) and `response.payload` (the
    response body) is POSITIONAL ONLY, so `zip(batch, response.payload, strict=True)`
    is what ties them back together; `strict=True` makes a length mismatch (which
    would otherwise silently misattribute one job's identifier to a neighboring
    result) raise instead of drifting quietly out of alignment.

    `contracts/raw/openfigi/raw_openfigi_mapping.schema.yaml` sources
    `input_id_type`/`input_id_value` from `_job.idType`/`_job.idValue` -- those two
    columns are load-bearing: they record WHICH identifier resolved each row (ISIN vs
    CUSIP vs TICKER), and those paths have measurably different reliability (a
    TICKER-resolved row can name a different company entirely).

    A job with matches (`{"data": [...]}`) becomes one record per matched FIGI. A job
    with no match (`{"warning": "No identifier found."}`) becomes exactly ONE record
    (still tagged with `_job`) instead of being dropped -- knowing which symbols
    failed to resolve is operationally required, not an edge case to discard.

    Only applies to a successful (2xx), list-shaped payload: a non-2xx response's
    payload is a single error object, not one result per job, so there is nothing to
    zip and the raw payload is returned unchanged.
    """
    if not (200 <= response.status < 300) or not isinstance(response.payload, list):
        return response.payload

    records: list[dict[str, Any]] = []
    for job, result in zip(batch, response.payload, strict=True):
        data = result.get("data") if isinstance(result, dict) else None
        if data:
            for record in data:
                records.append({**record, "_job": job})
        else:
            records.append({"_job": job, "warning": (result or {}).get("warning")})
    return records


def ingest_one_batch_with_payload(
    config: dict[str, Any],
    source_name: str,
    endpoint_name: str,
    batch: list[dict[str, Any]],
    batch_index: int,
    run_date: str,
    write_target: str,
    landing_root: str,
) -> tuple[str, ApiResponse]:
    """Fetch+validate+write one landing object for a `scope: batch` endpoint (OpenFIGI
    `/mapping`), returning (written_path, response) -- the POST/header-auth counterpart
    of `ingest_one_with_payload` above.

    `ingest_one_with_payload` can't be reused directly here: it always does a GET with
    query-param auth (`prepare_request`/`apply_query_param_auth`), while a batch request
    is a POST with a JSON array body and a header (`X-OPENFIGI-APIKEY`). There is also no
    per-request `values` template to render into the URL -- the whole batch's identity is
    the `batch_index`, which becomes `request_params` (for the envelope/manifest) and the
    `{batch_index}` in the endpoint's `landing.partition_key` template
    (`batch={batch_index}`).

    The envelope's `payload` is NOT the raw response: `_build_batch_records` zips it
    against `batch` first so every emitted record carries `_job` (see that function's
    docstring). `response` itself (status/size_bytes) is still returned untouched, so
    bandwidth accounting and non-2xx classification upstream are unaffected.
    """
    source_config = get_source_config(config=config, source_name=source_name)
    endpoint_config = get_endpoint_config(config=config, source_name=source_name, endpoint_name=endpoint_name)
    request_defaults = get_request_defaults(config=config)
    landing_defaults = get_landing_defaults(config=config)

    base_url = source_config["base_url"].rstrip("/")
    url = f"{base_url}{endpoint_config['path']}"

    response = fetch_with_retry(
        url=url,
        timeout_seconds=int(request_defaults.get("timeout_seconds", request_defaults.get("timeout", 30))),
        request_defaults=request_defaults,
        before_attempt=get_rate_limiter(source_name, source_config).wait,
        method=endpoint_config["method"],
        body=batch,
        headers=apply_header_auth(source_config.get("auth", {})),
    )

    values = {"batch_index": batch_index}
    partition_key = build_partition_key(endpoint_config=endpoint_config, values=values)
    payload = _build_batch_records(batch, response)

    written_path = _build_and_write_envelope(
        source_name=source_name,
        endpoint_name=endpoint_name,
        landing_defaults=landing_defaults,
        run_date=run_date,
        partition_key=partition_key,
        request_url=url,
        request_params={"batch_index": batch_index, "job_count": len(batch)},
        response=response,
        payload=payload,
        write_target=write_target,
        landing_root=landing_root,
    )

    return written_path, response


def ingest_one(
    config: dict[str, Any],
    source_name: str,
    endpoint_name: str,
    values: dict[str, Any],
    run_date: str,
    write_target: str,
    landing_root: str,
) -> str:
    written_path, _response = ingest_one_with_payload(
        config=config,
        source_name=source_name,
        endpoint_name=endpoint_name,
        values=values,
        run_date=run_date,
        write_target=write_target,
        landing_root=landing_root,
    )
    return written_path


def main() -> None:
    load_dotenv()

    args = parse_args()
    config = load_yaml(args.config)

    endpoint_config = get_endpoint_config(config=config, source_name=args.source, endpoint_name=args.endpoint)

    if not endpoint_config.get("enabled", True) and not args.include_disabled:
        raise ValueError(
            f"Endpoint {args.source}.{args.endpoint} is disabled. "
            "Use --include-disabled to run it explicitly"
        )

    run_date = args.run_date
    if not run_date:
        run_date = datetime.now(timezone.utc).date().isoformat()

    symbols = args.symbol or [None]

    written_paths = []

    if endpoint_config["scope"] == "per_symbol":
        runtime_inputs = args.symbol or [None]
        for symbol in runtime_inputs:
            runtime_values_list = build_runtime_values(endpoint_config=endpoint_config, symbol=symbol)

            for values in runtime_values_list:
                written_path = ingest_one(
                    config=config,
                    source_name=args.source,
                    endpoint_name=args.endpoint,
                    values=values,
                    run_date=run_date,
                    write_target=args.write_target,
                    landing_root=args.landing_root,
                )

                written_paths.append(written_path)
                print(f"wrote: {written_path}")
    
        print(f"completed: {len(written_paths)} object(s)")

    elif endpoint_config["scope"] == "macro_series":
        runtime_inputs = args.series_id or [None]
        for series_id in runtime_inputs:
            runtime_values_list = build_runtime_values(endpoint_config=endpoint_config, series_id=series_id)

            for values in runtime_values_list:
                written_path = ingest_one(
                    config=config,
                    source_name=args.source,
                    endpoint_name=args.endpoint,
                    values=values,
                    run_date=run_date,
                    write_target=args.write_target,
                    landing_root=args.landing_root,
                )

                written_paths.append(written_path)
                print(f"wrote: {written_path}")
    
        print(f"completed: {len(written_paths)} object(s)")

    else:
        runtime_values_list = build_runtime_values(endpoint_config=endpoint_config)

        for values in runtime_values_list:
            written_path = ingest_one(
                config=config,
                source_name=args.source,
                endpoint_name=args.endpoint,
                values=values,
                run_date=run_date,
                write_target=args.write_target,
                landing_root=args.landing_root,
            )

            written_paths.append(written_path)
            print(f"wrote: {written_path}")

        print(f"completed: {len(written_paths)} object(s)")

if __name__ == "__main__":
    main()