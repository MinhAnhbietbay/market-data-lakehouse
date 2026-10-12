import argparse
import os
from collections.abc import Iterator
from datetime import date, datetime, timezone
from typing import Any

from dotenv import load_dotenv

from pipelines.landing.bandwidth_ledger import (
    append_ledger_entry,
    ledger_total_bytes,
    metered_payload_bytes,
)
from pipelines.landing.config import get_endpoint_config, get_landing_defaults, load_yaml
from packages.common.run_manifest import RunManifest, resolve_manifest_path
from pipelines.landing.date_windows import (
    expand_windows,
    override_window_spec,
    validate_window_partition_keys,
)
from pipelines.landing.envelope import build_object_key, build_partition_key
from pipelines.landing.error_classifier import classify_error
from pipelines.landing.figi_jobs import batch_jobs, build_mapping_jobs, load_symbol_records
from pipelines.landing.http_client import ApiResponse
from pipelines.landing.ingest import build_runtime_values, ingest_one_batch_with_payload, ingest_one_with_payload
from pipelines.landing.object_store import object_exists
from pipelines.landing.row_cap import is_probably_truncated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run landing ingestion for multiple configured endpoints.")
    parser.add_argument("--config", default="configs/sources/data_sources.yaml")
    parser.add_argument("--universe-config", default="configs/universes/universe.yaml")
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--endpoint", action="append", default=[])
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--series-id", action="append", default=[])
    parser.add_argument("--use-runtime-set", default="fmp_macro_series")
    parser.add_argument("--run-date")
    parser.add_argument("--write-target", choices=["local", "minio", "both"], default="minio")
    parser.add_argument("--landing-root", default="/tmp/lakehouse-landing")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Bỏ qua (không gọi API) mọi object 2xx đã có cho run-date này -- chạy lại sau khi "
             "đứt mà không đốt băng thông lần hai. Object non-2xx nằm ở dt=...__non_2xx nên vẫn "
             "được gọi lại. Không áp cho paged_reference/batch (vài chục trang, kéo lại rẻ).",
    )
    parser.add_argument("--include-disabled", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--manifest-path")
    parser.add_argument("--manifest-root", default=os.getenv("RUN_MANIFEST_ROOT", "artifacts/run-manifests"))
    parser.add_argument(
        "--bandwidth-budget-bytes",
        type=int,
        default=None,
        help="Dừng run khi (sổ băng thông + byte payload compact của run này) vượt ngưỡng. FMP Premium: 50GB/30 ngày -- đặt ~45GB.",
    )
    parser.add_argument(
        "--bandwidth-ledger",
        default=None,
        help="File JSONL cộng dồn byte FMP tính qua mọi lần chạy. --bandwidth-budget-bytes so "
             "với (tổng sổ + lần chạy này), không chỉ lần chạy này.",
    )
    parser.add_argument(
        "--universe-tier",
        default=None,
        help="Chọn tier từ universe.yaml `tiers` (vd t1, t2). Mặc định dùng `symbols`.",
    )
    parser.add_argument(
        "--window-from",
        default=None,
        help="Backfill MỘT LẦN: thay cửa sổ `windows:` của endpoint bằng khoảng tuyệt đối "
             "này. Chỉ áp cho endpoint đã khai `windows`. Phải đi kèm --window-to.",
    )
    parser.add_argument(
        "--window-to",
        default=None,
        help="Đầu trên của khoảng backfill (xem --window-from).",
    )
    parser.add_argument(
        "--window-split-every",
        default=None,
        help="Chia khoảng backfill thành nhiều request, vd '89d'. FMP cắt ở 90 ngày "
             "lịch/request và cắt IM LẶNG, nên khoảng dài hơn thế BẮT BUỘC phải chia. "
             "Dấu trừ là tuỳ chọn -- '-89d' phải viết dạng --window-split-every=-89d vì "
             "argparse hiểu '-89d' là một option.",
    )
    parser.add_argument(
        "--identifiers-file",
        default=None,
        help="JSON [{symbol, isin, cusip, exchange_code}] để job OpenFIGI dùng nhánh "
             "ISIN thay vì TICKER. Thiếu file thì tất cả rơi xuống TICKER.",
    )
    return parser.parse_args()


def selected_sources(config: dict[str, Any], source_names: list[str]) -> list[str]:
    configured_sources = config.get("sources", {})
    if source_names:
        missing = sorted(set(source_names) - set(configured_sources))
        if missing:
            raise ValueError(f"unknown source(s): {', '.join(missing)}")
        return source_names
    return list(configured_sources.keys())


def selected_endpoints(source_config: dict[str, Any], endpoint_names: list[str]) -> list[str]:
    configured_endpoints = source_config.get("endpoints", {})
    if endpoint_names:
        return [name for name in endpoint_names if name in configured_endpoints]
    return list(configured_endpoints.keys())


def build_selection(
    config: dict[str, Any],
    source_names: list[str],
    endpoint_names: list[str],
) -> list[tuple[str, list[str]]]:
    selection = [
        (source_name, selected_endpoints(config["sources"][source_name], endpoint_names))
        for source_name in selected_sources(config, source_names)
    ]
    if endpoint_names:
        matched = {endpoint for _, endpoints in selection for endpoint in endpoints}
        missing = sorted(set(endpoint_names) - matched)
        if missing:
            raise ValueError(f"unknown endpoint(s): {', '.join(missing)}")
    return [(source_name, endpoints) for source_name, endpoints in selection if endpoints]


def runtime_values_for_endpoint(
    config: dict[str, Any],
    endpoint_config: dict[str, Any],
    symbols: list[str],
    series_ids: list[str],
    runtime_set: str,
    universe_config: dict[str, Any] | None = None,
    tier: str | None = None,
    today: date | None = None,
) -> list[dict[str, Any]]:
    scope = endpoint_config["scope"]

    if scope == "per_symbol":
        universe = universe_config or {}
        # A tier lookup that misses (unknown tier name, or a universe.yaml with no
        # `tiers` key at all) falls through to `[]`, not an exception here -- the
        # existing "no symbols" ValueError below is the single place that error
        # surfaces, so callers only need to handle one failure mode.
        if tier:
            selected_symbols = symbols or universe.get("tiers", {}).get(tier, [])
        else:
            selected_symbols = symbols or universe.get("symbols", [])
        if not selected_symbols:
            raise ValueError(
                "per_symbol endpoint requires at least one --symbol or a populated "
                "universe.yaml symbols list"
            )
        base_values = [
            values for symbol in selected_symbols for values in build_runtime_values(endpoint_config, symbol=symbol)
        ]
    elif scope == "macro_series":
        selected_series = series_ids or config.get("runtime_sets", {}).get(runtime_set, [])
        if not selected_series:
            raise ValueError("macro_series endpoint requires --series-id or a configured runtime set")
        base_values = [
            values for series_id in selected_series for values in build_runtime_values(endpoint_config, series_id=series_id)
        ]
    elif scope == "per_group":
        # Tên runtime set lấy từ ENDPOINT, không từ --runtime-set toàn cục: 4 endpoint A10
        # chia 2 danh sách khác nhau (sectors vs industries) và chạy cùng một lần run_batch.
        set_name = endpoint_config.get("runtime_set") or runtime_set
        selected_groups = config.get("runtime_sets", {}).get(set_name) or []
        if not selected_groups:
            raise ValueError(
                f"per_group endpoint requires a populated runtime set (looked for {set_name!r})"
            )
        base_values = [
            values
            for group in selected_groups
            for values in build_runtime_values(endpoint_config, group=group)
        ]
    else:
        base_values = build_runtime_values(endpoint_config)

    # Cửa sổ ngày là chiều fanout ĐỘC LẬP với scope: một symbol x 3 cửa sổ = 3 request.
    # Nhân ở đây (chứ không trong từng nhánh scope) để mọi scope -- per_symbol, per_group,
    # market_wide -- đều dùng được cùng một khai báo `windows:` mà không lặp code.
    windows = expand_windows(endpoint_config.get("windows"), today=today or date.today())
    if not windows:
        return base_values
    return [{**values, **window} for values in base_values for window in windows]


def ingest_paged_reference(
    config: dict[str, Any],
    source_name: str,
    endpoint_name: str,
    endpoint_config: dict[str, Any],
    run_date: str,
    write_target: str,
    landing_root: str,
) -> Iterator[tuple[dict[str, Any], str, ApiResponse]]:
    """Fetch+write pages for a `paged_reference` endpoint until a short/empty page.

    A generator, not a list-returning function: each page is yielded immediately
    after being fetched and written, so a caller iterating this in a `for` loop
    (see `main()`) can record it (e.g. to the run manifest) right away — pages
    already written before a later page raises are not lost.

    The `page` value driving each request comes from this loop's own counter,
    starting at 0 — NOT from `endpoint_config["params"]["page"]`, which is a
    template placeholder (e.g. `"{page}"`) filled in per-request by
    `prepare_request`/`render_mapping`, not a literal default. Each page is
    fetched and written before the *next* page's request is even built, so no
    page is ever fetched twice. A page is considered the last one when its
    payload has fewer items than `params.limit` (or `limit` isn't configured,
    in which case a single page is fetched), or when the response is non-2xx
    (a non-2xx payload isn't a valid page body, so counting/paginating past it
    would either double-count garbage or spin forever on an error page).
    """
    params = endpoint_config.get("params", {})
    limit = params.get("limit")
    page = 0

    while True:
        values = {"page": page}
        written_path, response = ingest_one_with_payload(
            config=config,
            source_name=source_name,
            endpoint_name=endpoint_name,
            values=values,
            run_date=run_date,
            write_target=write_target,
            landing_root=landing_root,
            # Pages of the SAME endpoint/run_date land as sibling files in one
            # directory (page-0.json, page-1.json, ...). A non-2xx page's payload
            # shape (error object) differs from a 2xx page's (array), which breaks
            # Spark schema inference for the whole directory downstream in raw
            # -- see pipelines/raw/landing_reader.py. Quarantining non-2xx pages
            # to a sibling dt={run_date}__non_2xx/ directory keeps them out of that
            # glob while still writing+recording them (debug value preserved).
            quarantine_non_2xx=True,
        )
        yield values, written_path, response

        payload = response.payload
        page_size = len(payload) if isinstance(payload, list) else 0
        # A non-2xx page has no valid payload to paginate from -- stop rather than
        # fetching further pages into what would just be more error responses.
        if response.status != 200 or limit is None or page_size < limit:
            return
        page += 1


def ingest_batch(
    config: dict[str, Any],
    source_name: str,
    endpoint_name: str,
    endpoint_config: dict[str, Any],
    universe_config: dict[str, Any],
    identifiers_path: str | None,
    run_date: str,
    write_target: str,
    landing_root: str,
) -> Iterator[tuple[dict[str, Any], str, ApiResponse]]:
    """Fetch+write one landing envelope per OpenFIGI `/mapping` batch (`scope: batch`).

    Mirrors `ingest_paged_reference`'s generator shape for the same reason: each batch
    is fetched and written immediately and yielded right away, so a caller iterating
    this in a `for` loop (see `main()`) can record it to the run manifest as it goes --
    a batch already written before a later batch raises is not lost.

    Unlike `per_symbol`, this scope has no `--symbol` override: the whole universe fans
    out into batches of up to `endpoint_config["batch_size"]` mapping jobs (100 per
    OpenFIGI's authenticated limit), built via `load_symbol_records` +
    `build_mapping_jobs` + `batch_jobs`. `identifiers_path` is `--identifiers-file`,
    threaded through unchanged so a run with a real identifiers file upgrades symbols
    from the TICKER resolution path to the (unambiguous) ISIN path.
    """
    records = load_symbol_records(universe_config, identifiers_path)
    batches = batch_jobs(build_mapping_jobs(records), endpoint_config["batch_size"])

    for batch_index, batch in enumerate(batches):
        written_path, response = ingest_one_batch_with_payload(
            config=config,
            source_name=source_name,
            endpoint_name=endpoint_name,
            batch=batch,
            batch_index=batch_index,
            run_date=run_date,
            write_target=write_target,
            landing_root=landing_root,
        )
        yield {"batch_index": batch_index}, written_path, response


def record_output(
    manifest: RunManifest,
    source_name: str,
    endpoint_name: str,
    endpoint_config: dict[str, Any],
    values: dict[str, Any],
    written_path: str,
    response: ApiResponse,
) -> tuple[bool, str | None]:
    """Ghi 1 landing object vào manifest, phân biệt 2xx / non-2xx / truncated. Trả (True, None)
    nếu là 2xx và không bị cắt, ngược lại (False, error_class) với error_class từ classify_error
    hoặc None nếu là truncated.

    Landing object vẫn được ghi ra storage kể cả khi non-2xx hay truncated (payload lỗi
    hữu ích để debug; 5000 dòng bị cắt vẫn là dữ liệu thật, dùng được sau khi ghép với
    cửa sổ còn lại) -- nhưng KHÔNG được đếm vào written_objects: raw lọc
    response_status == 200 nên một run toàn 404 sẽ tạo bảng rỗng mà manifest vẫn báo
    thành công -- đúng cách dead endpoint /analyst-stock-recommendations lọt qua cả
    branch mà không ai thấy. Cùng nguyên tắc áp cho truncated: một response 200 OK dài
    đúng bằng `row_cap` của endpoint (spec A1, verify LIVE -- FMP không báo lỗi khi cắt)
    trông y hệt một page hoàn chỉnh trừ khi ta đếm dòng và so với cap khai trong config.
    """
    is_ok = 200 <= response.status < 300
    row_cap = endpoint_config.get("row_cap")
    truncated = is_ok and is_probably_truncated(response.payload, row_cap)
    if is_ok:
        status = "truncated" if truncated else "written"
    else:
        status = "non_2xx"
    manifest.add_output(
        source=source_name,
        endpoint=endpoint_name,
        values=values,
        path=written_path,
        status=status,
        response_status=response.status,
        response_bytes=response.size_bytes,
    )
    error_class = None
    if not is_ok:
        error_class = classify_error(response.status, response.payload)
        manifest.add_failure(
            source=source_name,
            endpoint=endpoint_name,
            error=f"HTTP {response.status}",
            error_class=error_class,
            values=values,
        )
    elif truncated:
        manifest.add_failure(
            source=source_name,
            endpoint=endpoint_name,
            error=f"payload hit the {row_cap}-row cap; split the date window",
            values=values,
        )
    return is_ok and not truncated, error_class


def enforce_bandwidth_budget(
    manifest: RunManifest,
    response: ApiResponse,
    bandwidth_budget_bytes: int | None,
    manifest_path: str | None,
    manifest_root: str,
) -> None:
    """Cộng `response.size_bytes` vào tổng, dừng sạch nếu vượt `bandwidth_budget_bytes`.

    Cộng dồn bất kể response 2xx hay non-2xx: FMP tính byte JSON compact của payload
    (xem bandwidth_ledger.py), không theo status code, nên một chuỗi non-2xx (endpoint
    chết trả JSON lỗi) vẫn tiêu ngân sách y như response thành công. Gọi sau MỖI `record_output` --
    trần cứng 50GB/30 ngày của FMP Premium không chờ hết cả đợt pull mới chặn, nên
    bộ đếm này cũng không được chờ.

    `raise SystemExit(2)` ở đây cố ý dùng exit code khác với exit code 1 của "có
    endpoint fail" (cuối `main()`), để runbook/Airflow phân biệt được "hết băng
    thông" (cần dừng cả lịch, chờ cửa sổ 30 ngày trôi) với "một vài endpoint lỗi"
    (có thể retry ngay). `SystemExit` không bị `except Exception` ở call site trong
    `main()` nuốt mất, nên dừng sạch thay vì bị coi là lỗi endpoint bình thường.
    """
    manifest.metrics["response_bytes_total"] = (
        manifest.metrics.get("response_bytes_total", 0) + response.size_bytes
    )
    # status 0 = lỗi vận chuyển, payload do fetch_json tự dựng -- FMP không gửi gì, không tính.
    metered = 0 if response.status == 0 else metered_payload_bytes(response.payload)
    manifest.metrics["metered_bytes_total"] = manifest.metrics.get("metered_bytes_total", 0) + metered
    manifest.metrics["requests_total"] = manifest.metrics.get("requests_total", 0) + 1
    if bandwidth_budget_bytes is None:
        return
    spent = manifest.metrics.get("ledger_baseline_bytes", 0) + manifest.metrics["metered_bytes_total"]
    if spent <= bandwidth_budget_bytes:
        return
    manifest.metrics["bandwidth_budget_exceeded"] = True
    print(f"bandwidth budget exceeded: {spent} > {bandwidth_budget_bytes} bytes (ledger + this run) -- stopping run")
    finalize_manifest(manifest, manifest_path, manifest_root)
    raise SystemExit(2)


def handle_quota_exhausted(
    manifest: RunManifest,
    source_name: str,
    endpoint_name: str,
    manifest_path: str | None,
    manifest_root: str,
) -> None:
    """Print, finalize the manifest, and raise SystemExit(3) -- shared by both the
    `paged_reference` and per-symbol/market_wide call sites in `main()` so a
    `quota_exhausted` classification stops the whole run identically from either.
    """
    print(
        f"quota exhausted on {source_name}.{endpoint_name} -- stopping run "
        "(FMP daily call budget hit)"
    )
    finalize_manifest(manifest, manifest_path, manifest_root)
    raise SystemExit(3)


def finalize_manifest(manifest: RunManifest, manifest_path: str | None, manifest_root: str) -> None:
    manifest.metrics["written_objects"] = sum(1 for output in manifest.outputs if output["status"] == "written")
    manifest.metrics["non_2xx_objects"] = sum(1 for output in manifest.outputs if output["status"] == "non_2xx")
    manifest.metrics["truncated_objects"] = sum(
        1 for output in manifest.outputs if output["status"] == "truncated"
    )
    manifest.metrics["existing_objects"] = sum(1 for output in manifest.outputs if output["status"] == "exists")
    manifest.metrics["skipped_endpoints"] = sum(1 for output in manifest.outputs if output["status"] == "skipped")
    # Giá trị trong một fan-out chưa từng được gọi vì endpoint khoá giữa chừng -- tách
    # khỏi `skipped_endpoints` (bỏ qua cả endpoint) vì hai thứ khác cấp.
    manifest.metrics["not_attempted_values"] = sum(
        1 for output in manifest.outputs if output["status"] == "not_attempted"
    )
    manifest.metrics["failed_endpoints"] = len(manifest.failures)
    ledger_path = manifest.inputs.get("bandwidth_ledger")
    if ledger_path:
        # Ghi ở MỌI lối ra (xong, hết băng thông, hết quota, exception) vì cả 4 đều đi qua đây.
        append_ledger_entry(ledger_path, {
            "run_id": manifest.run_id,
            "run_date": manifest.run_date,
            "sources": manifest.inputs.get("sources"),
            "endpoints": manifest.inputs.get("endpoints"),
            "universe_tier": manifest.inputs.get("universe_tier"),
            "metered_bytes": manifest.metrics.get("metered_bytes_total", 0),
            "wire_bytes": manifest.metrics.get("response_bytes_total", 0),
            "requests": manifest.metrics.get("requests_total", 0),
        })
    manifest.finish()
    written_path = manifest.write(resolve_manifest_path(manifest, manifest_path, manifest_root))
    print(f"manifest: {written_path}")


def main() -> None:
    load_dotenv()
    args = parse_args()
    config = load_yaml(args.config)
    validate_window_partition_keys(config)
    run_date = args.run_date or datetime.now(timezone.utc).date().isoformat()
    manifest = RunManifest(
        run_type="landing_batch",
        run_date=run_date,
        inputs={
            "config": args.config,
            "sources": args.source,
            "endpoints": args.endpoint,
            "symbols": args.symbol,
            "series_ids": args.series_id,
            "write_target": args.write_target,
            "universe_tier": args.universe_tier,
            "bandwidth_ledger": args.bandwidth_ledger,
            "skip_existing": args.skip_existing,
            # Cửa sổ backfill nằm trong inputs, không chỉ suy ra từ tên object: manifest
            # là bằng chứng tái lập độc lập của một run (docs/run_manifests.md), và một
            # đợt backfill 42 cửa sổ vs một run thường 1 cửa sổ phải phân biệt được từ
            # chính manifest thay vì phải đi đọc lại đường dẫn từng object.
            "window_override": (
                None
                if args.window_from is None and args.window_to is None
                else {
                    "from": args.window_from,
                    "to": args.window_to,
                    "split_every": args.window_split_every,
                }
            ),
        },
    )
    manifest.metrics["response_bytes_total"] = 0
    manifest.metrics["bandwidth_budget_exceeded"] = False
    manifest.metrics["metered_bytes_total"] = 0
    manifest.metrics["requests_total"] = 0
    manifest.metrics["ledger_baseline_bytes"] = ledger_total_bytes(args.bandwidth_ledger)
    try:
        selection = build_selection(config, args.source, args.endpoint)
    except ValueError as exc:
        manifest.add_failure(stage="selection", error=str(exc))
        finalize_manifest(manifest, args.manifest_path, args.manifest_root)
        raise SystemExit(f"error: {exc}") from None

    written_paths: list[str] = []
    failures: list[tuple[str, str, str]] = []
    # Loaded lazily (and cached) the first time a per_symbol endpoint actually needs the
    # fallback, i.e. --symbol was not passed. This way a missing/malformed universe.yaml
    # only fails the endpoint(s) that need it -- via the same per-endpoint try/except
    # below that already records manifest failures -- instead of crashing every
    # run_batch.py invocation up front, even ones that never touch a per_symbol endpoint.
    universe_config: dict[str, Any] | None = None
    for source_name, endpoint_names in selection:
        for endpoint_name in endpoint_names:
            endpoint_config = get_endpoint_config(config, source_name, endpoint_name)
            endpoint_config = override_window_spec(
                endpoint_config,
                window_from=args.window_from,
                window_to=args.window_to,
                split_every=args.window_split_every,
            )

            if not endpoint_config.get("enabled", True) and not args.include_disabled:
                manifest.add_output(source=source_name, endpoint=endpoint_name, status="skipped", reason="disabled")
                print(f"skipped disabled endpoint: {source_name}.{endpoint_name}")
                continue

            # Endpoints WITHOUT a `tiers` key run at every tier -- this is the
            # backward-compatibility path for `market_wide`/`macro_series` endpoints
            # (and any per_symbol endpoint not yet migrated to declare `tiers`), which
            # were never tier-scoped and must keep running regardless of
            # `--universe-tier`.
            endpoint_tiers = endpoint_config.get("tiers")
            if args.universe_tier and endpoint_tiers and args.universe_tier not in endpoint_tiers:
                manifest.add_output(
                    source=source_name,
                    endpoint=endpoint_name,
                    status="skipped",
                    reason=f"not in tier {args.universe_tier}",
                )
                print(f"skipped endpoint not in tier {args.universe_tier}: {source_name}.{endpoint_name}")
                continue

            try:
                if endpoint_config["scope"] == "paged_reference":
                    page_results = ingest_paged_reference(
                        config=config,
                        source_name=source_name,
                        endpoint_name=endpoint_name,
                        endpoint_config=endpoint_config,
                        run_date=run_date,
                        write_target=args.write_target,
                        landing_root=args.landing_root,
                    )
                    for values, written_path, response in page_results:
                        written_paths.append(written_path)
                        _, error_class = record_output(
                            manifest, source_name, endpoint_name, endpoint_config, values, written_path, response
                        )
                        if error_class == "quota_exhausted":
                            handle_quota_exhausted(
                                manifest, source_name, endpoint_name, args.manifest_path, args.manifest_root
                            )
                        # No endpoint_locked handling needed here: ingest_paged_reference
                        # already stops its own page loop on any non-200 response (see its
                        # docstring), so a locked paged_reference endpoint already costs
                        # exactly 1 call -- there is no "remaining pages" to skip.
                        print(f"wrote: {written_path}")
                        enforce_bandwidth_budget(
                            manifest, response, args.bandwidth_budget_bytes, args.manifest_path, args.manifest_root
                        )
                    continue

                if endpoint_config["scope"] == "batch":
                    # No --symbol override for this scope (unlike per_symbol below) --
                    # a batch endpoint always fans out over the whole universe, so the
                    # lazy-load only needs to guard against loading it twice.
                    if universe_config is None:
                        universe_config = load_yaml(args.universe_config)
                    batch_results = ingest_batch(
                        config=config,
                        source_name=source_name,
                        endpoint_name=endpoint_name,
                        endpoint_config=endpoint_config,
                        universe_config=universe_config,
                        identifiers_path=args.identifiers_file,
                        run_date=run_date,
                        write_target=args.write_target,
                        landing_root=args.landing_root,
                    )
                    for values, written_path, response in batch_results:
                        written_paths.append(written_path)
                        _, error_class = record_output(
                            manifest, source_name, endpoint_name, endpoint_config, values, written_path, response
                        )
                        if error_class == "quota_exhausted":
                            handle_quota_exhausted(
                                manifest, source_name, endpoint_name, args.manifest_path, args.manifest_root
                            )
                        print(f"wrote: {written_path}")
                        enforce_bandwidth_budget(
                            manifest, response, args.bandwidth_budget_bytes, args.manifest_path, args.manifest_root
                        )
                    continue

                if endpoint_config["scope"] == "per_symbol" and not args.symbol and universe_config is None:
                    universe_config = load_yaml(args.universe_config)

                values_list = runtime_values_for_endpoint(
                    config=config,
                    endpoint_config=endpoint_config,
                    symbols=args.symbol,
                    series_ids=args.series_id,
                    runtime_set=args.use_runtime_set,
                    universe_config=universe_config,
                    tier=args.universe_tier,
                    today=date.fromisoformat(run_date),
                )

                for index, values in enumerate(values_list):
                    if args.skip_existing:
                        existing_key = build_object_key(
                            landing_defaults=get_landing_defaults(config),
                            source_name=source_name,
                            endpoint_name=endpoint_name,
                            run_date=run_date,
                            partition_key=build_partition_key(endpoint_config, values),
                        )
                        if object_exists(
                            args.write_target, args.landing_root,
                            get_landing_defaults(config)["bucket"], existing_key,
                        ):
                            manifest.add_output(
                                source=source_name, endpoint=endpoint_name, values=values,
                                path=existing_key, status="exists",
                            )
                            continue

                    written_path, response = ingest_one_with_payload(
                        config=config,
                        source_name=source_name,
                        endpoint_name=endpoint_name,
                        values=values,
                        run_date=run_date,
                        write_target=args.write_target,
                        landing_root=args.landing_root,
                        quarantine_non_2xx=True,
                    )
                    written_paths.append(written_path)
                    _, error_class = record_output(
                        manifest, source_name, endpoint_name, endpoint_config, values, written_path, response
                    )
                    if error_class == "quota_exhausted":
                        handle_quota_exhausted(
                            manifest, source_name, endpoint_name, args.manifest_path, args.manifest_root
                        )
                    if error_class == "endpoint_locked":
                        # `break` giữ nguyên (tiết kiệm quota -- endpoint khoá thì mọi
                        # symbol còn lại cũng khoá). Nhưng phải GHI LẠI phần chưa thử:
                        # nếu không, chúng không có output nào trong manifest, và
                        # "chưa gọi" trông y hệt "gọi rồi, không có gì" -- đúng cái
                        # nhầm lẫn khiến một run thủng đọc như một run đầy đủ.
                        # Status riêng "not_attempted", KHÔNG dùng lại "skipped": metric
                        # `skipped_endpoints` và test_universe_tier.py:173 đều lọc theo
                        # "skipped" với nghĩa "endpoint bị bỏ qua cả cụm" (disabled /
                        # ngoài tier). Đây là chuyện khác cấp -- vài giá trị trong một
                        # fan-out -- nên đếm riêng.
                        skipped_values = values_list[index + 1 :]
                        for skipped in skipped_values:
                            manifest.add_output(
                                source=source_name,
                                endpoint=endpoint_name,
                                values=skipped,
                                status="not_attempted",
                                reason="endpoint_locked earlier in this fan-out",
                            )
                        print(
                            f"endpoint locked on {source_name}.{endpoint_name} -- skipping "
                            f"{len(skipped_values)} remaining value(s) "
                            "(not available on current FMP plan)"
                        )
                        break
                    print(f"wrote: {written_path}")
                    enforce_bandwidth_budget(
                        manifest, response, args.bandwidth_budget_bytes, args.manifest_path, args.manifest_root
                    )
            except Exception as exc:
                failures.append((source_name, endpoint_name, str(exc)))
                manifest.add_failure(source=source_name, endpoint=endpoint_name, error=str(exc))
                print(f"failed: {source_name}.{endpoint_name}: {exc}")
                if not args.continue_on_error:
                    finalize_manifest(manifest, args.manifest_path, args.manifest_root)
                    raise

    finalize_manifest(manifest, args.manifest_path, args.manifest_root)
    # `failures` (list cục bộ) CHỈ chứa endpoint ném exception. Mọi non-2xx lẻ của một
    # symbol/series đã được `record_output` ghi vào `manifest.failures` từ trước, nên
    # `manifest.status` là "failed" trong khi dòng tổng kết cũ vẫn in "failed: 0
    # endpoint(s)" -- báo cáo sai một run thủng thành một run sạch. In cả hai con số,
    # tách bạch: exception cấp endpoint vs object lẻ không về được.
    print(
        f"completed: {len(written_paths)} object(s), "
        f"existing: {manifest.metrics.get('existing_objects', 0)} object(s), "
        f"failed: {len(failures)} endpoint(s), "
        f"non-2xx: {manifest.metrics.get('non_2xx_objects', 0)} object(s), "
        f"truncated: {manifest.metrics.get('truncated_objects', 0)} object(s), "
        f"skipped endpoints: {manifest.metrics.get('skipped_endpoints', 0)}, "
        f"not attempted: {manifest.metrics.get('not_attempted_values', 0)} value(s), "
        f"manifest status: {manifest.status}"
    )
    if failures:
        for source_name, endpoint_name, error in failures:
            print(f"failure: {source_name}.{endpoint_name}: {error}")
        raise SystemExit(1)
    # Exit code cố ý VẪN là 0 khi chỉ có non-2xx lẻ: gói FMP hiện tại làm 402 thành
    # chuyện thường ngày (F16-B), mà `landing_* >> staging_*` trong factory.py dùng
    # trigger rule mặc định `all_success` -- exit 1 ở đây sẽ chặn raw mỗi ngày và
    # đổi "thiếu vài endpoint" thành "không có dữ liệu nào". Ai muốn chặt hơn thì đọc
    # `manifest.status`/`non_2xx_objects` từ manifest, đừng đổi exit code ở đây.
    if manifest.failures:
        print(
            f"partial run: {len(manifest.failures)} object-level failure(s) recorded in "
            "the manifest -- exit code stays 0, see manifest for detail"
        )


if __name__ == "__main__":
    main()
