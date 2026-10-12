"""Dựng universe NYSE+NASDAQ từng tồn tại trong 10 năm, theo 3 bước (spec 2026-09-30 §2).

1. `--stage candidates`: đọc landing của company_screener_{nasdaq,nyse} (T1, đang niêm yết) và
   delisted_companies (T2), lọc sàn + ngày huỷ niêm yết + loại chứng khoán không phải cổ phiếu
   thường theo tên. Ghi candidates.json + candidates_universe.yaml.
2. Người vận hành kéo `profile` cho mọi ứng viên bằng run_batch (--universe-config
   candidates_universe.yaml).
3. `--stage final`: loại ADR / quỹ / ETF / SPAC theo profile, ghi universe.yaml + report.json.

Vì sao survivorship: bỏ mã delisted là thiên lệch không sửa được ở tầng model -- hết Premium thì
dữ liệu của chúng mất vĩnh viễn. Mã có ở cả hai danh sách thuộc T1 (bộ endpoint T1 là siêu tập).
Giới hạn đã biết: ticker bị tái sử dụng chỉ còn bản T1 (khoá theo ticker).
"""
import argparse
import json
import re
from pathlib import Path
from typing import Any

import yaml

from pipelines.landing.config import get_landing_defaults, load_yaml
from pipelines.landing.envelope import build_object_key
from pipelines.landing.object_store import make_s3_client

SCREENER_ENDPOINTS = ("company_screener_nasdaq", "company_screener_nyse")

# Thứ tự có nghĩa: từ khoá trước, "%" sau cùng -- "5.25% Senior Notes" là note, không phải
# preferred; "Depositary Shares ... Preferred" là preferred trước khi là depositary.
_NON_COMMON_PATTERNS = [
    ("preferred", re.compile(r"preferred|\bpfd\b", re.I)),
    ("warrant", re.compile(r"\bwarrants?\b", re.I)),
    # unit/right/note chỉ khớp ở CUỐI tên (hoặc "Notes due ..."): "Unit Corporation", "Bond Street
    # Holdings", "Note AB" là cổ phiếu thường (agy review 2026-09-30). Loại ở bước này là loại hẳn,
    # không qua kiểm profile, nên thà lọt còn hơn loại oan.
    ("unit", re.compile(r"\bunits?\s*$", re.I)),
    ("right", re.compile(r"\brights?\s*$", re.I)),
    ("note", re.compile(r"\bnotes?\s*$|\bnotes?\s+due\b|\bdebentures?\b|\bbonds?\s+due\b", re.I)),
    ("depositary", re.compile(r"\bdepositary\b|\bADRs?\b|\bADS\b", re.I)),
    ("preferred", re.compile(r"\d+(\.\d+)?\s*%", re.I)),
]
_SPAC_NAME = re.compile(r"\b(acquisition|merger)\s+(corp|corporation|co|company|inc|ltd|limited)\b", re.I)
# Chỉ dùng khi KHÔNG có profile (mã delisted): delisted-companies không có cờ isEtf/isFund.
# Không đưa "Trust" vào -- Northern Trust, Wilmington Trust là cổ phiếu thường.
_FUND_NAME = re.compile(r"\bETF\b|\bETN\b|\bfund\b|\biShares\b|\bProShares\b|\bDirexion\b", re.I)


def non_common_reason(name: str | None) -> str | None:
    for reason, pattern in _NON_COMMON_PATTERNS:
        if name and pattern.search(name):
            return reason
    return None


def load_landed_payloads(
    write_target: str,
    landing_root: str,
    landing_defaults: dict[str, Any],
    source_name: str,
    endpoint_name: str,
    run_date: str,
) -> dict[str, Any]:
    """Map partition_key -> payload cho mọi object 2xx của một endpoint/run_date.

    Chỉ quét `dt={run_date}/`: object non-2xx nằm ở `dt={run_date}__non_2xx/` nên tự động bị
    loại. Đọc được cả đĩa cục bộ lẫn MinIO -- bản cũ chỉ glob đĩa cục bộ trong khi landing ghi
    lên MinIO, nên luôn thấy rỗng.
    """
    prefix = build_object_key(landing_defaults, source_name, endpoint_name, run_date, "")
    prefix = prefix[: prefix.rindex("/") + 1]
    found: dict[str, Any] = {}
    if write_target == "local":
        for path in sorted((Path(landing_root) / prefix).glob("*.json")):
            found[path.stem] = json.loads(path.read_text(encoding="utf-8"))["payload"]
        return found
    client = make_s3_client()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing_defaults["bucket"], Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json") or "/" in key[len(prefix):]:
                continue
            body = client.get_object(Bucket=landing_defaults["bucket"], Key=key)["Body"].read()
            found[key[len(prefix):-len(".json")]] = json.loads(body)["payload"]
    return found


def _rows(pages: list[Any]) -> list[dict[str, Any]]:
    return [row for page in pages if isinstance(page, list) for row in page if isinstance(row, dict)]


def build_candidates(
    screener_pages: list[Any],
    delisted_pages: list[Any],
    exchanges: set[str],
    delisted_since: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    candidates: dict[str, dict[str, Any]] = {}
    excluded: dict[str, str] = {}

    def consider(symbol: str | None, name: str | None, exchange: str | None, tier: str, **extra: Any) -> None:
        if not symbol:
            return
        if exchange not in exchanges:
            excluded.setdefault(symbol, "exchange")
            return
        reason = non_common_reason(name)
        if reason:
            excluded.setdefault(symbol, reason)
            return
        excluded.pop(symbol, None)
        candidates[symbol] = {"tier": tier, "exchange": exchange, "name": name, **extra}

    for row in _rows(screener_pages):
        consider(row.get("symbol"), row.get("companyName"), row.get("exchangeShortName"), "t1",
                 market_cap=row.get("marketCap"), delisted_date=None)
    for row in _rows(delisted_pages):
        symbol = row.get("symbol")
        if symbol in candidates:          # có ở cả hai danh sách -> T1
            continue
        if (row.get("delistedDate") or "") < delisted_since:
            if row.get("exchange") in exchanges:
                excluded.setdefault(symbol, "delisted_before_window")
            else:
                excluded.setdefault(symbol, "exchange")
            continue
        consider(symbol, row.get("companyName"), row.get("exchange"), "t2",
                 market_cap=None, delisted_date=row.get("delistedDate"))
    return candidates, excluded


def apply_profile_filter(
    candidates: dict[str, dict[str, Any]],
    profiles: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    kept: dict[str, dict[str, Any]] = {}
    excluded: dict[str, str] = {}
    for symbol, candidate in candidates.items():
        payload = profiles.get(symbol)
        profile = payload[0] if isinstance(payload, list) and payload else payload
        if not isinstance(profile, dict) or "symbol" not in profile:
            # FMP có thể không còn profile cho mã đã chết, hoặc trả 200 kèm dict lỗi
            # ({"Error Message": ...}) -- cả hai đều là "không có profile". Không có cờ để loại
            # ADR/quỹ, chỉ còn tên; còn lại giữ và đánh dấu để report liệt kê.
            name = candidate.get("name") or ""
            if _SPAC_NAME.search(name):
                excluded[symbol] = "spac_by_name"
            elif _FUND_NAME.search(name):
                excluded[symbol] = "fund_by_name"
            else:
                kept[symbol] = {**candidate, "note": "no_profile"}
            continue
        if profile.get("isEtf") or profile.get("isFund"):
            excluded[symbol] = "fund_or_etf"
        elif profile.get("isAdr"):
            excluded[symbol] = "adr"
        elif (profile.get("industry") or "").strip().lower() == "shell companies":
            excluded[symbol] = "spac"
        else:
            kept[symbol] = candidate
    return kept, excluded


def write_universe_config(
    universe_path: str | Path,
    tiers: dict[str, list[str]],
    market: str,
    benchmark: str,
    start_date: str,
) -> None:
    """`symbols` = T1: run_batch dùng `symbols` khi không truyền --universe-tier (DAG hằng ngày)."""
    path = Path(universe_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"market": market, "benchmark": benchmark, "start_date": start_date,
            "tiers": {"t1": tiers["t1"], "t2": tiers["t2"]}, "symbols": tiers["t1"]}
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dựng universe NYSE+NASDAQ (spec 2026-09-30 §2).")
    parser.add_argument("--stage", choices=["candidates", "final"], required=True)
    parser.add_argument("--config", default="configs/sources/data_sources.yaml")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--write-target", choices=["local", "minio"], default="local")
    parser.add_argument("--landing-root", default="/tmp/lakehouse-landing")
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--universe-config", default="configs/universes/universe.yaml")
    parser.add_argument("--delisted-since", default="2016-10-01")
    parser.add_argument("--min-t1", type=int, default=1000)
    parser.add_argument("--max-t1-no-profile-ratio", type=float, default=0.05)
    parser.add_argument("--market", default="US")
    parser.add_argument("--benchmark", default="SPY")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_yaml(args.config)
    defaults = get_landing_defaults(config)
    exchanges = set(config["runtime_sets"]["universe_exchanges"])
    work = Path(args.work_dir)

    def landed(endpoint: str) -> dict[str, Any]:
        return load_landed_payloads(args.write_target, args.landing_root, defaults, "fmp", endpoint, args.run_date)

    if args.stage == "candidates":
        screener_pages = [p for ep in SCREENER_ENDPOINTS for p in landed(ep).values()]
        if not _rows(screener_pages):
            raise SystemExit(
                f"no company_screener rows landed for run-date {args.run_date} -- refusing to build an "
                "empty universe (wrong --run-date? endpoint 402?)"
            )
        candidates, excluded = build_candidates(
            screener_pages, list(landed("delisted_companies").values()), exchanges, args.delisted_since,
        )
        work.mkdir(parents=True, exist_ok=True)
        (work / "candidates.json").write_text(json.dumps(candidates, indent=1, sort_keys=True), encoding="utf-8")
        (work / "candidates_excluded.json").write_text(json.dumps(excluded, indent=1, sort_keys=True), encoding="utf-8")
        (work / "candidates_universe.yaml").write_text(
            yaml.safe_dump({"symbols": sorted(candidates)}, sort_keys=False), encoding="utf-8")
        tiers = {t: sum(1 for c in candidates.values() if c["tier"] == t) for t in ("t1", "t2")}
        print(f"candidates: t1={tiers['t1']} t2={tiers['t2']} excluded={len(excluded)} -> {work}")
        return

    candidates = json.loads((work / "candidates.json").read_text(encoding="utf-8"))
    pre_excluded_path = work / "candidates_excluded.json"
    pre_excluded = json.loads(pre_excluded_path.read_text(encoding="utf-8")) if pre_excluded_path.exists() else {}
    kept, excluded = apply_profile_filter(candidates, landed("profile"))
    tiers = {t: sorted(s for s, c in kept.items() if c["tier"] == t) for t in ("t1", "t2")}
    all_excluded = {**pre_excluded, **excluded}
    report = {
        "run_date": args.run_date,
        "counts": {"t1": len(tiers["t1"]), "t2": len(tiers["t2"]), "excluded": len(all_excluded)},
        "no_profile": sorted(s for s, c in kept.items() if c.get("note") == "no_profile"),
        "excluded": all_excluded,
        "blocked": None,
    }
    if len(tiers["t1"]) < args.min_t1:
        blocked_msg = (
            f"t1 has {len(tiers['t1'])} symbols < --min-t1 {args.min_t1} -- refusing to overwrite "
            f"{args.universe_config} (were profiles landed for run-date {args.run_date}?)"
        )
        report["blocked"] = blocked_msg
        (work / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True), encoding="utf-8")
        raise SystemExit(blocked_msg)

    # Mã ĐANG niêm yết (T1) gần như luôn có profile; tỉ lệ T1 thiếu profile cao nghĩa là
    # bước profile chưa chạy / sai ngày. T2 (delisted) KHÔNG tính vào tỉ lệ vì thiếu
    # profile là bình thường với mã đã chết.
    t1_total = sum(1 for c in candidates.values() if c.get("tier") == "t1")
    t1_no_profile = sum(1 for s, c in kept.items() if c.get("tier") == "t1" and c.get("note") == "no_profile")
    if t1_total > 0 and (t1_no_profile / t1_total) > args.max_t1_no_profile_ratio:
        ratio = t1_no_profile / t1_total
        blocked_msg = (
            f"t1 has {t1_no_profile}/{t1_total} ({ratio:.1%}) symbols with no_profile > "
            f"--max-t1-no-profile-ratio {args.max_t1_no_profile_ratio} -- refusing to overwrite "
            f"{args.universe_config} (were profiles landed for run-date {args.run_date}?)"
        )
        report["blocked"] = blocked_msg
        (work / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True), encoding="utf-8")
        raise SystemExit(blocked_msg)

    (work / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True), encoding="utf-8")
    write_universe_config(args.universe_config, tiers, args.market, args.benchmark, args.delisted_since)
    print(f"universe: t1={len(tiers['t1'])} t2={len(tiers['t2'])} excluded={len(all_excluded)} -> {args.universe_config}")


if __name__ == "__main__":
    main()
