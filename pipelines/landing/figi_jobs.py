"""Dựng job list cho OpenFIGI /v3/mapping. Thuần hàm, không I/O -- mọi luật an toàn
của việc phân giải định danh nằm ở đây để test được mà không cần mạng.
"""
import json
from typing import Any

# OpenFIGI dùng exchCode ở tầng composite quốc gia, không phải mã sàn của FMP.
# Mọi sàn Mỹ (NYSE/NASDAQ/AMEX/OTC) đều nằm trong composite "US". "US" tự nó
# cũng nằm trong tập này: configs/universes/*.yaml khai `market: US` -- market
# đã LÀ composite rồi, không phải tên sàn -- và load_symbol_records mặc định
# exchange_code = market khi chưa có venue cụ thể từ profile. Thiếu "US" ở đây,
# mọi symbol trong dev universe rơi xuống nhánh dot-suffix, không tìm thấy dấu
# chấm, và nổ guard fail-loud vốn dành cho case thật sự vô phương (đo thật: toàn
# bộ universe hỏng trước khi gọi HTTP).
_US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "OTC", "NYSEARCA", "BATS", "CBOE", "US"}
MAX_JOBS_PER_REQUEST = 100


def _exch_code(record: dict[str, Any]) -> str:
    exchange = (record.get("exchange_code") or "").upper()
    if exchange in _US_EXCHANGES:
        return "US"
    suffix = record["symbol"].rsplit(".", 1)
    if len(suffix) == 2:
        return suffix[1].upper()
    raise ValueError(
        f"cannot derive exchCode for {record['symbol']!r}: a TICKER job without exchCode "
        "resolves to a DIFFERENT company (measured: EQR -> BBG001S5YQR1, not BBG001S723L9)"
    )


def build_mapping_jobs(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for record in records:
        job: dict[str, Any] = {"includeUnlistedEquities": True}
        if record.get("isin"):
            job.update(idType="ID_ISIN", idValue=record["isin"])
        elif record.get("cusip"):
            job.update(idType="ID_CUSIP", idValue=record["cusip"])
        else:
            job.update(idType="TICKER", idValue=record["symbol"].rsplit(".", 1)[0],
                       exchCode=_exch_code(record))
        jobs.append(job)
    return jobs


def batch_jobs(jobs: list[dict[str, Any]], size: int = MAX_JOBS_PER_REQUEST) -> list[list[dict[str, Any]]]:
    return [jobs[i:i + size] for i in range(0, len(jobs), size)]


def load_symbol_records(
    universe_config: dict[str, Any],
    identifiers_path: str | None = None,
) -> list[dict[str, Any]]:
    """Ghép symbol universe với ISIN/CUSIP nếu có file định danh.

    Không có file -> mọi symbol đi nhánh TICKER (phủ 71/100 trên tập đã huỷ niêm yết).
    Có file -> nhánh ISIN (phủ 9/10). Quy trình hai nhịp là cố ý: lần chạy đầu chưa có
    raw nên dùng TICKER; sau khi raw_fmp_sec_profile có dữ liệu thì xuất file định
    danh và chạy lại để nâng cấp. Landing không đọc ngược raw.
    """
    identifiers: dict[str, dict[str, Any]] = {}
    if identifiers_path:
        with open(identifiers_path) as handle:
            identifiers = {row["symbol"]: row for row in json.load(handle)}

    records = []
    for symbol in universe_config["symbols"]:
        known = identifiers.get(symbol, {})
        records.append({
            "symbol": symbol,
            "isin": known.get("isin"),
            "cusip": known.get("cusip"),
            "exchange_code": known.get("exchange_code") or universe_config.get("market", "US"),
        })
    return records
