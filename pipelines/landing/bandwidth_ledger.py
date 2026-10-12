"""Sổ băng thông FMP cộng dồn qua nhiều lần chạy `run_batch`.

Vì sao cần: trần Premium là 50 GB / 30 ngày, còn mỗi `run_batch` chỉ thấy byte của chính nó.
Ngày 1 chạy tay hàng chục lệnh (mỗi endpoint một process) -- không có sổ chung thì
`--bandwidth-budget-bytes` chỉ là phanh của từng lệnh, không phải của cả tháng.

Vì sao đếm payload compact: FMP tính byte JSON compact của payload, không tính byte gzip
qua dây (đo trên dashboard 2026-08-19: cùng một request 56 KB gzip / 420 KB pretty đều bị tính
0,29 MB). Đếm byte qua dây sẽ thấp hơn thực tế ~5-7 lần.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def metered_payload_bytes(payload: Any) -> int:
    return len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def ledger_total_bytes(path: str | Path | None) -> int:
    """Tổng `metered_bytes` đã ghi. Chưa có file = 0; dòng hỏng thì nổ, không đoán."""
    if path is None:
        return 0
    ledger = Path(path)
    if not ledger.exists():
        return 0
    total = 0
    for line_no, line in enumerate(ledger.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            total += int(json.loads(line)["metered_bytes"])
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError(f"{ledger}:{line_no}: dòng sổ băng thông hỏng: {exc}") from exc
    return total


def append_ledger_entry(path: str | Path, entry: dict[str, Any]) -> None:
    ledger = Path(path)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    record = {**entry, "recorded_at": datetime.now(timezone.utc).isoformat()}
    with ledger.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")
