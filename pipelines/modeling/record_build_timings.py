"""Trích thời gian build từng model từ run_results.json của dbt.

Ngưỡng chuyển sang incremental ở spec 2026-08-20 §3.4.2 (10 phút, hoặc 20%
wall-clock cả build) chỉ có nghĩa nếu có ai đó thật sự ghi lại con số. Không
có bước này thì ngưỡng là chữ chết -- lúc cần đối chiếu sẽ không có dữ liệu,
và quyết định lại thành cảm tính, đúng thứ ngưỡng sinh ra để tránh.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

MODEL_PREFIX = "model."


def record_timings(run_results_path: Path, out_path: Path) -> dict[str, float]:
    payload = json.loads(Path(run_results_path).read_text())

    timings: dict[str, float] = {}
    for r in payload.get("results", []):
        uid = r.get("unique_id", "")
        if not uid.startswith(MODEL_PREFIX):
            continue
        if r.get("status") != "success":
            continue
        timings[uid.rsplit(".", 1)[-1]] = float(r["execution_time"])

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    # Mẫu số là elapsed_time ở root của run_results.json (wall-clock thực tế của cả build,
    # bao gồm cả tests và failed models) theo đúng spec §3.4.2. Không dùng sum(timings.values())
    # vì sum là tổng CPU time của các model thành công trên các worker. Hai số này hiện gần
    # bằng nhau do Spark session mode chạy tuần tự, nhưng sự trùng hợp này là bẫy: khi chạy
    # song song (threads: 4), sum sẽ lớn hơn nhiều so với wall-clock.
    total = float(payload.get("elapsed_time", 0.0))
    lines = [f"\n## {stamp} — tổng {total:.1f}s\n"]
    for name, secs in sorted(timings.items(), key=lambda kv: -kv[1]):
        share = (secs / total * 100) if total else 0.0
        lines.append(f"- `{name}` — {secs:.1f}s ({share:.1f}%)")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a") as fh:
        fh.write("\n".join(lines) + "\n")

    return timings
