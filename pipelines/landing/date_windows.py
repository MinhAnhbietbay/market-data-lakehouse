"""Interpret YAML-declared date windows -- the only place history depth may live.

Spec A1 requires pulling 30 years of history, but FMP's `full` OHLCV endpoint caps
responses at ~5000 rows (~20 years) and silently truncates instead of erroring
(verified LIVE). The only way to get the full 30 years is to split the pull into
several bounded requests. Depth is a business fact (how many years does the
strategy need, and how many rows/request does this vendor tolerate before it starts
truncating) -- neither belongs in Python. This module is deliberately a pure
*interpreter*: it turns a small declarative `windows:` block into concrete
`{from, to}` date pairs and contains no endpoint-name branching. Adding a new
endpoint, or changing depth for an existing one, is a YAML edit, never a code edit.
"""
import calendar
import re
from datetime import date, timedelta
from typing import Any

_RELATIVE_SPAN_PATTERN = re.compile(r"^-?(\d+)([dmy])$")


def _parse_relative_span(text: str, field_name: str) -> tuple[int, str]:
    """Parse "-<N><unit>" (unit in {d, m, y}) into (N, unit), N > 0.

    Rejects anything else -- including a merely-misspelled unit like "-30yr" --
    with `field_name` (always "span" or "split_every") baked into the message, so a
    typo in YAML fails loudly at config-parse time instead of silently producing a
    window with the wrong depth (which would only surface months later as "we're
    missing 20 years of history").

    N == 0 (e.g. "0y") is rejected here too, not just "obviously wrong" units: a
    zero-magnitude `split_every` would never move `_subtract_span`'s anchor, so the
    splitting loop in `expand_windows` would never reach `start` and would spin
    forever appending identical windows -- reachable via a one-character YAML typo
    ("10y" -> "0y"), not just deliberate misuse.
    """
    match = _RELATIVE_SPAN_PATTERN.match(text)
    if not match:
        raise ValueError(
            f"invalid {field_name} {text!r}: expected '-<N><unit>' with unit in "
            "{d, m, y} (e.g. '-30y')"
        )
    magnitude, unit = match.groups()
    if int(magnitude) == 0:
        raise ValueError(
            f"invalid {field_name} {text!r}: magnitude must be > 0 (a 0-magnitude "
            "split_every never advances, so window splitting would never terminate)"
        )
    return int(magnitude), unit


def _subtract_span(anchor: date, magnitude: int, unit: str) -> date:
    """Subtract a relative span from `anchor`, clamping day-of-month like relativedelta.

    Years are expressed as 12x months rather than a separate year branch: that's
    the only way "Feb 29 minus 1 year" and "Mar 31 minus 1 month" both land on a
    valid date (Feb 28, Feb 28/29) without duplicating the clamping logic in a
    dependency we're not allowed to add (Global Constraint: no new dependencies --
    stdlib-only stand-in for `dateutil.relativedelta`).
    """
    if unit == "d":
        return anchor - timedelta(days=magnitude)

    months = magnitude if unit == "m" else magnitude * 12
    total_months = anchor.year * 12 + (anchor.month - 1) - months
    year, month0 = divmod(total_months, 12)
    month = month0 + 1
    day = min(anchor.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def expand_windows(spec: dict[str, Any] | None, today: date) -> list[dict[str, str]]:
    """Turn a `windows:` YAML block into concrete `{from, to}` date-string pairs.

    `today` is injected (never `date.today()` read internally) so callers -- and
    every test in `test_date_windows.py` -- get identical output regardless of
    which day they actually run on.

    Three spec shapes, in priority order:
    - absent/empty -> `[]` (endpoint keeps requesting with no from/to -- the
      backward-compat path for the 24 endpoints that don't declare `windows` yet).
    - `{"from": ..., "to": ...}` -> passed through verbatim, for a fixed backfill
      window that must NOT drift with `today`. Adding `split_every` to this shape
      splits the fixed window the same way the relative shape is split -- needed
      because a vendor that truncates at N days/request truncates regardless of how
      the window was declared (FMP `/treasury-rates` + `/economic-indicators`,
      measured 2026-09-22: 90 calendar days back from `to`, silently).
    - `{"span": "-<N><unit>", "split_every": "-<N><unit>"}` (split_every optional)
      -> one or more windows counting back from `today`. Most-recent-first, so a
      run that runs out of bandwidth partway through has already fetched the data
      most likely to be used (recent history) before the older chunks.
    """
    if not spec:
        return []

    split_every = spec.get("split_every")

    has_from = "from" in spec
    has_to = "to" in spec
    if has_from or has_to:
        if not (has_from and has_to):
            raise ValueError("windows spec with an absolute date must declare both 'from' and 'to'")
        if not split_every:
            # Đường verbatim CỐ Ý không parse: giữ nguyên hành vi cũ cho mọi khai báo
            # tuyệt đối không chia nhỏ. Chỉ khi có `split_every` mới cần hiểu hai đầu
            # mốc như ngày thật để chia được.
            return [{"from": spec["from"], "to": spec["to"]}]
        start = date.fromisoformat(spec["from"])
        end = date.fromisoformat(spec["to"])
        if start > end:
            raise ValueError(
                f"windows spec has from {spec['from']!r} after to {spec['to']!r}"
            )
    elif "span" not in spec:
        raise ValueError(
            "windows spec must declare either both 'from' and 'to' (absolute) or "
            f"'span' (relative); got keys {sorted(spec.keys())!r}"
        )
    else:
        span_magnitude, span_unit = _parse_relative_span(spec["span"], "span")
        start = _subtract_span(today, span_magnitude, span_unit)
        end = today

    if not split_every:
        return [{"from": start.isoformat(), "to": end.isoformat()}]

    chunk_magnitude, chunk_unit = _parse_relative_span(split_every, "split_every")

    # Vòng chia dùng CHUNG cho cả hai nhánh: neo tuyệt đối chỉ khác nhánh tương đối ở
    # chỗ `end` lấy từ YAML thay vì từ `today`. Một nhà cung cấp cắt ở N ngày/request
    # cắt như thế bất kể cửa sổ được khai kiểu nào, nên luật chia phải giống nhau.
    windows: list[dict[str, str]] = []
    anchor = end
    window_to = end
    while True:
        window_from = _subtract_span(anchor, chunk_magnitude, chunk_unit)
        reached_start = window_from <= start
        if reached_start:
            window_from = start
        windows.append({"from": window_from.isoformat(), "to": window_to.isoformat()})
        if reached_start:
            break
        anchor = window_from
        window_to = window_from - timedelta(days=1)

    return windows


def validate_window_partition_keys(config: dict[str, Any]) -> None:
    """Every endpoint declaring `windows` must have {from} in its partition_key.

    `object_key` is built from `partition_key` (see envelope.py::build_object_key),
    and landing writes to the same object_key overwrite in place. Without {from} in
    the template, 2 or 3 windows for the same symbol all render to the identical
    partition_key -- the later window's write silently clobbers the earlier one's
    object, and the run manifest still reports the expected object count (each
    write did succeed, just onto the same key), so nothing else catches this.
    """
    for source_name, source_config in config.get("sources", {}).items():
        for endpoint_name, endpoint_config in source_config.get("endpoints", {}).items():
            if not endpoint_config.get("windows"):
                continue
            partition_key = endpoint_config.get("landing", {}).get("partition_key", "")
            if "{from}" not in partition_key:
                raise ValueError(
                    f"{source_name}.{endpoint_name} declares `windows` but its "
                    f"partition_key {partition_key!r} does not contain {{from}} -- "
                    "successive windows would overwrite each other's landing object"
                )


def override_window_spec(
    endpoint_config: dict[str, Any],
    *,
    window_from: str | None,
    window_to: str | None,
    split_every: str | None,
) -> dict[str, Any]:
    """Replace an endpoint's `windows:` block with a one-off absolute backfill range.

    Depth lives in YAML (this module's docstring) *for the recurring run*. A deep
    backfill is a different thing: it happens once, costs far more requests than a
    daily run may spend, and must be splittable into stages that fit a vendor's daily
    quota. Encoding it in YAML would charge every future nightly run for it --
    measured 2026-09-22, a 10-year macro backfill is 546 FMP requests against a
    250/day plan -- so it is passed per-invocation instead.

    Only endpoints that ALREADY declare `windows` are overridden. An endpoint without
    one has no `{from}` in its `partition_key` (see `validate_window_partition_keys`),
    so handing it 42 windows would silently collapse them onto one landing object.
    Skipping it keeps `--endpoint` the only thing that selects what gets backfilled.
    """
    if window_from is None and window_to is None:
        return endpoint_config
    if window_from is None or window_to is None:
        raise ValueError(
            "a backfill window needs both --window-from and --window-to "
            f"(got from={window_from!r}, to={window_to!r})"
        )
    if not endpoint_config.get("windows"):
        return endpoint_config

    spec: dict[str, Any] = {"from": window_from, "to": window_to}
    if split_every:
        spec["split_every"] = split_every
    # Bản sao nông là đủ và cố ý: `windows` bị thay hẳn bằng dict mới, mọi khoá khác
    # (params, landing, tiers...) chỉ được đọc.
    return {**endpoint_config, "windows": spec}
