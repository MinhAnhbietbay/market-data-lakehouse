"""Phân loại 4 chế độ hỏng của FMP -- đọc BODY, không chỉ status code.

Đo thật 2026-08-06/08 (xem docs/superpowers/specs/2026-08-11-landing-extension-design.md
§2): hai loại 402 khác hẳn nhau về ý nghĩa (khoá theo symbol vs khoá theo endpoint), cộng
một chuỗi không phải JSON (cùng loại khoá, trả text thuần), cộng một lỗi JSON riêng khi
cạn quota ngày. record_output trước đây chỉ ghi phẳng "HTTP {status}" -- không phân biệt
được "mã này ngoài whitelist, bỏ qua và chạy tiếp" với "hết quota ngày, dừng cả run" với
"lỗi thật, coi là fail".
"""
from __future__ import annotations

from typing import Any

_SYMBOL_LOCKED_MARKER = "Premium Query Parameter"
_ENDPOINT_LOCKED_MARKER = "Restricted Endpoint"
_QUOTA_EXHAUSTED_MARKER = "Limit Reach"


def _body_text(payload: Any) -> str:
    """Gộp mọi hình dạng payload lỗi (JSON dict, hoặc {"raw_body": "..."} không phải
    JSON) thành một chuỗi để so khớp marker -- caller không cần biết trước hình dạng."""
    if isinstance(payload, dict):
        if "raw_body" in payload:
            return str(payload["raw_body"])
        return " ".join(str(value) for value in payload.values())
    return str(payload)


def classify_error(status: int, payload: Any) -> str:
    if 200 <= status < 300:
        return "unknown"

    text = _body_text(payload)

    # Quota trước tiên: FMP đã quan sát trả "Limit Reach" dưới cả status 429 lẫn 402,
    # nên phải khớp bằng nội dung body trước khi xét đến hai loại 402 còn lại.
    if _QUOTA_EXHAUSTED_MARKER in text:
        return "quota_exhausted"
    if _SYMBOL_LOCKED_MARKER in text:
        return "symbol_locked"
    if _ENDPOINT_LOCKED_MARKER in text:
        return "endpoint_locked"
    if status == 429:
        return "quota_exhausted"
    return "unknown"
