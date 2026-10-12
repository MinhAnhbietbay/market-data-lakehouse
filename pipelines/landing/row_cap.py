from typing import Any


def is_probably_truncated(payload: Any, row_cap: int | None) -> bool:
    """True khi payload dài đúng bằng cap của endpoint -- dấu hiệu bị cắt im lặng.

    FMP cap cứng một số endpoint (OHLCV: 5000 dòng) và khi vượt thì trả cap dòng gần nhất
    với HTTP 200, không cờ, không lỗi. Không có cách nào phân biệt "đúng 5000 dòng thật"
    với "bị cắt còn 5000" từ một response đơn lẻ, nên ta chọn báo động: một chuỗi ngày kết
    thúc đúng ở tròn cap là trùng hợp gần như không xảy ra, còn bỏ sót thì mất lịch sử
    vĩnh viễn sau khi hết hạn Premium.

    Cách sửa khi bị cảnh báo là chia nhỏ cửa sổ ngày, không phải nới cap.
    """
    if row_cap is None or not isinstance(payload, list):
        return False
    return len(payload) >= row_cap
