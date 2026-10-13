{#
    Cờ "đây là phiên bản mới nhất ta BIẾT" cho một business key.

    Business key = khoá tự nhiên TRỪ known_from (spec 3b §4.5). Truyền đúng các
    cột đó, KHÔNG truyền symbol đơn -- bảng có hạt nhỏ hơn mã sẽ ra cờ sai.

    Dùng max() over chứ không row_number(): khoá tự nhiên (business key +
    known_from) là duy nhất, nên đúng MỘT dòng đạt max known_from trong mỗi
    business key -- không có hoà. Nếu khoá tự nhiên vỡ tính duy nhất thì
    unique_combination_of_columns đỏ trước, không phải cờ này.

    KHÔNG đặt tên is_current: đó là trục hiệu lực nghiệp vụ (valid_from/valid_to)
    của dim_company/dim_listing đợt 3a, khác trục thời điểm biết.

    Biểu thức trả về có ngoặc bao ngoài: không có nó thì gọi macro trong một ngữ
    cảnh có toán tử khác (coalesce, and, not) sẽ dính sai độ ưu tiên -- lỗi im
    lặng vì kết quả vẫn là boolean.
#}
{% macro latest_known_flag(business_key_cols) %}
(known_from = max(known_from) over (partition by {{ business_key_cols | join(', ') }}))
{% endmacro %}
