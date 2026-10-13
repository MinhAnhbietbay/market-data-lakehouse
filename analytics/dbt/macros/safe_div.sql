{#
    Chia an toàn, ba chế độ. Trước đợt này mỗi chỗ tự bọc nullif và
    __fam_fundamental.sql:85-88 chia trần không bọc gì -- với 72 field mà phần lớn
    là phép chia, để mỗi chỗ tự lo là cách chắc chắn nhất để có một chỗ quên.

    mode='nonzero'    -- mặc định: nullif(den, 0)
    mode='positive'   -- NULL khi den <= 0. Quy ước 5.0.5 (spec 2026-07-28 :698) gọi
                         tên đúng 4 field: pe_ratio, ev_ebitda, interest_coverage,
                         pb_ratio. Mẫu âm ở đó làm tỷ số mất ý nghĩa kinh tế.
    mode='sign_aware' -- NULL khi den <= 0, dành cho tỷ số CHẤT LƯỢNG
                         (cfo_to_net_income, fcf_to_earnings, capex_to_depreciation).
                         Khác 'positive' về LÝ DO, không về hành vi: ở đây vấn đề là
                         tử và mẫu cùng âm cho ra số dương giả (-20 / -10 = 2 đọc
                         thành "chất lượng gấp đôi"). Tách tên để người sau biết đây
                         là mở rộng có chủ đích của spec 2026-08-27 §5.1, không phải
                         diễn giải sai quy ước 5.0.5.
#}
{% macro safe_div(num, den, mode='nonzero') %}
{%- if mode not in ['nonzero', 'positive', 'sign_aware'] -%}
    {{ exceptions.raise_compiler_error("safe_div: mode phải là 'nonzero'/'positive'/'sign_aware', nhận: " ~ mode) }}
{%- endif -%}
{%- if mode == 'nonzero' -%}
    ({{ num }}) / nullif({{ den }}, 0)
{%- else -%}
    case when ({{ den }}) > 0 then ({{ num }}) / ({{ den }}) end
{%- endif -%}
{% endmacro %}
