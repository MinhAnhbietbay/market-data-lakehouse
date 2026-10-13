{#
    Sụp đổ một nguồn bitemporal về lưới ngày. Đây là TỔNG QUÁT HOÁ của mẫu
    as-of chuẩn (AGENTS.md:52, spec 2026-08-20 §3.2), KHÔNG phải mẫu thứ hai:
      shape='A' ≡ mẫu gốc, không có trục nghiệp vụ, cộng chặn staleness
      shape='B' ≡ mẫu gốc + vị từ khoảng [valid_from, valid_to)

    Hai bậc chọn, KHÔNG được gộp thành một max(known_from) toàn cục:
      1. chọn kỳ có period_end_date lớn nhất  (chỉ shape A nhiều kỳ)
      2. trong kỳ đó chọn known_from lớn nhất  (bước collapse version)
    Gộp lại sẽ lùi kỳ tài chính về quá khứ khi một bản đính chính của kỳ cũ
    đến sau kỳ mới. Spec §2.

    valid_to IS NULL phải xử lý tường minh: std_ar_listing.sql:211 đặt
    valid_to = coalesce(next_valid_from, delisted_date), nên niêm yết đang hoạt
    động có valid_to NULL. Viết `D < valid_to` trần sẽ trả UNKNOWN và loại sạch
    mọi thực thể đang hoạt động -- hỏng im lặng, đo được 5/84 dòng.

    max_staleness_days CHỈ có nghĩa với shape='A'. Shape B đã bị chặn bởi chính
    khoảng hiệu lực; áp thêm staleness sẽ xoá sector/industry của công ty đang
    hoạt động bình thường.

    key_cols có HAI loại: spine_keys (tồn tại trên cả spine lẫn source, vd
    symbol -- dùng để JOIN thật và PARTITION) và source_only_keys (chỉ tồn tại
    trên source, vd rating_bucket -- spine không có cột này, chỉ PARTITION).

    spine_rel có thể là ref() thật HOẶC tên một CTE cục bộ định nghĩa trước đó
    trong cùng câu lệnh (đã dùng ở __fam_group.bridged). adapter.get_columns_in_relation KHÔNG introspect
    được CTE cục bộ (chỉ hoạt động với relation thật) -- trả về rỗng. Khi đó
    coi MỌI key_cols là spine_keys (giả định spine cục bộ tự mang đủ cột key
    cần, đúng theo cách dự án đang dùng pattern này). Phát hiện khi Task 11
    build __fam_group thật với spine_rel='bridged', key_cols=['cik'].

    spine_keys có thể RỖNG HỢP LỆ (không phải do introspect thất bại): một
    spine chỉ có `date` (vd curated_dim_date cho macro/treasury) không chia
    sẻ cột nào với key_cols=['series_name'] hay ['tenor'] -- toàn bộ key_cols
    khi đó là source_only_keys. Điều kiện JOIN được gom vào một danh sách rồi
    nối bằng " and " thay vì giả định spine_keys luôn đóng góp ít nhất một vế
    -- nếu không, spine_keys rỗng sẽ sinh mệnh đề JOIN bắt đầu bằng "AND" trơ
    trọi (lỗi cú pháp). Dự đoán trước khi Task 13 build curated_macro_daily
    (spine=curated_dim_date, không có symbol/series_name/tenor nào cả).
#}
{% macro collapse_asof(source_rel, key_cols, spine_rel, shape, max_staleness_days=none, period_col=none) %}

{%- if shape not in ['A', 'B'] -%}
    {{ exceptions.raise_compiler_error("collapse_asof: shape phải là 'A' hoặc 'B', nhận: " ~ shape) }}
{%- endif -%}
{%- if shape == 'B' and max_staleness_days is not none -%}
    {{ exceptions.raise_compiler_error("collapse_asof: shape B không nhận max_staleness_days -- khoảng [valid_from, valid_to) đã là ràng buộc còn-hiệu-lực") }}
{%- endif -%}
{%- if shape == 'A' and max_staleness_days is none -%}
    {{ exceptions.raise_compiler_error("collapse_asof: shape A bắt buộc truyền max_staleness_days -- không truyền sẽ forward-fill vô hạn (spec §2)") }}
{%- endif -%}

{%- set spine_keys = [] -%}
{%- set source_only_keys = [] -%}
{%- set value_cols = [] -%}
{%- if execute -%}
    {%- set spine_columns = adapter.get_columns_in_relation(spine_rel) | map(attribute='name') | list -%}
    {%- if spine_columns | length == 0 -%}
        {%- for k in key_cols -%}
            {%- do spine_keys.append(k) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for k in key_cols -%}
            {%- if k in spine_columns -%}
                {%- do spine_keys.append(k) -%}
            {%- else -%}
                {%- do source_only_keys.append(k) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- set src_columns = adapter.get_columns_in_relation(source_rel) -%}
    {%- for col in src_columns -%}
        {%- if col.name not in spine_keys -%}
            {%- do value_cols.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

with __collapse_ranked as (
    -- Spark không có QUALIFY (AGENTS.md:44) -- xếp hạng ở đây, lọc rn=1 ở select
    -- ngoài, thay vì qualify. __collapse_rn không phải cột nghiệp vụ.
    select
        s.date,
        {%- for k in spine_keys %}
        s.{{ k }},
        {%- endfor %}
        {%- for c in value_cols %}
        v.{{ c }},
        {%- endfor %}
        row_number() over (
            partition by s.date
                {%- for k in spine_keys %}, s.{{ k }}{% endfor %}
                {%- for k in source_only_keys %}, v.{{ k }}{% endfor %}
            order by
                {%- if period_col is not none %}
                v.{{ period_col }} desc,
                {%- endif %}
                v.known_from desc
        ) as __collapse_rn
    from {{ spine_rel }} s
    left join {{ source_rel }} v
        {%- set join_conditions = [] %}
        {%- for k in spine_keys %}
        {%- do join_conditions.append('v.' ~ k ~ ' = s.' ~ k) %}
        {%- endfor %}
        {%- do join_conditions.append('v.known_from <= s.date') %}
        {%- if shape == 'B' %}
        {%- do join_conditions.append('v.valid_from <= s.date') %}
        {%- do join_conditions.append('(v.valid_to is null or s.date < v.valid_to)') %}
        {%- endif %}
        {%- if max_staleness_days is not none %}
        {%- do join_conditions.append('s.date <= date_add(v.known_from, ' ~ max_staleness_days ~ ')') %}
        {%- endif %}
        on {{ join_conditions | join('
        and ') }}
)
-- __collapse_rn không project ra ngoài được (Spark không có SELECT * EXCEPT, đã kiểm trực
-- tiếp) -- vô hại vì mọi caller (__fam_*) chọn cột theo tên, không select *.
select *
from __collapse_ranked
where __collapse_rn = 1

{% endmacro %}
