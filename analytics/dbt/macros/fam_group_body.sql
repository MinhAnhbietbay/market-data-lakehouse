{#
    Thân logic của __fam_group.sql, tách ra macro để __pit_test_group.sql
    (Task 18) gọi lại NGUYÊN VẸN thay vì copy tay -- cùng lý do/quyết định như
    fam_price_body.sql (xem macro đó). Quyết định 2026-08-26.

    5 tham số là 5 nguồn dữ liệu: __fam_group.sql truyền các std_*/__spine_daily
    thật, __pit_test_group.sql truyền các pit_seed_* fixture (Task 18). Toàn bộ
    logic bắc cầu cik↔symbol qua share_class_figi giữ NGUYÊN, không đổi một
    dòng so với bản trước khi tách macro. (workforce_rel thêm ở đợt 3c, shape A
    -- xem comment tại CTE workforce_asof bên dưới.)
#}
{% macro fam_group_body(listing_rel, spine_rel, security_rel, legal_entity_rel, workforce_rel) %}

{%- set listing_cols = adapter.get_columns_in_relation(listing_rel) | map(attribute='name') | list if execute else [] -%}
{%- set entity_cols = adapter.get_columns_in_relation(legal_entity_rel) | map(attribute='name') | list if execute else [] -%}

-- Hai chặng, as-of đặt ở chặng CÓ trục thời gian:
--   std_ip_legal_entity (cik, valid_from, valid_to, known_from)
--     ⨝ std_pd_security  (cik ↔ share_class_figi)   -- phẳng, KHÔNG có trục thời gian
--     ⨝ std_ar_listing   (share_class_figi ↔ symbol, valid_from, valid_to)
--
-- std_pd_security phẳng mà vẫn đúng vì share_class_figi KHÔNG đổi khi ticker
-- đổi -- đó là lý do đợt identity spine chọn nó làm khoá bền. Phần biến thiên
-- theo thời gian nằm ở std_ar_listing.
--
-- ANTI-PATTERN: nối thẳng cik ↔ symbol bằng std_pd_security rồi join vào spine.
-- Một công ty đổi ticker năm 2023 sẽ có toàn bộ lịch sử sector gắn vào ticker
-- HIỆN TẠI, kể cả những ngày ticker đó chưa tồn tại -- và vì là left-join, sai
-- im lặng.
--
-- Cả hai chặng as-of dùng collapse_asof (shape='B'), không viết tay lần thứ
-- hai -- giữ đúng nguyên tắc "một mẫu, một macro" (spec §2).

with listing_asof as (
    {{ collapse_asof(
        source_rel=listing_rel,
        key_cols=['symbol'],
        spine_rel=spine_rel,
        shape='B'
    ) }}
),

-- market_code/exchange_code/known_from của nhánh listing phải đi qua ĐÂY, không thể
-- lấy từ output của entity_asof: collapse_asof chỉ project s.date + s.<spine_keys> +
-- v.<value_cols>, mà spine ở đây là CTE cục bộ nên mọi key_cols thành spine_keys và
-- cột thừa bị bỏ. Select cuối đọc thẳng từ bridged -- đúng cách `symbol` đang sống sót.
bridged as (
    select
        la.date,
        la.symbol,
        {%- if 'market_code' in listing_cols %}
        la.market_code,
        {%- else %}
        cast(null as string) as market_code,
        {%- endif %}
        {%- if 'exchange_code' in listing_cols %}
        la.exchange_code,
        {%- else %}
        cast(null as string) as exchange_code,
        {%- endif %}
        la.known_from as listing_known_from,
        sec.cik
    from listing_asof la
    join {{ security_rel }} sec on sec.share_class_figi = la.share_class_figi
),

entity_asof as (
    {{ collapse_asof(
        source_rel=legal_entity_rel,
        key_cols=['cik'],
        spine_rel='bridged',
        shape='B'
    ) }}
),

-- Workforce dùng shape A, KHÁC nhánh legal_entity ngay trên (shape B). Lý do:
-- collapse_asof shape B đòi [valid_from, valid_to) và TỪ CHỐI max_staleness_days;
-- std_ip_workforce_observation không có hai cột đó, nó có period_end_date.
--
-- max_staleness_days = 550 dẫn từ số đo, không chọn tròn: khoảng cách lớn nhất
-- giữa hai quan sát liên tiếp của cùng một cik là 418 ngày (131 cặp trên 136 dòng).
-- 550 phủ 418 cộng biên cho một kỳ nộp trễ mà không kéo một con số nhân sự cũ đi
-- qua mốc 2 năm. Nợ N-n: đo lại khi universe vượt 5 symbol.
workforce_asof as (
    {{ collapse_asof(
        source_rel=workforce_rel,
        key_cols=['cik'],
        spine_rel='bridged',
        shape='A',
        max_staleness_days=550,
        period_col='period_end_date'
    ) }}
)

select
    b.date,
    b.symbol,
    e.sector          as vendor_sector,
    e.industry        as vendor_industry,
    b.market_code     as market,
    b.exchange_code   as exchange,
    {%- if 'country_code' in entity_cols %}
    e.country_code    as country,
    {%- else %}
    cast(null as string) as country,
    {%- endif %}
    w.employee_count,
    -- known_from phải phủ CẢ BA nhánh: giữ nguyên e.known_from thì market/exchange
    -- (nguồn: listing) nằm ngoài assert_collapse_asof_no_lookahead. greatest() của
    -- Spark bỏ qua NULL nên dòng chưa có workforce/listing không bị NULL-hoá.
    greatest(b.listing_known_from, e.known_from, w.known_from) as known_from,
    current_timestamp as loaded_at
from bridged b
left join entity_asof e on e.date = b.date and e.cik = b.cik
left join workforce_asof w on w.date = b.date and w.cik = b.cik

{% endmacro %}
