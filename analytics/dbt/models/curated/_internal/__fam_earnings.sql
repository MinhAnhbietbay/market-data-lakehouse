{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_earnings',
        tags=['layer::curated', 'internal']
    )
}}

-- 180 ngày: chu kỳ công bố là hàng quý nên ngưỡng của dữ liệu báo cáo hợp lý ở đây.
-- KHÔNG chép máy móc -- __fam_analyst.sql:22,49,59 dùng 90 cho dữ liệu analyst, nên
-- ngưỡng không phải hằng số toàn tầng (spec §4.2).
{{ collapse_asof(
    source_rel=ref('__fam_earnings_periodic'),
    key_cols=['symbol'],
    spine_rel=ref('__spine_daily'),
    shape='A',
    max_staleness_days=180,
    period_col='announcement_date'
) }}
