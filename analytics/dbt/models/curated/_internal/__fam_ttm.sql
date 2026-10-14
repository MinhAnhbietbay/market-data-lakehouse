{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_ttm',
        tags=['layer::curated', 'internal']
    )
}}

-- max_staleness_days=180 BẮT BUỘC: collapse_asof.sql:51-53 raise compiler error nếu
-- thiếu ("không truyền sẽ forward-fill vô hạn"). Giá trị phải khớp __fam_fundamental
-- (:23,33,43,53) -- lệch nhau sẽ làm revenue NULL trong khi revenue_ttm vẫn có giá
-- trị, hai cột cạnh nhau nói ngược nhau.
{{ collapse_asof(
    source_rel=ref('__fam_ttm_periodic'),
    key_cols=['symbol'],
    spine_rel=ref('__spine_daily'),
    shape='A',
    max_staleness_days=180,
    period_col='period_end_date'
) }}
