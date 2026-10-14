{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_bd_analyst_rating_distribution',
        tags=['layer::standardize', 'domain::business_direction']
    )
}}

-- UPDATE (2026-08-20, Task 10): raw_fmp_analyst_recommendations now exists in
-- Iceberg with real data (452 rows measured) -- the KNOWN BLOCKED note below is
-- stale as of the staging-full-fidelity rebuild and no longer describes the
-- current state.
-- Original note (2026-08-17, spec N-23), kept for history: stg_fmp_analyst_
-- recommendations did not exist in Iceberg yet (source endpoint
-- /grades-historical, availability: unknown -- one of the 5 contracts flagged
-- in staging-regen-scoped memory as blocked by a fixable request-param bug
-- (limit: 360 likely exceeds current FMP tier), not a confirmed paid_required
-- tier restriction); the model hard-failed dbt build with TABLE_NOT_FOUND until
-- that staging table landed.
with recommendations_src as (
    select
        symbol,
        grade_date,
        analyst_ratings_strong_buy,
        analyst_ratings_buy,
        analyst_ratings_hold,
        analyst_ratings_sell,
        analyst_ratings_strong_sell,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_analyst_recommendations') }}
    where symbol is not null and grade_date is not null
),

unpivoted as (
    -- stack(N, 'tên', giá_trị, ...) là cách xoay wide->long của Spark; không có
    -- UNPIVOT như Trino. N phải khớp đúng số cặp liệt kê sau nó -- lệch thì
    -- Spark báo lỗi lúc phân tích chứ không im lặng cắt bớt.
    select
        symbol,
        grade_date,
        run_date,
        landing_key,
        rating_bucket,
        analyst_count
    from recommendations_src
    lateral view stack(
        5,
        'strong_buy', analyst_ratings_strong_buy,
        'buy', analyst_ratings_buy,
        'hold', analyst_ratings_hold,
        'sell', analyst_ratings_sell,
        'strong_sell', analyst_ratings_strong_sell
    ) as rating_bucket, analyst_count
),

non_null_buckets as (
    -- Ở dạng wide, một bucket không có dữ liệu là một ô null.
    -- Ở dạng long nó phải BIẾN MẤT chứ không thành một dòng "analyst_count null":
    -- một dòng như thế nói "bucket này tồn tại và bằng null".
    select * from unpivoted where analyst_count is not null
),

{{ bitemporal_version_chain(
    source_cte='non_null_buckets',
    key_cols=['symbol', 'grade_date', 'rating_bucket'],
    fingerprint_cols=['analyst_count'],
    first_known_from_expr='grade_date',
    pit_basis='observation'
) }}

select
    symbol,
    grade_date,
    rating_bucket,
    analyst_count,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

