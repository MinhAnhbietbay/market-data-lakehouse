{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_gr_security_rating',
        tags=['layer::standardize', 'domain::group']
    )
}}

-- Security Rating (gr area), observation -- xếp hạng tổng hợp FMP tính
-- cho một mã tại một ngày. Tách 7 điểm thành phần sang std_gr_security_rating_score
-- (dạng long) để tuân thủ 3NF.

with ratings_src as (
    select
        symbol,
        rating_date,
        rating,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_ratings_historical') }}
    where symbol is not null and rating_date is not null
),

{{ bitemporal_version_chain(
    source_cte='ratings_src',
    key_cols=['symbol', 'rating_date'],
    fingerprint_cols=['rating'],
    first_known_from_expr='rating_date',
    pit_basis='observation'
) }}

select
    symbol,
    rating_date,
    rating,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

