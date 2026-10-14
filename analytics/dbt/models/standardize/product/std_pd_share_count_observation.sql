{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_pd_share_count_observation',
        tags=['layer::standardize', 'domain::product']
    )
}}

with shares_float_src as (
    select
        symbol,
        observation_date,
        float_shares,
        outstanding_shares,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_shares_float') }}
    where symbol is not null and observation_date is not null
),

-- observation_date là timestamp(6) with time zone ở raw -- cast về date
-- vì known_from bắt buộc kiểu date (global constraint).
{{ bitemporal_version_chain(
    source_cte='shares_float_src',
    key_cols=['symbol', 'observation_date'],
    fingerprint_cols=['float_shares', 'outstanding_shares'],
    first_known_from_expr='cast(observation_date as date)',
    pit_basis='observation'
) }}

select
    symbol,
    observation_date,
    float_shares,
    outstanding_shares,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

