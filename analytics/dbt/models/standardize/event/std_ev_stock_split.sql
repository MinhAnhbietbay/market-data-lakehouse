{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_stock_split',
        tags=['layer::standardize', 'domain::event']
    )
}}

with splits_ranked as (
    select
        symbol,
        split_effective_date,
        numerator,
        denominator,
        split_type,
        run_date,
        landing_key,
        row_number() over (
            partition by symbol, split_effective_date, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_splits') }}
    where symbol is not null and split_effective_date is not null
),

splits_latest as (
    select
        symbol,
        split_effective_date,
        numerator,
        denominator,
        split_type,
        run_date,
        landing_key
    from splits_ranked
    where rn = 1
),
{{ bitemporal_version_chain(
    source_cte='splits_latest',
    key_cols=['symbol', 'split_effective_date'],
    fingerprint_cols=['numerator', 'denominator', 'split_type'],
    first_known_from_expr='split_effective_date',
    pit_basis='observation'
) }}

select
    symbol,
    split_effective_date,
    numerator,
    denominator,
    split_type,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

