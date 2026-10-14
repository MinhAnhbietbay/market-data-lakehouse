{{
    config(
        materialized='table',
        partition_by=['years(period_end_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ac_ratio',
        tags=['layer::standardize', 'domain::accounting']
    )
}}

with ratios_src as (
    select
        symbol,
        fiscal_year,
        period,
        period_end_date,
        reported_currency,
        price_to_fair_value,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_ratios') }}
),

{{ bitemporal_version_chain(
    source_cte='ratios_src',
    key_cols=['symbol', 'fiscal_year', 'period'],
    fingerprint_cols=['period_end_date', 'reported_currency', 'price_to_fair_value'],
    first_known_from_expr='run_date',
    first_known_from_is_system_time=true,
    pit_basis='observation'
) }}

select
    symbol,
    fiscal_year,
    period,
    period_end_date,
    reported_currency,
    price_to_fair_value,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned
