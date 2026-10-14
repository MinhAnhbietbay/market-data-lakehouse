{{
    config(
        materialized='table',
        partition_by=['years(trade_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_tx_market_cap_observation',
        tags=['layer::standardize', 'domain::transaction']
    )
}}

with market_cap_src as (
    select
        symbol,
        trade_date,
        market_cap,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_historical_market_cap') }}
    where symbol is not null and trade_date is not null
),

{{ bitemporal_version_chain(
    source_cte='market_cap_src',
    key_cols=['symbol', 'trade_date'],
    fingerprint_cols=['market_cap'],
    first_known_from_expr='trade_date',
    pit_basis='observation'
) }}

select
    symbol,
    trade_date,
    market_cap,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

