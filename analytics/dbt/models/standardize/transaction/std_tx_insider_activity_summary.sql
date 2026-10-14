{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_tx_insider_activity_summary',
        tags=['layer::standardize', 'domain::transaction']
    )
}}

with insider_stats_src as (
    select
        symbol,
        cik,
        fiscal_year,
        fiscal_quarter,
        acquired_transactions,
        disposed_transactions,
        total_acquired,
        total_disposed,
        total_purchases,
        total_sales,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_insider_statistics') }}
    where symbol is not null and cik is not null
        and fiscal_year is not null and fiscal_quarter is not null
),

{{ bitemporal_version_chain(
    source_cte='insider_stats_src',
    key_cols=['symbol', 'cik', 'fiscal_year', 'fiscal_quarter'],
    fingerprint_cols=[
        'acquired_transactions',
        'disposed_transactions',
        'total_acquired',
        'total_disposed',
        'total_purchases',
        'total_sales'
    ],
    first_known_from_expr='run_date',
    first_known_from_is_system_time=true,
    pit_basis='observation'
) }}

select
    symbol,
    cik,
    fiscal_year,
    fiscal_quarter,
    acquired_transactions,
    disposed_transactions,
    total_acquired,
    total_disposed,
    total_purchases,
    total_sales,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

