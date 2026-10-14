{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_symbol_change_event',
        tags=['layer::standardize', 'domain::event']
    )
}}

-- analytics/dbt/models/standardize/event/std_ev_symbol_change_event.sql
-- Contract và source (raw_fmp_symbol_change) ĐÃ có; bảng rỗng vì endpoint
-- /symbol-change trả 402 (paid_required) trên gói FMP hiện tại (enabled: false).
-- Điều kiện mở khoá: nâng gói FMP rồi chạy lại landing + dbt build.

with symbol_change_src as (
    select
        old_symbol,
        new_symbol,
        change_date,
        company_name,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_symbol_change') }}
    where old_symbol is not null
      and new_symbol is not null
      and change_date is not null
),

{{ bitemporal_version_chain(
    source_cte='symbol_change_src',
    key_cols=['old_symbol', 'new_symbol', 'change_date'],
    fingerprint_cols=['company_name'],
    first_known_from_expr='change_date',
    pit_basis='observation'
) }}

select
    old_symbol,
    new_symbol,
    change_date,
    company_name,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned
