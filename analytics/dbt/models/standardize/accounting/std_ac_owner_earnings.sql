{{
    config(
        materialized='table',
        partition_by=['years(period_end_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ac_owner_earnings',
        tags=['layer::standardize', 'domain::accounting']
    )
}}

with owner_earnings_src as (
    select
        symbol,
        fiscal_year,
        period,
        period_end_date,
        reported_currency,
        maintenance_capex,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_owner_earnings') }}
    where symbol is not null and fiscal_year is not null and period is not null
),

-- Mượn ngày nộp thật từ bảng anh em cùng nguồn báo cáo. owner_earnings chỉ có
-- period_end_date; neo vào đó là lookahead 30-45 ngày (spec §3.2). Khi kỳ tương
-- ứng không có trong income_statement, lùi về period_end + FILING_LAG_DAYS --
-- hằng số dẫn từ số đo ở docs/superpowers/plans/measurements/2026-08-30-filing-lag.md,
-- KHÔNG phải một con số tròn chọn bừa.
filing_dates as (
    select distinct symbol, period_end_date, filing_date
    from {{ ref('std_ac_income_statement') }}
    where filing_date is not null
),

with_filing as (
    select
        e.*,
        coalesce(
            f.filing_date,
            date_add(e.period_end_date, {{ var('filing_lag_days') }})
        ) as anchor_filing_date
    from owner_earnings_src e
    left join filing_dates f
      on f.symbol = e.symbol and f.period_end_date = e.period_end_date
),

{{ bitemporal_version_chain(
    source_cte='with_filing',
    key_cols=['symbol', 'fiscal_year', 'period'],
    fingerprint_cols=['period_end_date', 'reported_currency', 'maintenance_capex'],
    first_known_from_expr='anchor_filing_date',
    pit_basis='filing'
) }}

select
    symbol,
    fiscal_year,
    period,
    period_end_date,
    reported_currency,
    maintenance_capex,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned




