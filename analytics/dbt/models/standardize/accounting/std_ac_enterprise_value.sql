{{
    config(
        materialized='table',
        partition_by=['years(period_end_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ac_enterprise_value',
        tags=['layer::standardize', 'domain::accounting']
    )
}}

with enterprise_values_ranked as (
    select
        symbol,
        period_end_date,
        stock_price,
        number_of_shares,
        minus_cash_and_cash_equivalents,
        add_total_debt,
        run_date,
        landing_key,
        row_number() over (
            partition by symbol, period_end_date, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_enterprise_values') }}
    where symbol is not null and period_end_date is not null
),

enterprise_values_latest as (
    select
        symbol,
        period_end_date,
        stock_price,
        number_of_shares,
        minus_cash_and_cash_equivalents,
        add_total_debt,
        run_date,
        landing_key
    from enterprise_values_ranked
    where rn = 1
),

-- Mượn ngày nộp thật từ bảng anh em cùng nguồn báo cáo. enterprise_value chỉ có
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
    from enterprise_values_latest e
    left join filing_dates f
      on f.symbol = e.symbol and f.period_end_date = e.period_end_date
),

{{ bitemporal_version_chain(
    source_cte='with_filing',
    key_cols=['symbol', 'period_end_date'],
    fingerprint_cols=['stock_price', 'number_of_shares', 'minus_cash_and_cash_equivalents', 'add_total_debt'],
    first_known_from_expr='anchor_filing_date',
    pit_basis='filing'
) }}

select
    symbol,
    period_end_date,
    stock_price,
    number_of_shares,
    minus_cash_and_cash_equivalents,
    add_total_debt,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned
