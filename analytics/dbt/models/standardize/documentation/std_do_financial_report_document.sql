{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_do_financial_report_document',
        tags=['layer::standardize', 'domain::documentation']
    )
}}

with financial_reports_dates_ranked as (
    select
        symbol,
        fiscal_year,
        period,
        link_xlsx,
        link_json,
        run_date,
        landing_key,
        row_number() over (
            partition by symbol, fiscal_year, period, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_financial_reports_dates') }}
    where symbol is not null and fiscal_year is not null and period is not null
),

financial_reports_dates_latest as (
    select
        symbol,
        fiscal_year,
        period,
        link_xlsx,
        link_json,
        run_date,
        landing_key
    from financial_reports_dates_ranked
    where rn = 1
),

with_period_anchor as (
    -- Không có cột ngày nào trong nguồn; dẫn ngày kết kỳ từ (fiscal_year, period)
    -- rồi cộng độ trễ nộp đã đo (Task 6). Nhãn assumed_filing_lag vì đây là suy
    -- diễn hai lớp, không phải ngày quan sát được.
    select
        d.*,
        date_add(
            case period
                when 'Q1' then date(cast(fiscal_year as string) || '-03-31')
                when 'Q2' then date(cast(fiscal_year as string) || '-06-30')
                when 'Q3' then date(cast(fiscal_year as string) || '-09-30')
                when 'Q4' then date(cast(fiscal_year as string) || '-12-31')
                when 'FY'  then date(cast(fiscal_year as string) || '-12-31')
            end,
            {{ var('filing_lag_days') }}
        ) as anchor_date
    from financial_reports_dates_latest d
),

{{ bitemporal_version_chain(
    source_cte='with_period_anchor',
    key_cols=['symbol', 'fiscal_year', 'period'],
    fingerprint_cols=['link_xlsx', 'link_json'],
    first_known_from_expr='anchor_date',
    pit_basis='filing',
    first_known_from_basis_label='assumed_filing_lag'
) }}

select
    symbol,
    fiscal_year,
    period,
    link_xlsx,
    link_json,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned




