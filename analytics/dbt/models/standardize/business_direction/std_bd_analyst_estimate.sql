{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_bd_analyst_estimate',
        tags=['layer::standardize', 'domain::business_direction']
    )
}}

with estimates_ranked as (
    select
        symbol,
        estimate_period_end_date,
        revenue_low,
        revenue_high,
        revenue_avg,
        ebitda_low,
        ebitda_high,
        ebitda_avg,
        ebit_low,
        ebit_high,
        ebit_avg,
        net_income_low,
        net_income_high,
        net_income_avg,
        sga_expense_low,
        sga_expense_high,
        sga_expense_avg,
        eps_avg,
        eps_high,
        eps_low,
        num_analysts_revenue,
        num_analysts_eps,
        run_date,
        landing_key,
        row_number() over (
            partition by symbol, estimate_period_end_date, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_analyst_estimates') }}
    where symbol is not null and estimate_period_end_date is not null
),

estimates_latest as (
    select
        symbol,
        estimate_period_end_date,
        revenue_low,
        revenue_high,
        revenue_avg,
        ebitda_low,
        ebitda_high,
        ebitda_avg,
        ebit_low,
        ebit_high,
        ebit_avg,
        net_income_low,
        net_income_high,
        net_income_avg,
        sga_expense_low,
        sga_expense_high,
        sga_expense_avg,
        eps_avg,
        eps_high,
        eps_low,
        num_analysts_revenue,
        num_analysts_eps,
        run_date,
        landing_key
    from estimates_ranked
    where rn = 1
),

with_lag_anchor as (
    -- Không cùng nguồn báo cáo với std_ac_income_statement nên không mượn được
    -- filing_date. Lag dẫn từ số đo (Task 6), không phải số tròn chọn bừa.
    select
        e.*,
        date_add(e.estimate_period_end_date, {{ var('filing_lag_days') }}) as anchor_date
    from estimates_latest e
),

{{ bitemporal_version_chain(
    source_cte='with_lag_anchor',
    key_cols=['symbol', 'estimate_period_end_date'],
    fingerprint_cols=[
        'revenue_low', 'revenue_high', 'revenue_avg',
        'ebitda_low', 'ebitda_high', 'ebitda_avg',
        'ebit_low', 'ebit_high', 'ebit_avg',
        'net_income_low', 'net_income_high', 'net_income_avg',
        'sga_expense_low', 'sga_expense_high', 'sga_expense_avg',
        'eps_avg', 'eps_high', 'eps_low',
        'num_analysts_revenue', 'num_analysts_eps'
    ],
    first_known_from_expr='anchor_date',
    pit_basis='observation',
    first_known_from_basis_label='assumed_filing_lag'
) }}

select
    symbol,
    estimate_period_end_date,
    revenue_low,
    revenue_high,
    revenue_avg,
    ebitda_low,
    ebitda_high,
    ebitda_avg,
    ebit_low,
    ebit_high,
    ebit_avg,
    net_income_low,
    net_income_high,
    net_income_avg,
    sga_expense_low,
    sga_expense_high,
    sga_expense_avg,
    eps_avg,
    eps_high,
    eps_low,
    num_analysts_revenue,
    num_analysts_eps,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned


