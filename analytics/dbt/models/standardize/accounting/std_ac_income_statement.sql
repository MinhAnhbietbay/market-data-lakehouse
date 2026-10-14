{{
    config(
        materialized='table',
        partition_by=['years(period_end_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ac_income_statement',
        tags=['layer::standardize', 'domain::accounting']
    )
}}

with income_statement_src as (
    select
        symbol,
        cik,
        fiscal_year,
        period,
        period_end_date,
        reported_currency,
        filing_date,
        accepted_date,
        revenue,
        cost_of_revenue,
        research_and_development_expenses,
        general_and_administrative_expenses,
        selling_and_marketing_expenses,
        selling_general_and_administrative_expenses,
        other_expenses,
        interest_income,
        interest_expense,
        depreciation_and_amortization,
        non_operating_income_excluding_interest,
        income_before_tax,
        income_tax_expense,
        net_income_from_discontinued_operations,
        other_adjustments_to_net_income,
        net_income,
        net_income_deductions,
        weighted_average_shs_out,
        weighted_average_shs_out_dil,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_income_statement') }}
    where symbol is not null and fiscal_year is not null and period is not null
),

-- accepted_date là ngày SEC chấp nhận hồ sơ: đây là "biết từ lúc nào" thật sự,
-- tốt hơn ngày ta quan sát. filing_date là dự phòng khi accepted_date null.
-- accepted_date là timestamp(6) with time zone ở raw -- cast về date vì
-- known_from bắt buộc kiểu date (global constraint).
-- F1 (2026-08-23): fingerprint TRƯỚC ĐÂY chỉ có 5 cột headline trong khi
-- select cuối phơi 30. 19 cột rơi ra ngoài -> một run mà cost_of_revenue
-- đổi còn 5 cột kia không đổi thì change-point bị lọc bỏ, giá trị mới KHÔNG
-- BAO GIỜ tồn tại và giá trị cũ đóng băng vĩnh viễn. Không lỗi, không log.
-- Nay fingerprint mọi cột nghiệp vụ; giá phải trả là nhiều version hơn,
-- đúng thứ một PIT store nên trả.
{{ bitemporal_version_chain(
    source_cte='income_statement_src',
    key_cols=['symbol', 'fiscal_year', 'period'],
    fingerprint_cols=[
        'period_end_date',
        'reported_currency',
        'cik',
        'filing_date',
        'accepted_date',
        'revenue',
        'cost_of_revenue',
        'research_and_development_expenses',
        'general_and_administrative_expenses',
        'selling_and_marketing_expenses',
        'selling_general_and_administrative_expenses',
        'other_expenses',
        'interest_income',
        'interest_expense',
        'depreciation_and_amortization',
        'non_operating_income_excluding_interest',
        'income_before_tax',
        'income_tax_expense',
        'net_income_from_discontinued_operations',
        'other_adjustments_to_net_income',
        'net_income',
        'net_income_deductions',
        'weighted_average_shs_out',
        'weighted_average_shs_out_dil'
    ],
    first_known_from_expr='coalesce(cast(accepted_date as date), filing_date)',
    pit_basis='filing'
) }}

select
    symbol,
    cik,
    fiscal_year,
    period,
    period_end_date,
    reported_currency,
    filing_date,
    accepted_date,
    revenue,
    cost_of_revenue,
    research_and_development_expenses,
    general_and_administrative_expenses,
    selling_and_marketing_expenses,
    selling_general_and_administrative_expenses,
    other_expenses,
    interest_income,
    interest_expense,
    depreciation_and_amortization,
    non_operating_income_excluding_interest,
    income_before_tax,
    income_tax_expense,
    net_income_from_discontinued_operations,
    other_adjustments_to_net_income,
    net_income,
    net_income_deductions,
    weighted_average_shs_out,
    weighted_average_shs_out_dil,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

