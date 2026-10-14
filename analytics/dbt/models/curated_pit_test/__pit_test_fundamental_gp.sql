{{
    config(
        materialized='table',
        file_format='iceberg',
        schema='curated_pit_test',
        alias='__pit_test_fundamental_gp',
        tags=['layer::curated_pit_test', 'test']
    )
}}

with income as (
    {{ collapse_asof(
        source_rel=ref('pit_seed_income_statement'),
        key_cols=['symbol'],
        spine_rel=ref('pit_seed_income_spine'),
        shape='A',
        max_staleness_days=180,
        period_col='period_end_date'
    ) }}
)

select
    date,
    symbol,
    revenue - cost_of_revenue                                       as gross_profit,
    -- interest_expense và income_tax_expense phải giữ NGUYÊN DẤU -- chúng có thể
    -- âm thật (thu lãi > chi lãi, tax benefit), và công thức SEC cộng ngược
    -- chúng vào net income. Bọc abs() sẽ làm EBITDA lệch gấp đôi khoản đó.
    net_income + interest_expense + income_tax_expense
        + depreciation_and_amortization                             as ebitda,
    (net_income - net_income_deductions)
        / weighted_average_shs_out                                  as eps_basic,
    (net_income - net_income_deductions)
        / weighted_average_shs_out_dil                              as eps_diluted
from income
