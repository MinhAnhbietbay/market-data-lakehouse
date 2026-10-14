{{
    config(
        materialized='table',
        partition_by=['months(date)'],
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_fundamental',
        tags=['layer::curated', 'internal']
    )
}}

-- Cả ba báo cáo đều period: quarter (data_sources.yaml:183, :196, :209) nên
-- đồng nhất loại kỳ. KHÔNG trộn nguồn khác loại kỳ vào model này -- nếu ai đó
-- flip một endpoint sang annual để kéo sâu lịch sử thì tử số quarterly sẽ nằm
-- trên mẫu số annual mà không có gì báo.

with income as (
    {{ collapse_asof(
        source_rel=ref('std_ac_income_statement'),
        key_cols=['symbol'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=180,
        period_col='period_end_date'
    ) }}
),
balance as (
    {{ collapse_asof(
        source_rel=ref('std_ac_balance_sheet_statement'),
        key_cols=['symbol'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=180,
        period_col='period_end_date'
    ) }}
),
cashflow as (
    {{ collapse_asof(
        source_rel=ref('std_ac_cash_flow_statement'),
        key_cols=['symbol'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=180,
        period_col='period_end_date'
    ) }}
),
shares as (
    {{ collapse_asof(
        source_rel=ref('std_pd_share_count_observation'),
        key_cols=['symbol'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=180
    ) }}
),
owner_earnings as (
    {{ collapse_asof(
        source_rel=ref('std_ac_owner_earnings'),
        key_cols=['symbol'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=180,
        period_col='period_end_date'
    ) }}
)

select
    s.date,
    s.symbol,

    -- =========================================================================
    -- Income Statement (17 fields: 11 passthrough, 6 derived)
    -- =========================================================================
    i.revenue,
    i.cost_of_revenue                                               as cogs,
    i.revenue - i.cost_of_revenue                                   as gross_profit,
    i.research_and_development_expenses                             as rd_expense,
    i.selling_general_and_administrative_expenses                   as sga_expense,
    i.revenue - i.cost_of_revenue - (
        i.selling_general_and_administrative_expenses
        + i.research_and_development_expenses
        + i.other_expenses
    )                                                               as operating_income,
    i.interest_expense,
    i.income_before_tax                                             as pretax_income,
    i.income_tax_expense                                            as income_tax,
    i.net_income,
    i.net_income - i.net_income_deductions                          as net_income_common,
    -- interest_expense và income_tax_expense phải giữ NGUYÊN DẤU -- chúng có thể
    -- âm thật (thu lãi > chi lãi, tax benefit), và công thức SEC cộng ngược
    -- chúng vào net income. Bọc abs() sẽ làm EBITDA lệch gấp đôi khoản đó.
    i.net_income + i.interest_expense + i.income_tax_expense
        + i.depreciation_and_amortization                           as ebitda,
    i.depreciation_and_amortization                                 as depreciation,
    (i.net_income - i.net_income_deductions)
        / i.weighted_average_shs_out                                as eps_basic,
    (i.net_income - i.net_income_deductions)
        / i.weighted_average_shs_out_dil                            as eps_diluted,
    i.weighted_average_shs_out                                      as shares_basic,
    i.weighted_average_shs_out_dil                                  as shares_diluted,

    -- =========================================================================
    -- Balance Sheet (20 fields: 12 passthrough, 8 derived -- includes shares_outstanding,
    -- float_shares từ std_pd_share_count_observation, không phải từ báo cáo tài chính)
    -- =========================================================================
    b.cash_and_cash_equivalents + b.short_term_investments
        + b.net_receivables + b.inventory + b.prepaids
        + b.other_current_assets + b.property_plant_equipment_net
        + b.goodwill + b.intangible_assets + b.long_term_investments
        + b.tax_assets + b.other_non_current_assets                 as total_assets,
    b.cash_and_cash_equivalents + b.short_term_investments
        + b.net_receivables + b.inventory + b.prepaids
        + b.other_current_assets                                    as current_assets,
    b.cash_and_cash_equivalents + b.short_term_investments          as cash_equivalents,
    b.net_receivables                                               as receivables,
    b.inventory                                                     as inventories,
    b.other_current_assets,
    b.property_plant_equipment_net + b.goodwill
        + b.intangible_assets + b.long_term_investments
        + b.tax_assets + b.other_non_current_assets                 as non_current_assets,
    b.property_plant_equipment_net                                  as pp_and_e,
    b.intangible_assets                                             as intangibles,
    b.goodwill,
    -- tax_payables KHÔNG cộng riêng: FMP's total_liabilities/total_current_liabilities không tính nó
    -- như một khoản mục độc lập -- xác nhận bằng đối chiếu vendor (spec §7.3, Task 19), formula
    -- trước đây cộng thừa đúng bằng giá trị tax_payables trên mọi dòng lệch.
    -- capital_lease_obligations (không có _current) là TỔNG lease (current+non-current) theo FMP,
    -- nên total_liabilities không được cộng thêm capital_lease_obligations_current lần nữa -- current_
    -- liabilities thì được, vì đó là nơi DUY NHẤT nó xuất hiện ở mức current.
    b.account_payables + b.other_payables + b.accrued_expenses
        + b.short_term_debt
        + b.deferred_revenue
        + b.other_current_liabilities + b.long_term_debt
        + b.deferred_revenue_non_current
        + b.deferred_tax_liabilities_non_current
        + b.other_non_current_liabilities + b.other_liabilities
        + b.capital_lease_obligations                               as total_liabilities,
    b.account_payables + b.other_payables + b.accrued_expenses
        + b.short_term_debt + b.capital_lease_obligations_current
        + b.deferred_revenue
        + b.other_current_liabilities                               as current_liabilities,
    b.account_payables                                              as accounts_payable,
    b.short_term_debt,
    b.long_term_debt,
    -- treasury_stock đã là số ÂM trong raw (contra-equity, FMP tự mã hoá dấu) -- CỘNG, không trừ.
    -- Trừ một số đã âm sẽ lật dấu, cộng thừa gấp đôi giá trị treasury_stock vào equity. Xác nhận:
    -- không dòng nào trong raw_fmp_balance_sheet có treasury_stock > 0.
    b.common_stock + b.preferred_stock
        + b.additional_paid_in_capital + b.retained_earnings
        + b.accumulated_other_comprehensive_income_loss
        + b.other_total_stockholders_equity
        + b.minority_interest + b.treasury_stock                    as total_equity,
    b.common_stock + b.additional_paid_in_capital
        + b.retained_earnings
        + b.accumulated_other_comprehensive_income_loss
        + b.other_total_stockholders_equity + b.treasury_stock      as common_equity,
    b.retained_earnings,
    sh.outstanding_shares                                           as shares_outstanding,
    sh.float_shares,

    -- =========================================================================
    -- Cash Flow Statement (12 fields: 9 passthrough, 3 derived)
    -- =========================================================================
    c.operating_cash_flow                                           as cfo,
    c.net_cash_provided_by_investing_activities                     as cfi,
    c.net_cash_provided_by_financing_activities                     as cff,
    c.capital_expenditure                                           as capex,
    oe.maintenance_capex,
    c.depreciation_and_amortization_cash_flow                       as depreciation_cf,
    c.stock_based_compensation                                      as stock_based_comp,
    c.change_in_working_capital                                      as changes_working_capital,
    c.common_dividends_paid + c.preferred_dividends_paid            as dividends_paid,
    c.common_stock_repurchased                                      as share_repurchases,
    c.operating_cash_flow
        + c.net_cash_provided_by_investing_activities
        + c.net_cash_provided_by_financing_activities
        + c.effect_of_forex_changes_on_cash                         as net_change_cash,
    c.operating_cash_flow - abs(c.capital_expenditure)              as free_cash_flow,

    greatest(i.known_from, b.known_from, c.known_from, sh.known_from, oe.known_from) as known_from,
    current_timestamp                                               as loaded_at
from {{ ref('__spine_daily') }} s
left join income         i  on i.date = s.date and i.symbol = s.symbol
left join balance        b  on b.date = s.date and b.symbol = s.symbol
left join cashflow       c  on c.date = s.date and c.symbol = s.symbol
left join shares         sh on sh.date = s.date and sh.symbol = s.symbol
left join owner_earnings oe on oe.date = s.date and oe.symbol = s.symbol
