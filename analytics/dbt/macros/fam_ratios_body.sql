{% macro fam_ratios_body(fundamental_rel, ttm_rel, price_rel, analyst_rel, earnings_rel, group_rel) %}

with base as (
    select
        f.date, f.symbol,
        f.short_term_debt,
        f.long_term_debt,
        f.cash_equivalents,
        f.common_equity,
        f.shares_basic,
        f.revenue,
        f.gross_profit,
        f.operating_income,
        f.ebitda,
        f.net_income,
        f.net_income_common,
        f.cfo,
        f.capex,
        f.free_cash_flow, f.depreciation_cf, f.stock_based_comp,
        f.rd_expense,
        f.sga_expense,
        f.income_tax,
        f.pretax_income,
        f.total_equity,
        f.total_assets,
        f.current_assets,
        f.current_liabilities,
        f.inventories,
        f.retained_earnings,
        f.total_liabilities,
        t.eps_diluted_ttm,
        t.revenue_ttm,
        t.ebitda_ttm,
        t.ebit_ttm,
        t.free_cash_flow_ttm,
        t.cfo_ttm,
        t.net_income_ttm,
        t.gross_profit_ttm,
        t.avg_common_equity,
        t.avg_total_assets,
        t.avg_invested_capital,
        t.avg_capital_employed,
        t.interest_expense_ttm,
        t.cash_only,
        t.maintenance_capex_ttm,
        t.cogs_ttm, t.avg_inventories, t.avg_receivables, t.avg_accounts_payable, t.avg_pp_and_e,
        t.revenue_p0, t.revenue_p1, t.revenue_p4,
        t.gross_profit_p0, t.gross_profit_p4,
        t.net_income_p0, t.net_income_p4,
        t.eps_diluted_p0, t.eps_diluted_p4,
        t.ebitda_p0, t.ebitda_p4,
        t.free_cash_flow_p0, t.free_cash_flow_p4,
        t.total_assets_p0, t.total_assets_p4,
        t.shares_diluted_p0, t.shares_diluted_p4,
        p.close,
        a.eps_estimate_next_y, a.rating_strong_buy, a.rating_buy, a.rating_hold,
        a.rating_sell, a.rating_strong_sell, a.num_analysts_rating,
        e.earnings_surprise_pct, e.earnings_surprise_sign, e.revenue_surprise_pct,
        e.standardized_unexpected_earnings,
        g.vendor_sector,
        g.employee_count
    from {{ fundamental_rel }} f
    left join {{ ttm_rel }}      t on t.date = f.date and t.symbol = f.symbol
    left join {{ price_rel }}    p on p.date = f.date and p.symbol = f.symbol
    left join {{ analyst_rel }}  a on a.date = f.date and a.symbol = f.symbol
    left join {{ earnings_rel }} e on e.date = f.date and e.symbol = f.symbol
    left join {{ group_rel }}    g on g.date = f.date and g.symbol = f.symbol
),

with_mcap as (
    -- market_cap = close * shares_basic (đợt 3c, spec §3.4.2).
    -- shares_basic = weighted_average_shs_out: BÌNH QUÂN GIA QUYỀN trong kỳ, không
    -- phải cổ phiếu lưu hành cuối kỳ. Chọn nó vì đo được: khớp vendor p50 = 0,0
    -- (max lệch 0,354%) trên 315 dòng, và phủ 1.360 dòng thay vì 4. Nợ N-q.
    select *, close * shares_basic as market_cap
    from base
),

derived as (
    select
        *,
        short_term_debt + long_term_debt                        as __total_debt,
        short_term_debt + long_term_debt - cash_equivalents      as __net_debt,
        market_cap + short_term_debt + long_term_debt - cash_equivalents as __ev
    from with_mcap
)

select
    date, symbol,

    -- ===== B2: Valuation (14 field) =====
    -- Toàn nhóm dùng close CHƯA điều chỉnh (spec §3.1). 4 tỷ số EV chia bằng
    -- enterprise_value, KHÔNG phải market_cap -- đó là toàn bộ điểm khác biệt giữa
    -- hai họ bội số này.
    market_cap,
    -- BRAIN tính cap bằng TRIỆU (Available Market Data). Giữ market_cap ở đơn vị USD
    -- cho mọi consumer hiện có; cap là cột riêng vì nó khác GIÁ TRỊ, không chỉ khác tên.
    market_cap / 1e6                                                 as cap,
    {{ safe_div('close', 'eps_diluted_ttm', mode='positive') }}      as pe_ratio,
    {{ safe_div('close', 'eps_estimate_next_y') }}                   as pe_ratio_fwd,
    {{ safe_div('market_cap', 'common_equity', mode='positive') }}   as pb_ratio,
    {{ safe_div('market_cap', 'revenue_ttm') }}                      as ps_ratio,
    __total_debt                                                     as total_debt,
    __net_debt                                                       as net_debt,
    __ev                                                             as enterprise_value,
    {{ safe_div('__ev', 'ebitda_ttm', mode='positive') }}            as ev_ebitda,
    {{ safe_div('__ev', 'ebit_ttm') }}                               as ev_ebit,
    {{ safe_div('__ev', 'revenue_ttm') }}                            as ev_sales,
    {{ safe_div('__ev', 'free_cash_flow_ttm') }}                     as ev_fcf,
    {{ safe_div('market_cap', 'free_cash_flow_ttm') }}               as price_to_fcf,
    {{ safe_div('market_cap', 'cfo_ttm') }}                          as price_to_cfo,
    {{ safe_div('common_equity', 'shares_basic') }}               as book_value_per_share,

    -- ===== B3: Profitability (10 field) =====
    {{ safe_div('gross_profit', 'revenue') }}                        as gross_margin,
    {{ safe_div('operating_income', 'revenue') }}                    as operating_margin,
    {{ safe_div('ebitda', 'revenue') }}                              as ebitda_margin,
    {{ safe_div('net_income', 'revenue') }}                          as net_margin,
    {{ safe_div('net_income_common', 'revenue') }}                   as net_margin_common,
    -- capex FMP trả giá trị ÂM -> dùng phép CỘNG khi muốn trừ dòng tiền ra (quy ước 5.0.1)
    {{ safe_div('cfo + capex', 'revenue') }}                         as fcf_margin,
    {{ safe_div('rd_expense', 'revenue') }}                          as rd_intensity,
    {{ safe_div('sga_expense', 'revenue') }}                         as sga_intensity,
    -- clamp [0,1]; pretax_income <= 0 -> 0.21 (thuế suất luật định Mỹ), 2026-07-28 :749
    case when pretax_income > 0
         then least(greatest(income_tax / pretax_income, 0), 1)
         else 0.21 end                                               as tax_rate_effective,
    {{ safe_div('pretax_income', 'operating_income') }}              as interest_burden,

    -- ===== B4: Return on Capital (7 field) =====
    {{ safe_div('net_income_ttm', 'avg_common_equity') }}            as roe,
    {{ safe_div('net_income_ttm', 'avg_total_assets') }}             as roa,
    -- nopat dùng ebit_ttm, KHÔNG dùng operating_income (spec §3.4)
    ebit_ttm * (1 - case when pretax_income > 0
                         then least(greatest(income_tax / pretax_income, 0), 1)
                         else 0.21 end)                              as nopat,
    total_equity + __total_debt - cash_equivalents                   as invested_capital,
    {{ safe_div('ebit_ttm * (1 - case when pretax_income > 0 then least(greatest(income_tax / pretax_income, 0), 1) else 0.21 end)',
                'avg_invested_capital') }}                           as roic,
    {{ safe_div('ebit_ttm', 'avg_capital_employed') }}               as roce,
    {{ safe_div('gross_profit_ttm', 'avg_total_assets') }}           as gross_profit_on_assets,

    -- ===== B5: Leverage & Solvency (7 field) =====
    {{ safe_div('__total_debt', 'common_equity') }}                  as debt_to_equity,
    {{ safe_div('__total_debt', 'total_assets') }}                   as debt_to_assets,
    {{ safe_div('__net_debt', 'ebitda_ttm') }}                       as net_debt_to_ebitda,
    {{ safe_div('__net_debt', 'common_equity') }}                    as net_debt_to_equity,
    {{ safe_div('ebit_ttm', 'abs(interest_expense_ttm)', mode='positive') }} as interest_coverage,
    {{ safe_div('total_assets', 'common_equity') }}                  as equity_multiplier,
    -- Altman (1968) 5-factor dựng cho công ty PHI TÀI CHÍNH; với ngân hàng Z-score là
    -- con số vô nghĩa chứ không phải "rủi ro cao" (2026-07-28 :772).
    -- X3/X5 phải dùng _ttm -- mất hậu tố sẽ làm hai số hạng nhỏ đi ~4 lần và MỌI
    -- Z-score sai.
    case when vendor_sector = 'Financial Services' then null else
        1.2 * {{ safe_div('current_assets - current_liabilities', 'total_assets') }}
      + 1.4 * {{ safe_div('retained_earnings', 'total_assets') }}
      + 3.3 * {{ safe_div('ebit_ttm', 'total_assets') }}
      + 0.6 * {{ safe_div('market_cap', 'total_liabilities') }}
      + 1.0 * {{ safe_div('revenue_ttm', 'total_assets') }}
    end                                                              as altman_z_score,

    -- ===== B6: Liquidity (5 field) =====
    {{ safe_div('current_assets', 'current_liabilities') }}          as current_ratio,
    {{ safe_div('current_assets - inventories', 'current_liabilities') }} as quick_ratio,
    -- cash_only, KHÔNG dùng cash_equivalents: cột đó là cash + short_term_investments
    -- (__fam_fundamental.sql:104), tức nới lỏng đúng cái tỷ số này đo (spec §3.4b).
    {{ safe_div('cash_only', 'current_liabilities') }}               as cash_ratio,
    current_assets - current_liabilities                             as working_capital,
    {{ safe_div('current_assets - current_liabilities', 'total_assets') }} as working_capital_to_assets,

    -- ===== B7: Efficiency (9 field) =====
    {{ safe_div('revenue_ttm', 'avg_total_assets') }}                as asset_turnover,
    {{ safe_div('cogs_ttm', 'avg_inventories') }}                    as inventory_turnover,
    {{ safe_div('revenue_ttm', 'avg_receivables') }}                 as receivables_turnover,
    {{ safe_div('cogs_ttm', 'avg_accounts_payable') }}               as payables_turnover,
    {{ safe_div('365', 'cogs_ttm / nullif(avg_inventories, 0)') }}   as days_inventory_outstanding,
    {{ safe_div('365', 'revenue_ttm / nullif(avg_receivables, 0)') }} as days_sales_outstanding,
    {{ safe_div('365', 'cogs_ttm / nullif(avg_accounts_payable, 0)') }} as days_payable_outstanding,
    {{ safe_div('365', 'revenue_ttm / nullif(avg_receivables, 0)') }}
      + {{ safe_div('365', 'cogs_ttm / nullif(avg_inventories, 0)') }}
      - {{ safe_div('365', 'cogs_ttm / nullif(avg_accounts_payable, 0)') }} as cash_conversion_cycle,
    {{ safe_div('revenue_ttm', 'avg_pp_and_e') }}                    as fixed_asset_turnover,

    -- ===== B8: Growth Rates (9 field) =====
    -- So GIÁ TRỊ KỲ, không so TTM (spec §3.3): catalog ghi rõ "vs. same quarter last
    -- year" / "vs. prior quarter". So quý-với-quý giữ được tính mùa vụ mà một cửa sổ
    -- TTM trượt sẽ làm mượt mất -- chính tín hiệu nhóm này sinh ra để bắt.
    {{ safe_div('revenue_p0', 'revenue_p4') }} - 1                   as revenue_growth_yoy,
    {{ safe_div('revenue_p0', 'revenue_p1') }} - 1                   as revenue_growth_qoq,
    {{ safe_div('gross_profit_p0', 'gross_profit_p4') }} - 1         as gross_profit_growth_yoy,
    {{ safe_div('net_income_p0', 'net_income_p4') }} - 1             as net_income_growth_yoy,
    {{ safe_div('eps_diluted_p0', 'eps_diluted_p4') }} - 1           as eps_growth_yoy,
    {{ safe_div('ebitda_p0', 'ebitda_p4') }} - 1                     as ebitda_growth_yoy,
    {{ safe_div('free_cash_flow_p0', 'free_cash_flow_p4') }} - 1     as fcf_growth_yoy,
    {{ safe_div('total_assets_p0', 'total_assets_p4') }} - 1         as total_assets_growth_yoy,
    {{ safe_div('shares_diluted_p0', 'shares_diluted_p4') }} - 1     as shares_growth_yoy,

    -- ===== B9: Cash Flow Quality (6 field) =====
    -- 3 field dùng sign_aware: tử và mẫu CÙNG ÂM cho ra số dương giả
    -- (-20 / -10 = 2 đọc thành "chất lượng gấp đôi") -- spec §5.1.
    {{ safe_div('cfo', 'net_income', mode='sign_aware') }}           as cfo_to_net_income,
    {{ safe_div('net_income_ttm - cfo_ttm', 'avg_total_assets') }}   as accruals_ratio,
    {{ safe_div('free_cash_flow', 'net_income', mode='sign_aware') }} as fcf_to_earnings,
    {{ safe_div('abs(capex)', 'revenue') }}                          as capex_to_revenue,
    {{ safe_div('abs(capex)', 'depreciation_cf', mode='sign_aware') }} as capex_to_depreciation,
    {{ safe_div('stock_based_comp', 'revenue') }}                    as stock_comp_to_revenue,
    cfo_ttm - maintenance_capex_ttm                                  as owner_earnings,

    -- ===== B10: chuyển tiếp từ __fam_earnings (Task 9) =====
    earnings_surprise_pct,
    earnings_surprise_sign,
    revenue_surprise_pct,
    standardized_unexpected_earnings,

    -- ===== B11: Analyst (1 field; 4 field còn lại là gap) =====
    {{ safe_div('rating_strong_buy * 2 + rating_buy * 1 + rating_hold * 0 + rating_sell * -1 + rating_strong_sell * -2',
                'num_analysts_rating') }}                            as analyst_consensus_score,

    {{ safe_div('revenue_ttm', 'employee_count') }}                  as revenue_per_employee

from derived

{% endmacro %}
