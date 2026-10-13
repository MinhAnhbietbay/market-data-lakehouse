{#
    18 đại lượng trung gian TTM/avg + các giá trị kỳ mà nhóm B8 cần.
    KHÔNG cái nào là field catalog -- chúng ở lại nội bộ, không ra cột curated
    (catalog_curated_guard.py:100-103 báo vi phạm cả chiều "cột thừa").

    Ngưỡng chặn khuyết kỳ KHÁC NHAU theo nhóm (spec §4.2). sum() của SQL vẫn trả
    tổng của 3 kỳ khi chỉ có 3 -- im lặng và sai, nên phải chặn tường minh.
#}
{% macro fam_ttm_body(income_rel, balance_rel, cashflow_rel, owner_earnings_rel=none) %}

with periods as (
    -- gộp 3 statement về một grain kỳ trước khi mở cửa sổ
    select
        i.symbol, i.period_end_date, i.period,
        greatest(i.known_from, b.known_from, c.known_from) as known_from,
        i.revenue, i.cost_of_revenue, i.interest_expense, i.income_before_tax,
        i.selling_general_and_administrative_expenses, i.research_and_development_expenses,
        i.other_expenses,
        i.income_tax_expense, i.net_income, i.net_income_deductions,
        i.depreciation_and_amortization,
        i.weighted_average_shs_out, i.weighted_average_shs_out_dil,
        -- total_assets/total_liabilities/total_equity KHÔNG tồn tại như cột output trên
        -- std_ac_balance_sheet_statement (bỏ ở commit a016178, 3NF) -- dẫn xuất từ thành
        -- phần con, công thức chép nguyên văn từ __fam_fundamental.sql:96-142 (cùng model
        -- nguồn, cùng lý do, khác nơi tiêu thụ).
        b.cash_and_cash_equivalents + b.short_term_investments
          + b.net_receivables + b.inventory + b.prepaids
          + b.other_current_assets + b.property_plant_equipment_net
          + b.goodwill + b.intangible_assets + b.long_term_investments
          + b.tax_assets + b.other_non_current_assets                 as total_assets,
        -- total_current_liabilities cũng không tồn tại thô -- dẫn xuất giống hệt
        -- current_liabilities của __fam_fundamental.sql:127-130.
        b.account_payables + b.other_payables + b.accrued_expenses
          + b.short_term_debt + b.capital_lease_obligations_current
          + b.deferred_revenue + b.other_current_liabilities         as total_current_liabilities,
        b.cash_and_cash_equivalents, b.short_term_investments, b.inventory,
        b.net_receivables, b.account_payables, b.property_plant_equipment_net,
        -- total_liabilities: CHÚ Ý công thức này KHÁC total_current_liabilities ở trên --
        -- không cộng tax_payables (FMP's total_liabilities không tính nó riêng, xác nhận
        -- qua đối chiếu vendor), dùng capital_lease_obligations (TỔNG, không phải _current).
        b.account_payables + b.other_payables + b.accrued_expenses
          + b.short_term_debt
          + b.deferred_revenue
          + b.other_current_liabilities + b.long_term_debt
          + b.deferred_revenue_non_current
          + b.deferred_tax_liabilities_non_current
          + b.other_non_current_liabilities + b.other_liabilities
          + b.capital_lease_obligations                               as total_liabilities,
        -- treasury_stock đã ÂM sẵn trong raw (contra-equity) -- CỘNG, không trừ.
        b.common_stock + b.preferred_stock
          + b.additional_paid_in_capital + b.retained_earnings
          + b.accumulated_other_comprehensive_income_loss
          + b.other_total_stockholders_equity
          + b.minority_interest + b.treasury_stock                    as total_equity,
        b.retained_earnings,
        b.short_term_debt, b.long_term_debt,
        c.operating_cash_flow as cfo,
        c.capital_expenditure, c.stock_based_compensation,
        c.depreciation_and_amortization_cash_flow as depreciation_cf,
        {% if owner_earnings_rel is not none -%}
        oe.maintenance_capex
        {%- else -%}
        cast(null as decimal(28, 4)) as maintenance_capex
        {%- endif %}
    from {{ income_rel }} i
    join {{ balance_rel }} b
      on b.symbol = i.symbol and b.period_end_date = i.period_end_date
     and b.period = i.period
    join {{ cashflow_rel }} c
      on c.symbol = i.symbol and c.period_end_date = i.period_end_date
     and c.period = i.period
    {% if owner_earnings_rel is not none -%}
    -- LEFT join, KHÔNG phải join thường: std_ac_owner_earnings chỉ có 25 dòng, còn
    -- periods trải toàn bộ lịch sử statement. Inner join sẽ cắt lịch sử TTM xuống
    -- 25 dòng và làm hỏng CẢ 72 ratio của đợt 2 -- kiểu hỏng lan rộng nhất đợt này.
    left join {{ owner_earnings_rel }} oe
      on oe.symbol = i.symbol and oe.period_end_date = i.period_end_date
     and oe.period = i.period
    {%- endif %}
    where i.period in ('Q1', 'Q2', 'Q3', 'Q4')
),

-- n = 5: đủ cho nhóm dòng chảy (P[0..3]), nhóm avg_* (cần P[4]) và nhóm B8 (cần P[4]).
-- Thừa một kỳ với nhóm dòng chảy, rẻ hơn nhiều so với dựng hai bảng.
windowed as (
    {{ period_window_body(
        source_rel='periods',
        key_col='symbol',
        axis_col='period_end_date',
        n=5
    ) }}
)

select
    symbol,
    __target_axis                                       as period_end_date,
    __as_of                                             as known_from,

    count(distinct case when __rank <= 3 then period_end_date end) as __n_flow,
    count(distinct period_end_date)                                as __n_all,

    -- 11 đại lượng dòng chảy: tổng 4 quý, NULL nếu khuyết kỳ
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then revenue end) end     as revenue_ttm,
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then cost_of_revenue end) end as cogs_ttm,
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then revenue - cost_of_revenue end) end as gross_profit_ttm,
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then net_income end) end   as net_income_ttm,
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then net_income - net_income_deductions end) end as net_income_common_ttm,
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then interest_expense end) end as interest_expense_ttm,
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then cfo end) end          as cfo_ttm,
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then cfo - abs(capital_expenditure) end) end as free_cash_flow_ttm,
    -- ebitda = net_income + interest_expense + income_tax + D&A. interest_expense và
    -- income_tax_expense giữ NGUYÊN DẤU (có thể âm thật) -- bọc abs() sẽ lệch gấp đôi.
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then net_income + interest_expense
                       + income_tax_expense + depreciation_and_amortization end) end as ebitda_ttm,
    -- ebit = pretax_income + interest_expense. KHÔNG gán = operating_income (spec §3.4).
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then income_before_tax + interest_expense end) end as ebit_ttm,
    -- operating_income = gross_profit - opex, dựng theo cùng cách __fam_fundamental làm
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3 then revenue - cost_of_revenue
                       - (selling_general_and_administrative_expenses
                          + research_and_development_expenses + other_expenses) end) end as operating_income_ttm,

    -- eps_diluted_ttm = TỔNG 4 quý (spec §3.2 -- vendor đã restate theo split).
    -- KHÔNG dùng net_income_ttm / shares[latest]: bất biến split nhưng sai lệch có hệ
    -- thống ở mọi mã có phát hành thêm/buyback trong năm.
    case when count(distinct case when __rank <= 3 then period_end_date end) = 4
         then sum(case when __rank <= 3
                       then (net_income - net_income_deductions) / nullif(weighted_average_shs_out_dil, 0) end) end as eps_diluted_ttm,

    -- Ngưỡng RIÊNG, đếm KỲ CÓ GIÁ TRỊ chứ không đếm kỳ có mặt. 11 đại lượng dòng
    -- chảy trên kia đếm được bằng kỳ vì cả 11 đến từ join INNER ba statement: có kỳ
    -- thì chắc chắn có giá trị. maintenance_capex qua LEFT join phá bất biến đó --
    -- 4 kỳ có mặt mà 1-2 kỳ có giá trị, và sum() bỏ qua NULL sẽ trả tổng 2 quý DÁN
    -- NHÃN TTM. Cùng họ với bug accruals_ratio đợt 2 (commit b8d22a4).
    case when count(distinct case when __rank <= 3 and maintenance_capex is not null
                                  then period_end_date end) = 4
         then sum(case when __rank <= 3 then maintenance_capex end) end as maintenance_capex_ttm,

    -- 7 đại lượng tồn kho: (X[P[0]] + X[P[4]]) / 2, cần ĐỦ 5 kỳ.
    -- P[3] là số dư cuối quý cũ nhất trong cửa sổ TTM -- mới cách P[0] 9 tháng.
    -- Dùng P[3] thay P[4] vẫn chạy, vẫn ra số, chỉ lệch một quý, và mọi chỉ tiêu
    -- turnover/return-on-capital lệch theo mà không có gì đỏ lên.
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then total_assets end)
             + max(case when __rank = 4 then total_assets end)) / 2 end as avg_total_assets,
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then total_equity - net_income_deductions end)
             + max(case when __rank = 4 then total_equity - net_income_deductions end)) / 2 end as avg_common_equity,
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then total_equity + short_term_debt + long_term_debt - (cash_and_cash_equivalents + short_term_investments) end)
             + max(case when __rank = 4 then total_equity + short_term_debt + long_term_debt - (cash_and_cash_equivalents + short_term_investments) end)) / 2 end as avg_invested_capital,
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then inventory end)
             + max(case when __rank = 4 then inventory end)) / 2 end as avg_inventories,
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then net_receivables end)
             + max(case when __rank = 4 then net_receivables end)) / 2 end as avg_receivables,
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then account_payables end)
             + max(case when __rank = 4 then account_payables end)) / 2 end as avg_accounts_payable,
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then property_plant_equipment_net end)
             + max(case when __rank = 4 then property_plant_equipment_net end)) / 2 end as avg_pp_and_e,
    case when count(distinct period_end_date) = 5
         then (max(case when __rank = 0 then total_assets - total_current_liabilities end)
             + max(case when __rank = 4 then total_assets - total_current_liabilities end)) / 2 end as avg_capital_employed,

    -- cash thuần cho cash_ratio: KHÔNG dùng cash_equivalents của curated, cột đó là
    -- cash_and_cash_equivalents + short_term_investments (__fam_fundamental.sql:104),
    -- tức nới lỏng đúng cái mà cash_ratio sinh ra để đo (spec §3.4b).
    max(case when __rank = 0 then cash_and_cash_equivalents end)    as cash_only,

    -- giá trị kỳ cho nhóm B8 (spec §3.3: B8 so giá trị KỲ, không so TTM)
    max(case when __rank = 0 then revenue end)                      as revenue_p0,
    max(case when __rank = 1 then revenue end)                      as revenue_p1,
    max(case when __rank = 4 then revenue end)                      as revenue_p4,
    max(case when __rank = 0 then revenue - cost_of_revenue end)    as gross_profit_p0,
    max(case when __rank = 4 then revenue - cost_of_revenue end)    as gross_profit_p4,
    max(case when __rank = 0 then net_income end)                   as net_income_p0,
    max(case when __rank = 4 then net_income end)                   as net_income_p4,
    max(case when __rank = 0 then (net_income - net_income_deductions) / nullif(weighted_average_shs_out_dil, 0) end) as eps_diluted_p0,
    max(case when __rank = 4 then (net_income - net_income_deductions) / nullif(weighted_average_shs_out_dil, 0) end) as eps_diluted_p4,
    max(case when __rank = 0 then net_income + interest_expense + income_tax_expense + depreciation_and_amortization end) as ebitda_p0,
    max(case when __rank = 4 then net_income + interest_expense + income_tax_expense + depreciation_and_amortization end) as ebitda_p4,
    max(case when __rank = 0 then cfo - abs(capital_expenditure) end) as free_cash_flow_p0,
    max(case when __rank = 4 then cfo - abs(capital_expenditure) end) as free_cash_flow_p4,
    max(case when __rank = 0 then total_assets end)                 as total_assets_p0,
    max(case when __rank = 4 then total_assets end)                 as total_assets_p4,
    max(case when __rank = 0 then weighted_average_shs_out_dil end) as shares_diluted_p0,
    max(case when __rank = 4 then weighted_average_shs_out_dil end) as shares_diluted_p4

from windowed
group by symbol, __target_axis, __as_of

{% endmacro %}
