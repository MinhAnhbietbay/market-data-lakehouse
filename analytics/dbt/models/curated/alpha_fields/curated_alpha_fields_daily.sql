{{
    config(materialized='table', partition_by=['months(date)'], file_format='iceberg',
           schema=var('env_curated'), alias='curated_alpha_fields_daily',
           tags=['layer::curated', 'engine_table'])
}}

-- Chỉ join 4 họ lên spine. KHÔNG có logic PIT nào ở đây -- toàn bộ nằm trong
-- collapse_asof và trong từng __fam_*. Nếu bạn thấy mình sắp viết một
-- `known_from <=` trong file này, tức là có thứ đặt sai chỗ.

-- Cắt delisting: lấy ngày sớm nhất trong std_ev_delisting_event -- nguồn CHUYÊN
-- BIỆT cho sự kiện huỷ niêm yết. Bất đối xứng có chủ đích -- để mã đã chết sinh
-- dữ liệu là lỗi lọt vào backtest, còn cắt sớm một ngày chỉ mất một quan sát.
-- Cắt theo mốc NGHIỆP VỤ (delisted_date) chứ không theo known_from của sự
-- kiện, nên tin delist đến trễ sẽ NULL-hoá hồi tố [delisted_date, known_from)
-- -- freeze test coi đây là miễn trừ (b), spec §7.2.1.
--
-- Plan gốc dự định lấy MIN(delisted_date) giữa HAI nguồn (std_ev_delisting_event
-- và std_ar_listing.delisted_date) -- nhưng thực ra không có hai nguồn độc lập:
-- std_ar_listing tự tính delisted_date của nó bằng left join vào chính
-- std_ev_delisting_event (xem std_ar_listing.sql, CTE with_delisting). Và
-- std_ar_listing cũng không expose delisted_date ở final select (chỉ có
-- valid_to = coalesce(next_valid_from, delisted_date), gộp chung "delisted"
-- với "bị thay bởi bản ghi mới hơn" -- vd ticker đổi tên -- nên dùng valid_to
-- làm delist_date sẽ cắt nhầm dữ liệu của công ty vẫn đang giao dịch). Chỉ
-- đọc std_ev_delisting_event trực tiếp: không mất lớp dự phòng nào (dữ liệu
-- nguồn giống hệt), chỉ bỏ một bước gián tiếp không cần thiết.
with delisted as (
    select symbol, min(delisted_date) as delist_date
    from {{ ref('std_ev_delisting_event') }}
    group by symbol
)

select
    s.date,
    s.symbol,
    {# nhóm price: 7 cột từ p #}
    case when d.delist_date is null or s.date <= d.delist_date then p.open end as open,
    case when d.delist_date is null or s.date <= d.delist_date then p.high end as high,
    case when d.delist_date is null or s.date <= d.delist_date then p.low end as low,
    case when d.delist_date is null or s.date <= d.delist_date then p.close end as close,
    case when d.delist_date is null or s.date <= d.delist_date then p.volume end as volume,
    case when d.delist_date is null or s.date <= d.delist_date then p.typical_price end as typical_price,
    case when d.delist_date is null or s.date <= d.delist_date then p.adj_close end as adj_close,
    {# nhóm derived_ohlcv: 13 cột từ p #}
    case when d.delist_date is null or s.date <= d.delist_date then p.adj_factor end as adj_factor,
    case when d.delist_date is null or s.date <= d.delist_date then p.adj_open end as adj_open,
    case when d.delist_date is null or s.date <= d.delist_date then p.adj_high end as adj_high,
    case when d.delist_date is null or s.date <= d.delist_date then p.adj_low end as adj_low,
    case when d.delist_date is null or s.date <= d.delist_date then p.adj_volume end as adj_volume,
    case when d.delist_date is null or s.date <= d.delist_date then p.returns end as returns,
    case when d.delist_date is null or s.date <= d.delist_date then p.log_return end as log_return,
    case when d.delist_date is null or s.date <= d.delist_date then p.dollar_volume end as dollar_volume,
    case when d.delist_date is null or s.date <= d.delist_date then p.adj_dollar_volume end as adj_dollar_volume,
    case when d.delist_date is null or s.date <= d.delist_date then r.market_cap end as market_cap,
    case when d.delist_date is null or s.date <= d.delist_date then r.cap end as cap,
    case when d.delist_date is null or s.date <= d.delist_date then p.amplitude end as amplitude,
    case when d.delist_date is null or s.date <= d.delist_date then p.gap_pct end as gap_pct,
    case when d.delist_date is null or s.date <= d.delist_date then p.prev_close end as prev_close,
    case when d.delist_date is null or s.date <= d.delist_date then p.adv20 end as adv20,
    case when d.delist_date is null or s.date <= d.delist_date then p.split end as split,
    case when d.delist_date is null or s.date <= d.delist_date then p.dividend end as dividend,
    {# nhóm fundamentals: 48 cột từ f #}
    case when d.delist_date is null or s.date <= d.delist_date then f.revenue end as revenue,
    case when d.delist_date is null or s.date <= d.delist_date then f.cogs end as cogs,
    case when d.delist_date is null or s.date <= d.delist_date then f.gross_profit end as gross_profit,
    case when d.delist_date is null or s.date <= d.delist_date then f.rd_expense end as rd_expense,
    case when d.delist_date is null or s.date <= d.delist_date then f.sga_expense end as sga_expense,
    case when d.delist_date is null or s.date <= d.delist_date then f.operating_income end as operating_income,
    case when d.delist_date is null or s.date <= d.delist_date then f.interest_expense end as interest_expense,
    case when d.delist_date is null or s.date <= d.delist_date then f.pretax_income end as pretax_income,
    case when d.delist_date is null or s.date <= d.delist_date then f.income_tax end as income_tax,
    case when d.delist_date is null or s.date <= d.delist_date then f.net_income end as net_income,
    case when d.delist_date is null or s.date <= d.delist_date then f.net_income_common end as net_income_common,
    case when d.delist_date is null or s.date <= d.delist_date then f.ebitda end as ebitda,
    case when d.delist_date is null or s.date <= d.delist_date then f.depreciation end as depreciation,
    case when d.delist_date is null or s.date <= d.delist_date then f.eps_basic end as eps_basic,
    case when d.delist_date is null or s.date <= d.delist_date then f.eps_diluted end as eps_diluted,
    case when d.delist_date is null or s.date <= d.delist_date then f.shares_basic end as shares_basic,
    case when d.delist_date is null or s.date <= d.delist_date then f.shares_diluted end as shares_diluted,
    case when d.delist_date is null or s.date <= d.delist_date then f.total_assets end as total_assets,
    case when d.delist_date is null or s.date <= d.delist_date then f.current_assets end as current_assets,
    case when d.delist_date is null or s.date <= d.delist_date then f.cash_equivalents end as cash_equivalents,
    case when d.delist_date is null or s.date <= d.delist_date then f.receivables end as receivables,
    case when d.delist_date is null or s.date <= d.delist_date then f.inventories end as inventories,
    case when d.delist_date is null or s.date <= d.delist_date then f.other_current_assets end as other_current_assets,
    case when d.delist_date is null or s.date <= d.delist_date then f.non_current_assets end as non_current_assets,
    case when d.delist_date is null or s.date <= d.delist_date then f.pp_and_e end as pp_and_e,
    case when d.delist_date is null or s.date <= d.delist_date then f.intangibles end as intangibles,
    case when d.delist_date is null or s.date <= d.delist_date then f.goodwill end as goodwill,
    case when d.delist_date is null or s.date <= d.delist_date then f.total_liabilities end as total_liabilities,
    case when d.delist_date is null or s.date <= d.delist_date then f.current_liabilities end as current_liabilities,
    case when d.delist_date is null or s.date <= d.delist_date then f.accounts_payable end as accounts_payable,
    case when d.delist_date is null or s.date <= d.delist_date then f.short_term_debt end as short_term_debt,
    case when d.delist_date is null or s.date <= d.delist_date then f.long_term_debt end as long_term_debt,
    case when d.delist_date is null or s.date <= d.delist_date then f.total_equity end as total_equity,
    case when d.delist_date is null or s.date <= d.delist_date then f.common_equity end as common_equity,
    case when d.delist_date is null or s.date <= d.delist_date then f.retained_earnings end as retained_earnings,
    case when d.delist_date is null or s.date <= d.delist_date then f.shares_outstanding end as shares_outstanding,
    case when d.delist_date is null or s.date <= d.delist_date then f.cfo end as cfo,
    case when d.delist_date is null or s.date <= d.delist_date then f.cfi end as cfi,
    case when d.delist_date is null or s.date <= d.delist_date then f.cff end as cff,
    case when d.delist_date is null or s.date <= d.delist_date then f.capex end as capex,
    case when d.delist_date is null or s.date <= d.delist_date then f.maintenance_capex end as maintenance_capex,
    case when d.delist_date is null or s.date <= d.delist_date then f.depreciation_cf end as depreciation_cf,
    case when d.delist_date is null or s.date <= d.delist_date then f.stock_based_comp end as stock_based_comp,
    case when d.delist_date is null or s.date <= d.delist_date then f.changes_working_capital end as changes_working_capital,
    case when d.delist_date is null or s.date <= d.delist_date then f.dividends_paid end as dividends_paid,
    case when d.delist_date is null or s.date <= d.delist_date then f.share_repurchases end as share_repurchases,
    case when d.delist_date is null or s.date <= d.delist_date then f.net_change_cash end as net_change_cash,
    case when d.delist_date is null or s.date <= d.delist_date then f.free_cash_flow end as free_cash_flow,
    case when d.delist_date is null or s.date <= d.delist_date then f.float_shares end as float_shares,
    {# nhóm analyst: 15 cột (12 từ a, 3 từ ae) #}
    case when d.delist_date is null or s.date <= d.delist_date then a.num_analysts_eps end as num_analysts_eps,
    case when d.delist_date is null or s.date <= d.delist_date then a.eps_estimate_cur_y end as eps_estimate_cur_y,
    case when d.delist_date is null or s.date <= d.delist_date then a.eps_estimate_next_y end as eps_estimate_next_y,
    case when d.delist_date is null or s.date <= d.delist_date then a.num_analysts_rating end as num_analysts_rating,
    case when d.delist_date is null or s.date <= d.delist_date then a.rating_strong_buy end as rating_strong_buy,
    case when d.delist_date is null or s.date <= d.delist_date then a.rating_buy end as rating_buy,
    case when d.delist_date is null or s.date <= d.delist_date then a.rating_hold end as rating_hold,
    case when d.delist_date is null or s.date <= d.delist_date then a.rating_sell end as rating_sell,
    case when d.delist_date is null or s.date <= d.delist_date then a.rating_strong_sell end as rating_strong_sell,
    case when d.delist_date is null or s.date <= d.delist_date then a.price_target_mean end as price_target_mean,
    case when d.delist_date is null or s.date <= d.delist_date then a.price_target_high end as price_target_high,
    case when d.delist_date is null or s.date <= d.delist_date then a.price_target_low end as price_target_low,
    case when d.delist_date is null or s.date <= d.delist_date then ae.analyst_upgrade_count end as analyst_upgrade_count,
    case when d.delist_date is null or s.date <= d.delist_date then ae.analyst_downgrade_count end as analyst_downgrade_count,
    case when d.delist_date is null or s.date <= d.delist_date then ae.analyst_action_count end as analyst_action_count,
    {# nhóm dimension: 6 cột từ g #}
    case when d.delist_date is null or s.date <= d.delist_date then g.vendor_sector end as vendor_sector,
    case when d.delist_date is null or s.date <= d.delist_date then g.vendor_industry end as vendor_industry,
    case when d.delist_date is null or s.date <= d.delist_date then g.market end as market,
    case when d.delist_date is null or s.date <= d.delist_date then g.exchange end as exchange,
    case when d.delist_date is null or s.date <= d.delist_date then g.country end as country,
    case when d.delist_date is null or s.date <= d.delist_date then g.employee_count end as employee_count,
    {# nhóm ratios: 72 cột từ r (B2-B11) #}
    case when d.delist_date is null or s.date <= d.delist_date then r.pe_ratio end as pe_ratio,
    case when d.delist_date is null or s.date <= d.delist_date then r.pe_ratio_fwd end as pe_ratio_fwd,
    case when d.delist_date is null or s.date <= d.delist_date then r.pb_ratio end as pb_ratio,
    case when d.delist_date is null or s.date <= d.delist_date then r.ps_ratio end as ps_ratio,
    case when d.delist_date is null or s.date <= d.delist_date then r.total_debt end as total_debt,
    case when d.delist_date is null or s.date <= d.delist_date then r.net_debt end as net_debt,
    case when d.delist_date is null or s.date <= d.delist_date then r.enterprise_value end as enterprise_value,
    case when d.delist_date is null or s.date <= d.delist_date then r.ev_ebitda end as ev_ebitda,
    case when d.delist_date is null or s.date <= d.delist_date then r.ev_ebit end as ev_ebit,
    case when d.delist_date is null or s.date <= d.delist_date then r.ev_sales end as ev_sales,
    case when d.delist_date is null or s.date <= d.delist_date then r.ev_fcf end as ev_fcf,
    case when d.delist_date is null or s.date <= d.delist_date then r.price_to_fcf end as price_to_fcf,
    case when d.delist_date is null or s.date <= d.delist_date then r.price_to_cfo end as price_to_cfo,
    case when d.delist_date is null or s.date <= d.delist_date then r.book_value_per_share end as book_value_per_share,
    case when d.delist_date is null or s.date <= d.delist_date then r.gross_margin end as gross_margin,
    case when d.delist_date is null or s.date <= d.delist_date then r.operating_margin end as operating_margin,
    case when d.delist_date is null or s.date <= d.delist_date then r.ebitda_margin end as ebitda_margin,
    case when d.delist_date is null or s.date <= d.delist_date then r.net_margin end as net_margin,
    case when d.delist_date is null or s.date <= d.delist_date then r.net_margin_common end as net_margin_common,
    case when d.delist_date is null or s.date <= d.delist_date then r.fcf_margin end as fcf_margin,
    case when d.delist_date is null or s.date <= d.delist_date then r.rd_intensity end as rd_intensity,
    case when d.delist_date is null or s.date <= d.delist_date then r.sga_intensity end as sga_intensity,
    case when d.delist_date is null or s.date <= d.delist_date then r.tax_rate_effective end as tax_rate_effective,
    case when d.delist_date is null or s.date <= d.delist_date then r.interest_burden end as interest_burden,
    case when d.delist_date is null or s.date <= d.delist_date then r.roe end as roe,
    case when d.delist_date is null or s.date <= d.delist_date then r.roa end as roa,
    case when d.delist_date is null or s.date <= d.delist_date then r.nopat end as nopat,
    case when d.delist_date is null or s.date <= d.delist_date then r.invested_capital end as invested_capital,
    case when d.delist_date is null or s.date <= d.delist_date then r.roic end as roic,
    case when d.delist_date is null or s.date <= d.delist_date then r.roce end as roce,
    case when d.delist_date is null or s.date <= d.delist_date then r.gross_profit_on_assets end as gross_profit_on_assets,
    case when d.delist_date is null or s.date <= d.delist_date then r.debt_to_equity end as debt_to_equity,
    case when d.delist_date is null or s.date <= d.delist_date then r.debt_to_assets end as debt_to_assets,
    case when d.delist_date is null or s.date <= d.delist_date then r.net_debt_to_ebitda end as net_debt_to_ebitda,
    case when d.delist_date is null or s.date <= d.delist_date then r.net_debt_to_equity end as net_debt_to_equity,
    case when d.delist_date is null or s.date <= d.delist_date then r.interest_coverage end as interest_coverage,
    case when d.delist_date is null or s.date <= d.delist_date then r.equity_multiplier end as equity_multiplier,
    case when d.delist_date is null or s.date <= d.delist_date then r.altman_z_score end as altman_z_score,
    case when d.delist_date is null or s.date <= d.delist_date then r.current_ratio end as current_ratio,
    case when d.delist_date is null or s.date <= d.delist_date then r.quick_ratio end as quick_ratio,
    case when d.delist_date is null or s.date <= d.delist_date then r.cash_ratio end as cash_ratio,
    case when d.delist_date is null or s.date <= d.delist_date then r.working_capital end as working_capital,
    case when d.delist_date is null or s.date <= d.delist_date then r.working_capital_to_assets end as working_capital_to_assets,
    case when d.delist_date is null or s.date <= d.delist_date then r.asset_turnover end as asset_turnover,
    case when d.delist_date is null or s.date <= d.delist_date then r.inventory_turnover end as inventory_turnover,
    case when d.delist_date is null or s.date <= d.delist_date then r.receivables_turnover end as receivables_turnover,
    case when d.delist_date is null or s.date <= d.delist_date then r.payables_turnover end as payables_turnover,
    case when d.delist_date is null or s.date <= d.delist_date then r.days_inventory_outstanding end as days_inventory_outstanding,
    case when d.delist_date is null or s.date <= d.delist_date then r.days_sales_outstanding end as days_sales_outstanding,
    case when d.delist_date is null or s.date <= d.delist_date then r.days_payable_outstanding end as days_payable_outstanding,
    case when d.delist_date is null or s.date <= d.delist_date then r.cash_conversion_cycle end as cash_conversion_cycle,
    case when d.delist_date is null or s.date <= d.delist_date then r.fixed_asset_turnover end as fixed_asset_turnover,
    case when d.delist_date is null or s.date <= d.delist_date then r.revenue_growth_yoy end as revenue_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.revenue_growth_qoq end as revenue_growth_qoq,
    case when d.delist_date is null or s.date <= d.delist_date then r.gross_profit_growth_yoy end as gross_profit_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.net_income_growth_yoy end as net_income_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.eps_growth_yoy end as eps_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.ebitda_growth_yoy end as ebitda_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.fcf_growth_yoy end as fcf_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.total_assets_growth_yoy end as total_assets_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.shares_growth_yoy end as shares_growth_yoy,
    case when d.delist_date is null or s.date <= d.delist_date then r.cfo_to_net_income end as cfo_to_net_income,
    case when d.delist_date is null or s.date <= d.delist_date then r.accruals_ratio end as accruals_ratio,
    case when d.delist_date is null or s.date <= d.delist_date then r.fcf_to_earnings end as fcf_to_earnings,
    case when d.delist_date is null or s.date <= d.delist_date then r.capex_to_revenue end as capex_to_revenue,
    case when d.delist_date is null or s.date <= d.delist_date then r.capex_to_depreciation end as capex_to_depreciation,
    case when d.delist_date is null or s.date <= d.delist_date then r.stock_comp_to_revenue end as stock_comp_to_revenue,
    case when d.delist_date is null or s.date <= d.delist_date then r.earnings_surprise_pct end as earnings_surprise_pct,
    case when d.delist_date is null or s.date <= d.delist_date then r.earnings_surprise_sign end as earnings_surprise_sign,
    case when d.delist_date is null or s.date <= d.delist_date then r.revenue_surprise_pct end as revenue_surprise_pct,
    case when d.delist_date is null or s.date <= d.delist_date then r.standardized_unexpected_earnings end as standardized_unexpected_earnings,
    case when d.delist_date is null or s.date <= d.delist_date then r.analyst_consensus_score end as analyst_consensus_score,
    case when d.delist_date is null or s.date <= d.delist_date then r.revenue_per_employee end as revenue_per_employee,
    case when d.delist_date is null or s.date <= d.delist_date then r.owner_earnings end as owner_earnings,
    current_timestamp as loaded_at
from {{ ref('__spine_daily') }} s
left join {{ ref('__fam_price') }}         p  on p.date = s.date and p.symbol = s.symbol
left join {{ ref('__fam_fundamental') }}   f  on f.date = s.date and f.symbol = s.symbol
left join {{ ref('__fam_analyst') }}       a  on a.date = s.date and a.symbol = s.symbol
left join {{ ref('__fam_analyst_event') }} ae on ae.date = s.date and ae.symbol = s.symbol
left join {{ ref('__fam_group') }}         g  on g.date = s.date and g.symbol = s.symbol
left join {{ ref('__fam_ratios') }}        r  on r.date = s.date and r.symbol = s.symbol
left join delisted d on d.symbol = s.symbol
