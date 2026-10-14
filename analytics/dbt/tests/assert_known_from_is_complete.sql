-- Luật PIT chỉ có giá trị nếu KHÔNG bảng nào lọt lưới. Test này liệt kê thủ công
-- các bảng thuộc nhóm filing; thêm bảng mới vào nhóm thì thêm vào đây.
with unioned as (
    select 'std_ac_income_statement' as model, known_from, pit_basis from {{ ref('std_ac_income_statement') }}
    union all select 'std_ac_balance_sheet_statement', known_from, pit_basis from {{ ref('std_ac_balance_sheet_statement') }}
    union all select 'std_ac_cash_flow_statement', known_from, pit_basis from {{ ref('std_ac_cash_flow_statement') }}
    union all select 'std_ev_cash_dividend', known_from, pit_basis from {{ ref('std_ev_cash_dividend') }}
    union all select 'std_ev_earnings_announcement', known_from, pit_basis from {{ ref('std_ev_earnings_announcement') }}
    union all select 'std_bd_analyst_grade_action', known_from, pit_basis from {{ ref('std_bd_analyst_grade_action') }}
    union all select 'std_bd_analyst_rating_distribution', known_from, pit_basis from {{ ref('std_bd_analyst_rating_distribution') }}
    union all select 'std_gr_security_rating', known_from, pit_basis from {{ ref('std_gr_security_rating') }}
    union all select 'std_tx_daily_price_bar', known_from, pit_basis from {{ ref('std_tx_daily_price_bar') }}
    union all select 'std_tx_market_cap_observation', known_from, pit_basis from {{ ref('std_tx_market_cap_observation') }}
    union all select 'std_pd_share_count_observation', known_from, pit_basis from {{ ref('std_pd_share_count_observation') }}
    union all select 'std_cm_treasury_rate', known_from, pit_basis from {{ ref('std_cm_treasury_rate') }}
)
select model, count(*) as bad_rows
from unioned
where known_from is null or pit_basis not in ('filing', 'observation')
group by model
