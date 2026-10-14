-- known_from là "ta biết được từ lúc nào". Một ngày ở tương lai nghĩa là bảng
-- tự khai chưa ai biết dòng này, trong khi nó đang nằm ngay đó. Nguồn earnings
-- trả cả lịch công bố sắp tới, nên đây không phải giả thuyết: đo ngày
-- 2026-08-22 có 5 dòng như vậy trước khi sửa.
{% set versioned_models = [
    'std_ac_balance_sheet_statement',
    'std_ac_cash_flow_statement',
    'std_ac_enterprise_value',
    'std_ac_income_statement',
    'std_ac_owner_earnings',
    'std_ac_ratio',
    'std_ac_revenue_segment_breakdown',
    'std_ar_index_membership',
    'std_ar_listing',
    'std_bd_analyst_estimate',
    'std_bd_analyst_grade_action',
    'std_bd_analyst_rating_distribution',
    'std_bd_price_target_consensus',
    'std_cm_macro_observation',
    'std_cm_treasury_rate',
    'std_do_financial_report_document',
    'std_ev_cash_dividend',
    'std_ev_delisting_event',
    'std_ev_earnings_announcement',
    'std_ev_economic_data_release',
    'std_ev_ipo_event',
    'std_ev_merger_acquisition',
    'std_ev_stock_split',
    'std_ev_symbol_change_event',
    'std_gr_security_rating',
    'std_gr_security_rating_score',
    'std_ip_legal_entity',
    'std_ip_workforce_observation',
    'std_pd_share_count_observation',
    'std_pr_beneficial_ownership_position',
    'std_tx_daily_price_bar',
    'std_tx_insider_activity_summary',
    'std_tx_insider_transaction',
    'std_tx_market_cap_observation'
] %}

{% for model_name in versioned_models %}
select '{{ model_name }}' as model_name, known_from
from {{ ref(model_name) }}
where known_from > current_date
{% if not loop.last %}union all{% endif %}
{% endfor %}
