-- Truy vấn as-of lấy "phiên bản có known_from lớn nhất mà <= d". Nếu known_from
-- giảm giữa hai phiên bản của cùng một khoá thì phép chọn đó trả về BẢN CŨ
-- sau khi đã có bản mới -- sai âm thầm, không lỗi, không log.
--
-- Bất biến của cả tầng standardize: với mọi model có chuỗi phiên bản theo source_run_date,
-- known_from phải tăng đơn điệu theo source_run_date trên từng khoá tự nhiên.
-- ORDER BY source_run_date, KHÔNG phải known_from. Sắp xếp theo chính cột
-- đang kiểm là một tautology: lag(known_from) khi đó luôn <= known_from,
-- nên điều kiện vi phạm vĩnh viễn sai và test xanh kể cả lúc có lỗi.
--
{% set monotonic_models = {
    'std_ac_balance_sheet_statement': ['symbol', 'fiscal_year', 'period'],
    'std_ac_cash_flow_statement': ['symbol', 'fiscal_year', 'period'],
    'std_ac_enterprise_value': ['symbol', 'period_end_date'],
    'std_ac_income_statement': ['symbol', 'fiscal_year', 'period'],
    'std_ac_owner_earnings': ['symbol', 'fiscal_year', 'period'],
    'std_ac_ratio': ['symbol', 'fiscal_year', 'period'],
    'std_ac_revenue_segment_breakdown': ['symbol', 'fiscal_year', 'period', 'segment_name'],
    'std_ar_index_membership': ['index_symbol', 'symbol', 'valid_from'],
    'std_ar_listing': ['symbol', 'market_code', 'valid_from'],
    'std_bd_analyst_estimate': ['symbol', 'estimate_period_end_date'],
    'std_bd_analyst_grade_action': ['symbol', 'grade_date', 'grading_company_key'],
    'std_bd_analyst_rating_distribution': ['symbol', 'grade_date', 'rating_bucket'],
    'std_bd_price_target_consensus': ['symbol', 'observation_date'],
    'std_cm_macro_observation': ['series_name', 'observation_date'],
    'std_cm_treasury_rate': ['observation_date', 'tenor'],
    'std_do_financial_report_document': ['symbol', 'fiscal_year', 'period'],
    'std_ev_cash_dividend': ['symbol', 'ex_dividend_date'],
    'std_ev_delisting_event': ['symbol', 'delisted_date'],
    'std_ev_earnings_announcement': ['symbol', 'announcement_date'],
    'std_ev_economic_data_release': ['event_key', 'country_code', 'release_date'],
    'std_ev_ipo_event': ['symbol', 'effectiveness_date'],
    'std_ev_merger_acquisition': ['symbol', 'cik', 'announcement_date', 'targeted_party_key'],
    'std_ev_stock_split': ['symbol', 'split_effective_date'],
    'std_ev_symbol_change_event': ['old_symbol', 'new_symbol', 'change_date'],
    'std_gr_security_rating': ['symbol', 'rating_date'],
    'std_gr_security_rating_score': ['symbol', 'rating_date', 'score_name'],
    'std_ip_legal_entity': ['cik', 'valid_from'],
    'std_ip_workforce_observation': ['cik', 'period_end_date'],
    'std_pd_share_count_observation': ['symbol', 'observation_date'],
    'std_pr_beneficial_ownership_position': ['symbol', 'cik'],
    'std_tx_daily_price_bar': ['symbol', 'trade_date'],
    'std_tx_insider_activity_summary': ['symbol', 'cik', 'fiscal_year', 'fiscal_quarter'],
    'std_tx_insider_transaction': ['reporting_cik', 'symbol', 'transaction_date', 'transaction_type'],
    'std_tx_market_cap_observation': ['symbol', 'trade_date']
} %}

with
{% for model_name, key_cols in monotonic_models.items() %}
{{ model_name }}_lagged as (
    select
        '{{ model_name }}' as model_name,
        known_from,
        source_run_date,
        lag(known_from) over (
            partition by {{ key_cols | join(', ') }}
            order by source_run_date
        ) as prev_known_from
    from {{ ref(model_name) }}
),

{{ model_name }}_descending as (
    select
        model_name,
        known_from,
        prev_known_from,
        source_run_date
    from {{ model_name }}_lagged
    where prev_known_from is not null
      and known_from < prev_known_from
),
{% endfor %}

all_descending as (
{% for model_name in monotonic_models %}
    select * from {{ model_name }}_descending
    {% if not loop.last %}union all{% endif %}
{% endfor %}
)

select * from all_descending
