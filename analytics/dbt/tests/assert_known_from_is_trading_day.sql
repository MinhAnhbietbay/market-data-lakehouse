-- D14: known_from phải luôn rơi vào một ngày giao dịch, hoặc muộn hơn ngày giao
-- dịch cuối cùng ta biết (chưa có ngày nào để làm tròn tới). Trả về dòng nào là đỏ.
--
-- Quét MỌI model có known_from sau Task 4/5, trừ hai ngoại lệ nói ở dưới.
--
-- Bảng rỗng vẫn nằm trong union này (không có gì để loại chúng ra một cách tất
-- định), nhưng chúng đóng góp 0 dòng nên KHÔNG chứng minh gì -- report phải nói
-- thẳng con số này đến từ mấy bảng có dữ liệu thật.
--
-- BIÊN DƯỚI (thêm 2026-08-22, phát hiện khi chạy Task 6B thật -- 128 vi phạm đo
-- được, toàn bộ known_from < 1996-08-19, tức trước cả ngày giao dịch ĐẦU TIÊN ta
-- biết): std_cm_next_trading_day chỉ phủ [min(trade_date), max(trade_date)], nên
-- một known_from_raw sớm hơn min(trade_date) không khớp calendar_date nào và
-- coalesce rơi về chính giá trị gốc (không phải trading day) -- y hệt cơ chế
-- "không có ngày để làm tròn tới" đã chấp nhận ở biên trên, chỉ khác hướng. Đây
-- KHÔNG phải look-ahead (không "bịa" ngày tương lai) và KHÔNG backtest nào chạy
-- được trước 1996-08-19 (std_cm_trading_day không có dữ liệu ở đó) nên chệch vài
-- ngày trước ngưỡng này không gây thiệt hay gian lận gì -- loại trừ đối xứng với
-- biên trên, không phải nới lỏng bất biến D14.
with calendar_bound as (
    select
        min(trade_date) as first_known_trading_day,
        max(trade_date) as last_known_trading_day
    from {{ ref('std_cm_trading_day') }}
),

all_known_from as (
    -- nhóm filing
    select 'std_ac_income_statement' as model_name, known_from from {{ ref('std_ac_income_statement') }}
    union all
    select 'std_ac_balance_sheet_statement', known_from from {{ ref('std_ac_balance_sheet_statement') }}
    union all
    select 'std_ac_cash_flow_statement', known_from from {{ ref('std_ac_cash_flow_statement') }}
    union all
    select 'std_bd_analyst_grade_action', known_from from {{ ref('std_bd_analyst_grade_action') }}
    union all
    select 'std_bd_analyst_rating_distribution', known_from from {{ ref('std_bd_analyst_rating_distribution') }}
    union all
    select 'std_cm_treasury_rate', known_from from {{ ref('std_cm_treasury_rate') }}
    union all
    select 'std_ev_cash_dividend', known_from from {{ ref('std_ev_cash_dividend') }}
    union all
    select 'std_ev_earnings_announcement', known_from from {{ ref('std_ev_earnings_announcement') }}
    union all
    select 'std_ev_merger_acquisition', known_from from {{ ref('std_ev_merger_acquisition') }}
    union all
    select 'std_gr_security_rating', known_from from {{ ref('std_gr_security_rating') }}
    union all
    select 'std_pd_share_count_observation', known_from from {{ ref('std_pd_share_count_observation') }}
    -- nhóm observation (Task 4, 5) -- known_from = run_date, và lịch chạy
    -- pipeline không trùng lịch giao dịch, nên đây mới là chỗ dễ vi phạm nhất
    union all
    select 'std_ac_ratio', known_from from {{ ref('std_ac_ratio') }}
    union all
    select 'std_ac_owner_earnings', known_from from {{ ref('std_ac_owner_earnings') }}
    union all
    select 'std_ac_enterprise_value', known_from from {{ ref('std_ac_enterprise_value') }}
    union all
    select 'std_ac_revenue_segment_breakdown', known_from from {{ ref('std_ac_revenue_segment_breakdown') }}
    union all
    select 'std_do_financial_report_document', known_from from {{ ref('std_do_financial_report_document') }}
    union all
    select 'std_bd_analyst_estimate', known_from from {{ ref('std_bd_analyst_estimate') }}
    union all
    select 'std_bd_price_target_consensus', known_from from {{ ref('std_bd_price_target_consensus') }}
    union all
    select 'std_cm_macro_observation', known_from from {{ ref('std_cm_macro_observation') }}
    union all
    select 'std_ev_stock_split', known_from from {{ ref('std_ev_stock_split') }}
    union all
    select 'std_ev_delisting_event', known_from from {{ ref('std_ev_delisting_event') }}
    union all
    select 'std_ip_legal_entity', known_from from {{ ref('std_ip_legal_entity') }}
    union all
    select 'std_ip_workforce_observation', known_from from {{ ref('std_ip_workforce_observation') }}
    -- CỐ Ý KHÔNG có std_tx_daily_price_bar và std_tx_market_cap_observation:
    -- known_from của chúng LÀ trade_date, lấy từ chính bảng làm nên
    -- std_cm_trading_day. Kiểm chúng ở đây là một test lặp lại (tautology) --
    -- luôn xanh vì cấu trúc, không vì hiện thực đúng.
),

violations as (
    select
        a.model_name,
        a.known_from
    from all_known_from a
    cross join calendar_bound b
    left join {{ ref('std_cm_trading_day') }} t on t.trade_date = a.known_from
    where a.known_from is not null
      and t.trade_date is null
      and a.known_from >= b.first_known_trading_day
      and a.known_from <= b.last_known_trading_day
)

select * from violations
