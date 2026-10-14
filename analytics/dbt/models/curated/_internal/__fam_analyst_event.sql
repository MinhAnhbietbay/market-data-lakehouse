{{
    config(materialized='table', partition_by=['months(date)'], file_format='iceberg',
           schema=var('env_curated'), alias='__fam_analyst_event',
           tags=['layer::curated', 'internal'])
}}

-- Họ ĐẾM SỰ KIỆN -- KHÔNG dùng collapse_asof. Mọi họ khác forward-fill một trạng
-- thái; họ này đếm biến cố xảy ra ĐÚNG ngày đó. Ngày không có sự kiện phải là 0
-- chứ không phải NULL: ts_sum(x, 90) của DSL cộng qua NULL sẽ ra NULL, và sự kiện
-- thưa (5.805 biến cố trên 5 x 7.546 ô lưới) sẽ làm mọi cửa sổ hỏng.
--
-- Neo vào known_from, KHÔNG phải grade_date: 102/5.805 dòng có hai ngày này khác
-- nhau, neo vào grade_date là đưa sự kiện vào panel TRƯỚC ngày ta biết nó.
--
-- Đẩy sang ngày giao dịch kế tiếp qua std_cm_next_trading_day dù đo hôm nay 0/5.805
-- sự kiện rơi ngoài ngày giao dịch -- "hôm nay bằng 0" là một quan sát, không phải
-- một bảo đảm; bỏ bước này thì sự kiện rơi vào thứ Bảy biến mất im lặng.
with events as (
    select
        coalesce(n.next_trading_day, g.known_from) as date,
        g.symbol,
        g.action
    from {{ ref('std_bd_analyst_grade_action') }} g
    left join {{ ref('std_cm_next_trading_day') }} n
      on n.calendar_date = g.known_from
),

counted as (
    select
        date,
        symbol,
        -- sum(case ...) chứ KHÔNG phải count(*) filter (where ...): cú pháp FILTER
        -- chưa từng được dùng ở đâu trong repo này, và đợt 2 đã mất một vòng vì một
        -- cú pháp Spark không đỡ (UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY,
        -- period_window_body.sql). Không thử vận may với cú pháp ở một model mới.
        sum(case when action = 'upgrade'   then 1 else 0 end) as upgrade_count,
        sum(case when action = 'downgrade' then 1 else 0 end) as downgrade_count,
        -- action_count đếm MỌI dòng kể cả action NULL (đo: 1 dòng): nó là mức độ
        -- chú ý của giới phân tích, và một sự kiện thiếu nhãn vẫn là một sự kiện.
        -- Vì vậy action_count >= upgrade + downgrade luôn đúng, KHÔNG phải lỗi.
        count(*)                                              as action_count
    from events
    group by date, symbol
)

select
    s.date,
    s.symbol,
    coalesce(c.upgrade_count, 0)   as analyst_upgrade_count,
    coalesce(c.downgrade_count, 0) as analyst_downgrade_count,
    coalesce(c.action_count, 0)    as analyst_action_count,
    current_timestamp              as loaded_at
from {{ ref('__spine_daily') }} s
left join counted c on c.date = s.date and c.symbol = s.symbol
