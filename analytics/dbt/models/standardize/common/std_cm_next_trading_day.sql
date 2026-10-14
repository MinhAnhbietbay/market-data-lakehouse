{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_cm_next_trading_day',
        tags=['layer::standardize', 'domain::common']
    )
}}

-- D14: known_from rơi vào ngày không giao dịch thì làm tròn TỚI NGÀY GIAO DỊCH KẾ TIẾP.
-- Hiện thực bằng bảng tra để mọi model chỉ cần một join đẳng thức. Cách viết
-- "(select min(trade_date) ... where trade_date >= d)" ngay trong model là subquery
-- tương quan chạy một lần cho MỖI DÒNG -- trên std_tx_daily_price_bar (45M dòng ở quy
-- mô đích) thì nó không chạy nổi.
with bounds as (
    select min(trade_date) as first_day, max(trade_date) as last_day
    from {{ ref('std_cm_trading_day') }}
),

calendar as (
    select explode(sequence(first_day, last_day, interval 1 day)) as calendar_date
    from bounds
),

joined as (
    select
        c.calendar_date,
        t.trade_date,
        row_number() over (partition by c.calendar_date order by t.trade_date) as rn
    from calendar c
    join {{ ref('std_cm_trading_day') }} t
      on t.trade_date >= c.calendar_date
)

select
    calendar_date,
    trade_date as next_trading_day
from joined
where rn = 1
