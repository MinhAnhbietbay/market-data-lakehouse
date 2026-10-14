{{
    config(
        materialized='table',
        partition_by=['months(date)'],
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_date',
        tags=['layer::curated', 'engine_table']
    )
}}

-- Engine snap biên IS/OS về ngày giao dịch có thật, đếm lookback_days theo
-- phiên, và dựng grid đầy đủ bằng tích chéo universe × trading_days. Cả ba
-- việc đọc bảng này.
--
-- Grain là TOÀN BỘ ngày lịch, KHÔNG chỉ ngày giao dịch (spec quant-compute-
-- engine.md: "Biên -7 năm/-2 năm là offset lịch, sau đó snap về ngày giao dịch
-- có thật gần nhất trong dim_date" -- offset đó rơi vào cuối tuần/ngày lễ là
-- chuyện thường, nên bảng phải phủ mọi ngày lịch để lookup không bao giờ miss).
-- Xác nhận trên dữ liệu thật (2026-08-2x): std_cm_next_trading_day phủ 10956
-- ngày lịch (1996-08-19 .. 2026-08-17), std_cm_trading_day chỉ có 7546 ngày
-- giao dịch.
--
-- LỖI THỜI 2026-09-14: con số 30 năm trên đo TRƯỚC quyết định chốt độ sâu
-- OHLCV về 10 năm (configs/sources/data_sources.yaml, ohlcv_non_split_adjusted,
-- "Độ sâu chốt 10 năm (quyết định 2026-08-19), không phải 30" -- cùng ngày,
-- nhưng quyết định đó không lan ngược vào comment này). Sau `down -v` +
-- rebuild sạch, phạm vi thật của bảng này chỉ còn ~10 năm gần nhất (đo
-- 2026-09-14: 2016-09-13 .. 2026-09-11, 3651 dòng) -- đúng theo config hiện
-- hành, không phải thiếu dữ liệu. Xem assert_curated_workforce_date_orphan_count.sql
-- cho hệ quả downstream của việc này.
--
-- next_trading_day lấy từ std_cm_next_trading_day, semantics round-forward
-- BAO GỒM CHÍNH NÓ nếu calendar_date đã là ngày giao dịch (xác nhận: 1996-09-04
-- là ngày giao dịch, next_trading_day = 1996-09-04). prev_trading_day không có
-- nguồn sẵn -- suy ra đối xứng bằng window carry-forward (KHÔNG dùng non-equi
-- join/correlated subquery: std_cm_next_trading_day.sql tự cảnh báo pattern đó
-- chạy một lần mỗi dòng, không chạy nổi ở quy mô lớn).

with calendar as (
    select
        calendar_date as date,
        next_trading_day
    from {{ ref('std_cm_next_trading_day') }}
),

trading as (
    select trade_date
    from {{ ref('std_cm_trading_day') }}
),

flagged as (
    select
        c.date,
        c.next_trading_day,
        t.trade_date is not null as is_trading_day,
        case when t.trade_date is not null then c.date end as self_if_trading
    from calendar c
    left join trading t on t.trade_date = c.date
)

select
    date,
    is_trading_day,
    next_trading_day,
    max(self_if_trading) over (
        order by date
        rows between unbounded preceding and current row
    ) as prev_trading_day,
    current_timestamp as loaded_at
from flagged
