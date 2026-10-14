{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_bd_price_target_consensus',
        tags=['layer::standardize', 'domain::business_direction']
    )
}}

-- Price Target Consensus (bd area), daily_fact -- F11b (spec 2026-08-18): gộp
-- /price-target-consensus (high/low/median/consensus) và /price-target-summary (count +
-- avg target theo cửa sổ tháng/quý/năm/toàn thời gian) vào một bảng vật lý duy nhất, khoá
-- (symbol, observation_date). Cả hai bảng raw đều tổng hợp observation_date từ
-- @run_date (không có cột date thật) nên PK ở đây trùng khít giữa hai nhánh khi cùng chạy
-- trong một run_date -- full outer join, không union, vì hai nhánh KHÔNG chia sẻ cột giá
-- trị nào ngoài khoá (đây là ghép thuộc tính theo chiều ngang, không phải ghép theo hàng).
with consensus_ranked as (
    select
        symbol,
        observation_date,
        target_high,
        target_low,
        target_consensus,
        target_median,
        run_date,
        row_number() over (
            partition by symbol, observation_date, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_price_target_consensus') }}
    where symbol is not null and observation_date is not null
),

consensus_latest as (
    select
        symbol, observation_date, target_high, target_low, target_consensus, target_median, run_date
    from consensus_ranked
    where rn = 1
),

summary_ranked as (
    select
        symbol,
        observation_date,
        last_month_count,
        last_month_avg_price_target,
        last_quarter_count,
        last_quarter_avg_price_target,
        last_year_count,
        last_year_avg_price_target,
        all_time_count,
        all_time_avg_price_target,
        run_date,
        row_number() over (
            partition by symbol, observation_date, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_price_target_summary') }}
    where symbol is not null and observation_date is not null
),

summary_latest as (
    select
        symbol, observation_date, last_month_count, last_month_avg_price_target,
        last_quarter_count, last_quarter_avg_price_target, last_year_count,
        last_year_avg_price_target, all_time_count, all_time_avg_price_target, run_date
    from summary_ranked
    where rn = 1
),

-- run_date khớp CHÍNH XÁC hai nhánh: cả hai bảng raw cùng nạp trong một batch
-- FMP hàng ngày (cùng quy ước run_date toàn tầng raw), nên join theo run_date ở
-- đây tương đương join theo "cùng một lần kéo". CẢNH BÁO đã biết: nếu về sau hai
-- nguồn tách lịch kéo (không còn cùng run_date), cách join này sẽ mất giá trị nhánh
-- kia ở phiên bản mới thay vì forward-fill nó -- chưa gặp trong dữ liệu hiện tại
-- (toàn bộ tầng raw chỉ có 1 run_date, đo được 2026-08-22), nhưng đây là nợ thiết kế
-- cần biết trước khi 2 nguồn có lịch kéo lệch nhau.
merged as (
    select
        coalesce(consensus_latest.symbol, summary_latest.symbol) as symbol,
        coalesce(consensus_latest.observation_date, summary_latest.observation_date) as observation_date,
        coalesce(consensus_latest.run_date, summary_latest.run_date) as run_date,
        consensus_latest.target_high,
        consensus_latest.target_low,
        consensus_latest.target_consensus,
        consensus_latest.target_median,
        summary_latest.last_month_count,
        summary_latest.last_month_avg_price_target,
        summary_latest.last_quarter_count,
        summary_latest.last_quarter_avg_price_target,
        summary_latest.last_year_count,
        summary_latest.last_year_avg_price_target,
        summary_latest.all_time_count,
        summary_latest.all_time_avg_price_target
    from consensus_latest
    full outer join summary_latest
        on consensus_latest.symbol = summary_latest.symbol
        and consensus_latest.observation_date = summary_latest.observation_date
        and consensus_latest.run_date = summary_latest.run_date
),

{{ bitemporal_version_chain(
    source_cte='merged',
    key_cols=['symbol', 'observation_date'],
    fingerprint_cols=[
        'target_high', 'target_low', 'target_consensus', 'target_median',
        'last_month_count', 'last_month_avg_price_target',
        'last_quarter_count', 'last_quarter_avg_price_target',
        'last_year_count', 'last_year_avg_price_target',
        'all_time_count', 'all_time_avg_price_target'
    ],
    first_known_from_expr='observation_date',
    pit_basis='observation',
    order_by='run_date desc'
) }}

select
    symbol,
    observation_date,
    target_high,
    target_low,
    target_consensus,
    target_median,
    last_month_count,
    last_month_avg_price_target,
    last_quarter_count,
    last_quarter_avg_price_target,
    last_year_count,
    last_year_avg_price_target,
    all_time_count,
    all_time_avg_price_target,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

