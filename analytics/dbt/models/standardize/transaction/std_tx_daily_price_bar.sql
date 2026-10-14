{{
    config(
        materialized='table',
        partition_by=['months(trade_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_tx_daily_price_bar',
        tags=['layer::standardize', 'domain::transaction']
    )
}}

-- F12 (spec 2026-08-18): raw_fmp_ohlcv_non_split_adjusted đổi tên cột adj_open/adj_high/
-- adj_low/adj_close -> open/high/low/close (giá THÔ chưa điều chỉnh split, không phải giá
-- đã điều chỉnh -- FMP đặt tên field trả về là adj* một cách gây hiểu nhầm). `open` và
-- `close` đã kiểm tra không nằm trong danh sách từ khoá reserved của Trino nên không cần
-- quote.
with ohlcv_src as (
    select
        symbol,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_ohlcv_non_split_adjusted') }}
    where symbol is not null and trade_date is not null
),

-- thanh giá biết được lúc đóng cửa phiên đó.
{{ bitemporal_version_chain(
    source_cte='ohlcv_src',
    key_cols=['symbol', 'trade_date'],
    fingerprint_cols=['open', 'high', 'low', 'close', 'volume'],
    first_known_from_expr='trade_date',
    pit_basis='observation',
    round_to_trading_day=false
) }}
-- round_to_trading_day=false ở đây bắt buộc, không phải tuỳ chọn: std_cm_trading_day.sql:12
-- dựng lịch giao dịch từ chính bảng này, nên ref('std_cm_next_trading_day') tạo chu trình DAG
-- và dbt từ chối build. Làm tròn cũng là phép rỗng chứng minh được — trade_date tự nó đã là ngày giao dịch.

select
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

