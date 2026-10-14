{{
    config(
        materialized='table',
        partition_by=['years(ex_dividend_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_cash_dividend',
        tags=['layer::standardize', 'domain::event']
    )
}}

with dividends_src as (
    select
        symbol,
        ex_dividend_date,
        dividend,
        record_date,
        payment_date,
        declaration_date,
        frequency,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_dividends') }}
    where symbol is not null and ex_dividend_date is not null
),

-- declaration_date là ngày công ty công bố cổ tức -- known_from chuẩn nhất, và
-- từ 2026-08-23 là lựa chọn chính: neo vào ex_dividend_date làm cổ tức VÔ HÌNH
-- với backtest trong toàn bộ khoảng từ lúc công bố tới ngày ex.
-- 15/247 dòng đo được (2026-08-20) có declaration_date null -- toàn bộ là cổ
-- tức lịch sử cũ (AAPL trước 1996) mà FMP không trả về declaration_date.
-- coalesce fallback về ex_dividend_date cho đúng 15 dòng đó (luôn not_null --
-- lọc ở subquery bên trên, và ex_dividend_date >= declaration_date theo định
-- nghĩa) nên vẫn bảo thủ ở chỗ cần bảo thủ, thay vì bảo thủ cho cả 247 dòng.
-- macro tự bọc least(..., run_date) nên ngày công bố ở tương lai vẫn bị kẹp.
{{ bitemporal_version_chain(
    source_cte='dividends_src',
    key_cols=['symbol', 'ex_dividend_date'],
    fingerprint_cols=['dividend', 'record_date', 'payment_date', 'declaration_date', 'frequency'],
    first_known_from_expr='coalesce(declaration_date, ex_dividend_date)',
    pit_basis='observation'
) }}

select
    symbol,
    ex_dividend_date,
    dividend,
    record_date,
    payment_date,
    declaration_date,
    frequency,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned


