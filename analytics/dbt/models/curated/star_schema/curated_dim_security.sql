{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_security',
        tags=['layer::curated', 'star_table']
    )
}}

-- Dim chứng khoán, Type 1: nguồn KHÔNG có cột lịch sử nào (không valid_from,
-- không known_from) -- đây là thuộc tính tĩnh, không phải bảng biến thiên.
--
-- Khoá là share_class_figi chứ không phải symbol: tên bị delist không có
-- composite FIGI, share_class_figi là khoá duy nhất sống sót qua delisting và
-- đổi mã (spec §2.1). symbol giữ làm thuộc tính.
--
-- 82 dòng, trong đó cik NULL ở 35 dòng (ETF/fund không có mã đăng ký SEC).

select
    share_class_figi,
    symbol,
    cik,
    isin,
    cusip,
    open_figi_composite,
    security_type,
    is_etf,
    is_adr,
    is_fund,
    asset_type,
    market_sector,
    price_currency,
    current_timestamp as loaded_at
from {{ ref('std_pd_security') }}
