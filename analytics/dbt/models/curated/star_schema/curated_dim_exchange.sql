{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_exchange',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển sàn giao dịch. Nguồn chỉ có đúng một cột mã -- không có tên sàn,
-- đó là giới hạn của nguồn, không phải thiếu sót khi promote. 51 dòng.

select
    exchange_code,
    current_timestamp as loaded_at
from {{ ref('std_lo_exchange_venue') }}
