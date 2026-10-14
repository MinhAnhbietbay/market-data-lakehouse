{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_currency',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển tiền tệ cho BI. Promotion 1:1 từ standardize -- giữ nguyên tên cột,
-- không biến đổi (spec §3.5). Không partition_by: 178 dòng.

select
    currency_code,
    currency_name,
    minor_unit,
    current_timestamp as loaded_at
from {{ ref('std_cm_currency') }}
