{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_country',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển quốc gia. Promotion 1:1, 249 dòng.

select
    country_code,
    country_name,
    region,
    current_timestamp as loaded_at
from {{ ref('std_lo_country') }}
