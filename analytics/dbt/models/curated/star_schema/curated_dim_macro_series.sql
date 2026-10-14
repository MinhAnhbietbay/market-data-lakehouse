{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_macro_series',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển tên chuỗi vĩ mô hợp lệ. 12 dòng.

select
    series_name,
    current_timestamp as loaded_at
from {{ ref('std_cm_macro_series') }}
