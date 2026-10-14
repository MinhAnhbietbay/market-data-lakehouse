{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_market_index',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển ký hiệu chỉ số thị trường. CHỈ có ký hiệu và tên -- KHÔNG có mức
-- chỉ số. Vì vậy các gap vix/sp500_index/dollar_index_dxy trong
-- configs/curated/field_sources.yaml không đóng được bằng bảng này. 425 dòng.

select
    entity_name_key,
    entity_name,
    symbol,
    current_timestamp as loaded_at
from {{ ref('std_pd_market_index') }}
