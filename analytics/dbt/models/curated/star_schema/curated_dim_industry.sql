{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_industry',
        tags=['layer::curated', 'star_table']
    )
}}

-- RỖNG tại thời điểm dựng: endpoint /available-industries trả HTTP 402
-- (đo 2026-08-20). Nợ N-2.

select
    industry_key,
    industry,
    current_timestamp as loaded_at
from {{ ref('std_gr_industry') }}
