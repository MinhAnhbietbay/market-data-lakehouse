{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_sector',
        tags=['layer::curated', 'star_table']
    )
}}

-- RỖNG tại thời điểm dựng: endpoint /available-sectors trả HTTP 402 trên gói
-- FMP hiện tại (đo 2026-08-20). Vẫn dựng theo schema -- endpoint bị chặn không
-- phải lý do dừng thiết kế (spec §2.3). Nợ N-1.

select
    sector_key,
    sector,
    current_timestamp as loaded_at
from {{ ref('std_gr_sector') }}
