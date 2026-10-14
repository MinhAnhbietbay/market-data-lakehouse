{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_sic_code',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển mã SIC. Tên nguồn (std_cm_industry_classification_system) hứa một
-- "hệ phân loại ngành" chung chung; nội dung thật là SIC, nên dim gọi đúng
-- thứ nó chứa (spec §2.2). 444 dòng.

select
    sic_code,
    sic_description,
    sic_group,
    current_timestamp as loaded_at
from {{ ref('std_cm_industry_classification_system') }}
