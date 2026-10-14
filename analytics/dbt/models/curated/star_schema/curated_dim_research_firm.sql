{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_research_firm',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển công ty nghiên cứu/phân tích. 149 dòng.

select
    research_firm_name_key,
    research_firm_name,
    current_timestamp as loaded_at
from {{ ref('std_ip_research_firm') }}
