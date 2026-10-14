{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_corporate_insider',
        tags=['layer::curated', 'star_table']
    )
}}

-- RỖNG tại thời điểm dựng: endpoint insider bị gate FMP tier. Fact tương ứng
-- thuộc đợt 3b. Nợ N-3.

select
    reporting_cik,
    person_name,
    current_timestamp as loaded_at
from {{ ref('std_ip_corporate_insider') }}
