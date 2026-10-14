{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_economic_data_release',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact công bố số liệu kinh tế vĩ mô.

with src as (
    select * from {{ ref('std_ev_economic_data_release') }}
)

select
    *,
    {{ latest_known_flag(['event_key', 'country_code', 'release_date']) }} as is_latest_known,
    current_timestamp as loaded_at
from src
