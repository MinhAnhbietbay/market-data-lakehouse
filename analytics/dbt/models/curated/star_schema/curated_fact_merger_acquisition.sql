{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_merger_acquisition',
        tags=['layer::curated', 'star_table']
    )
}}

-- Phân giải HAI vế: bên mua (symbol) và bên bị mua (targeted_symbol). Chỉ resolve
-- một vế là để lại một nửa sự kiện không nối được vào dim.
--
-- PHẢI tách hai CTE: resolve_security_at hardcode alias _sec, gọi hai lần trong
-- cùng một scope là trùng alias và Spark từ chối.

with src as (
    select * from {{ ref('std_ev_merger_acquisition') }}
),

acquirer as (
    select s.*, _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'announcement_date') }}
),

target as (
    select a.*, _sec.share_class_figi as targeted_share_class_figi
    from acquirer as a
    {{ resolve_security_at('a', 'targeted_symbol', 'announcement_date') }}
)

select
    *,
    {{ latest_known_flag(['symbol', 'cik', 'announcement_date', 'targeted_party_key']) }} as is_latest_known,
    current_timestamp as loaded_at
from target
