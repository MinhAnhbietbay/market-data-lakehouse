{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_bridge_index_membership',
        tags=['layer::curated', 'star_table']
    )
}}

-- Bảng cầu thành viên chỉ số thị trường (bridge).

with src as (
    select * from {{ ref('std_ar_index_membership') }}
),

resolved as (
    select
        s.index_symbol,
        s.symbol,
        s.valid_from,
        s.valid_to,
        s.source_run_date,
        s.known_from,
        s.known_from_basis,
        s.pit_basis,
        _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'valid_from') }}
)

select
    *,
    {{ latest_known_flag(['index_symbol', 'symbol', 'valid_from']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
