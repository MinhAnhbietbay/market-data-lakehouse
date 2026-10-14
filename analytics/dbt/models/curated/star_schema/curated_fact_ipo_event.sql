{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_ipo_event',
        tags=['layer::curated', 'star_table']
    )
}}

-- Vẫn resolve, nhưng DỰ KIẾN NULL GẦN NHƯ TOÀN BỘ: ngày IPO có hiệu lực thường
-- nằm TRƯỚC phiên bản niêm yết đầu tiên của chính mã đó, nên rơi ra ngoài mọi
-- cửa sổ [valid_from, valid_to). Ghi trước để lần đầu có dữ liệu thật, NULL hàng
-- loạt không bị đọc nhầm thành lỗi phân giải (spec §4.2).

with src as (
    select * from {{ ref('std_ev_ipo_event') }}
),

resolved as (
    select
        s.symbol,
        s.effectiveness_date,
        s.source_run_date,
        s.known_from,
        s.known_from_basis,
        s.pit_basis,
        _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'effectiveness_date') }}
)

select
    *,
    {{ latest_known_flag(['symbol', 'effectiveness_date']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
