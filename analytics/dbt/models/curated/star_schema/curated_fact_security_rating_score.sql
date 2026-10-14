{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_security_rating_score',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact điểm thành phần xếp hạng chứng khoán (dạng long). 35 dòng đo 2026-08-28.
--
-- share_class_figi phân giải as-of theo rating_date: đo được 35/35 dòng
-- phân giải được, vì std_ar_listing.valid_from lùi tới 1980-12-12. Cột vẫn
-- cho phép NULL theo luật spec §4.1 -- sự kiện rơi vào khe giữa hai phiên bản
-- niêm yết trả NULL, đó là câu trả lời đúng, không phải lỗi cần vá.

with src as (
    select * from {{ ref('std_gr_security_rating_score') }}
),

resolved as (
    select
        s.symbol,
        s.rating_date,
        s.score_name,
        s.score_value,
        s.source_run_date,
        s.known_from,
        s.known_from_basis,
        s.pit_basis,
        _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'rating_date') }}
)

select
    *,
    {{ latest_known_flag(['symbol', 'rating_date', 'score_name']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
