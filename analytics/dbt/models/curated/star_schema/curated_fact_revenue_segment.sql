{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_revenue_segment',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact phân khúc doanh thu theo địa lý (dạng long). 16 dòng đo 2026-08-28.
--
-- share_class_figi phân giải as-of theo period_end_date: đo được 16/16 dòng
-- phân giải được, vì std_ar_listing.valid_from lùi tới 1980-12-12. Cột vẫn
-- cho phép NULL theo luật spec §4.1 -- sự kiện rơi vào khe giữa hai phiên bản
-- niêm yết trả NULL, đó là câu trả lời đúng, không phải lỗi cần vá.

with src as (
    select * from {{ ref('std_ac_revenue_segment_breakdown') }}
),

resolved as (
    select
        s.symbol,
        s.fiscal_year,
        s.period,
        s.period_end_date,
        s.reported_currency,
        s.segment_name,
        s.segment_revenue,
        s.source_run_date,
        s.known_from,
        s.known_from_basis,
        s.pit_basis,
        _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'period_end_date') }}
)

select
    *,
    {{ latest_known_flag(['symbol', 'fiscal_year', 'period', 'segment_name']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
