{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_analyst_grade_action',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact hành động xếp hạng của nhà phân tích. 5.805 dòng / 5 mã / 149 hãng
-- nghiên cứu (đo 2026-08-28), grade_date 2011-12-28 -> 2026-08-12.
--
-- share_class_figi phân giải as-of theo grade_date: đo được 5.805/5.805 dòng
-- phân giải được, vì std_ar_listing.valid_from lùi tới 1980-12-12. Cột vẫn
-- cho phép NULL theo luật spec §4.1 -- sự kiện rơi vào khe giữa hai phiên bản
-- niêm yết trả NULL, đó là câu trả lời đúng, không phải lỗi cần vá.

with src as (
    select * from {{ ref('std_bd_analyst_grade_action') }}
),

resolved as (
    select
        s.symbol,
        s.grade_date,
        s.grading_company_key,
        s.grading_company,
        s.previous_grade,
        s.new_grade,
        s.action,
        s.source_run_date,
        s.known_from,
        s.known_from_basis,
        s.pit_basis,
        _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'grade_date') }}
)

select
    *,
    {{ latest_known_flag(['symbol', 'grade_date', 'grading_company_key']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
