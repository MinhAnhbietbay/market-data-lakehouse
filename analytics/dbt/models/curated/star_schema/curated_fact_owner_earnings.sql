{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_owner_earnings',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact thu nhập chủ sở hữu (owner earnings). 25 dòng đo 2026-08-28.
--
-- BẢNG VENDOR TỰ TÍNH. curated tính lại các đại lượng này từ statement PIT ở
-- lớp panel (curated_alpha_fields_daily). Hai bên có thể lệch; bảng này để đối
-- chiếu, KHÔNG phải số chính thức. Test đo độ lệch là nợ N-b, ngoài phạm vi
-- đợt 3b theo quyết định người dùng (spec §8).
--
-- share_class_figi phân giải as-of theo period_end_date: đo được 25/25 dòng
-- phân giải được, vì std_ar_listing.valid_from lùi tới 1980-12-12. Cột vẫn
-- cho phép NULL theo luật spec §4.1 -- sự kiện rơi vào khe giữa hai phiên bản
-- niêm yết trả NULL, đó là câu trả lời đúng, không phải lỗi cần vá.

with src as (
    select * from {{ ref('std_ac_owner_earnings') }}
),

resolved as (
    select
        s.symbol,
        s.fiscal_year,
        s.period,
        s.period_end_date,
        s.reported_currency,
        s.maintenance_capex,
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
    {{ latest_known_flag(['symbol', 'fiscal_year', 'period']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
