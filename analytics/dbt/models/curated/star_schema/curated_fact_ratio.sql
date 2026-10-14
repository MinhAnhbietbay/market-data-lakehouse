{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_ratio',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact tỷ số tài chính từ vendor.
--
-- BẢNG VENDOR TỰ TÍNH. curated tính lại các đại lượng này từ statement PIT ở
-- lớp panel (curated_alpha_fields_daily). Hai bên có thể lệch; bảng này để đối
-- chiếu, KHÔNG phải số chính thức. Test đo độ lệch là nợ N-b, ngoài phạm vi
-- đợt 3b theo quyết định người dùng (spec §8).

with src as (
    select * from {{ ref('std_ac_ratio') }}
),

resolved as (
    select
        s.symbol,
        s.fiscal_year,
        s.period,
        s.period_end_date,
        s.reported_currency,
        s.price_to_fair_value,
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
