{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_enterprise_value',
        tags=['layer::curated', 'star_table']
    )
}}

-- HẠT LÀ FISCAL-YEAR-END, KHÔNG PHẢI QUÝ. Nguồn raw_fmp_enterprise_values
-- landed theo period: annual và không có cột fiscal_year/period -- khác mọi
-- bảng khác trong slice ac (accounting/schema.yml:240-249). Join tương lai chỉ
-- dựa trên period_end_date tối đa khớp 1 trong 4 quý. Đừng so bảng này với
-- curated_fact_owner_earnings (theo quý) rồi kết luận dữ liệu thủng.
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
    select * from {{ ref('std_ac_enterprise_value') }}
),

resolved as (
    select
        s.symbol,
        s.period_end_date,
        s.stock_price,
        s.number_of_shares,
        s.minus_cash_and_cash_equivalents,
        s.add_total_debt,
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
    {{ latest_known_flag(['symbol', 'period_end_date']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
