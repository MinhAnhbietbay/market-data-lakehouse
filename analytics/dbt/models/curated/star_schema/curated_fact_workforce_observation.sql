{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_workforce_observation',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact quan sát quy mô nhân sự. Fact DUY NHẤT đi trục pháp nhân (cik), không
-- phải trục chứng khoán -- nguồn không có symbol.
--
-- KHÔNG dùng resolve_entity_at, dù nó tồn tại và có vẻ đúng chỗ. Đo 2026-08-28:
-- 0/136 dòng phân giải được, vì std_ip_legal_entity.valid_from chỉ có hai giá
-- trị 2026-08-18/2026-08-19 -- đó là NGÀY INGEST, không phải ngày hiệu lực
-- nghiệp vụ, còn period_end_date ở đây lùi tới 1994-06-30. Dựng as-of bây giờ
-- là dựng một cột NULL 100% rồi gọi nó là PIT, tệ hơn không có cột vì nó trông
-- như đã giải quyết. Đổi sang as-of khi legal_entity có effective-dating thật
-- (nợ N-a, spec §4.4).
--
-- cik nối dim_company và hạ nguồn PHẢI kèm is_current = true hoặc vị từ as-of:
-- dim_company có 43 dòng / 42 cik, join trần theo cik sẽ nhân dòng.

select
    cik,
    period_end_date,
    filing_date,
    accepted_date,
    employee_count,
    form_type,
    filing_url,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis,
    {{ latest_known_flag(['cik', 'period_end_date']) }} as is_latest_known,
    current_timestamp as loaded_at
from {{ ref('std_ip_workforce_observation') }}
