{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_company',
        tags=['layer::curated', 'star_table']
    )
}}

-- Dim công ty, Type 2: mang nguyên valid_from/valid_to/is_current lên. BI cần
-- ảnh hiện tại thì lọc is_current; cần "sector tại 2023" thì vẫn còn dữ liệu.
-- Ép về Type 1 tiết kiệm ba cột và vứt toàn bộ lịch sử (spec §2.1).
--
-- Đo 2026-08-28: 43 dòng / 42 cik / 42 dòng is_current -- đúng một công ty có
-- 2 phiên bản.
--
-- LƯU Ý cho hạ nguồn: cik ĐƠN không duy nhất. Join theo cik phải kèm
-- is_current = true hoặc một vị từ as-of, nếu không sẽ nhân dòng (spec §4).
--
-- sector/industry ở đây là taxonomy riêng của FMP, KHÔNG phải GICS. Việc sửa
-- nhãn vendor_sector/vendor_industry trong configs/fields/dimension.yaml thuộc
-- đợt 3c, không làm ở đây.

select
    cik,
    company_name,
    website,
    description,
    ceo,
    full_time_employees,
    phone,
    address,
    city,
    state,
    postal_code,
    mailing_address,
    state_of_incorporation,
    country_code,
    currency_code,
    industry_key,
    industry,
    sector_key,
    sector,
    registrant_name,
    fiscal_year_end,
    tax_identification_number,
    sic_code,
    valid_from,
    valid_to,
    is_current,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis,
    current_timestamp as loaded_at
from {{ ref('std_ip_legal_entity') }}
