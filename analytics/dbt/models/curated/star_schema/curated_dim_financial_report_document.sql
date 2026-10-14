{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_financial_report_document',
        tags=['layer::curated', 'star_table']
    )
}}

-- Sổ đăng ký tài liệu báo cáo tài chính. Là DIM, không phải fact: nguồn không
-- có một số đo nào, chỉ có symbol/fiscal_year/period + hai link (spec §1).
--
-- Không có cột ngày nên KHÔNG phân giải as-of được. Nối dim_security bằng symbol
-- TRẦN. Phép nối này đúng có điều kiện: dim_security là Type 1, không có cột
-- thời gian, nên tính đúng đắn dựa hoàn toàn vào symbol là duy nhất trong dim --
-- đo 2026-08-28: 82 dòng / 82 symbol phân biệt, đúng. Hỏng khi mã bị thu hồi rồi
-- cấp lại cho công ty khác (nợ N-j).

select
    symbol,
    fiscal_year,
    period,
    link_xlsx,
    link_json,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis,
    {{ latest_known_flag(['symbol', 'fiscal_year', 'period']) }} as is_latest_known,
    current_timestamp as loaded_at
from {{ ref('std_do_financial_report_document') }}
