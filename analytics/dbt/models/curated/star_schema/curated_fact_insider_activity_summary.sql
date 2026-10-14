{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_insider_activity_summary',
        tags=['layer::curated', 'star_table']
    )
}}

-- share_class_figi CÓ mặt nhưng NULL toàn bộ: nguồn chỉ có fiscal_year và
-- fiscal_quarter, KHÔNG có cột ngày nào. Suy một ngày từ quý tài chính rồi đưa
-- vào resolve_security_at là chế dữ liệu để làm vừa lòng một macro -- sai đúng
-- kiểu look-ahead mà đợt pit-correctness vừa đóng. NULL là câu trả lời trung
-- thực (spec §4.2).

with src as (
    select
        symbol,
        cik,
        fiscal_year,
        fiscal_quarter,
        acquired_transactions,
        disposed_transactions,
        total_acquired,
        total_disposed,
        total_purchases,
        total_sales,
        source_run_date,
        known_from,
        known_from_basis,
        pit_basis,
        cast(null as string) as share_class_figi
    from {{ ref('std_tx_insider_activity_summary') }}
)

select
    *,
    {{ latest_known_flag(['symbol', 'cik', 'fiscal_year', 'fiscal_quarter']) }} as is_latest_known,
    current_timestamp as loaded_at
from src
