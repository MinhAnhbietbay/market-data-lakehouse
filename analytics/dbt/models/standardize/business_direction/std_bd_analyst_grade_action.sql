{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_bd_analyst_grade_action',
        tags=['layer::standardize', 'domain::business_direction']
    )
}}

-- analytics/dbt/models/standardize/business_direction/std_bd_analyst_grade_action.sql
with grades_src as (
    select
        symbol,
        grade_date,
        upper(trim(grading_company)) as grading_company_key,
        grading_company,
        previous_grade,
        new_grade,
        action,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_grades') }}
    where symbol is not null and grade_date is not null and grading_company is not null
),

-- grading_company (chuỗi thô) CỐ Ý nằm ngoài fingerprint. Khoá đã chuẩn hoá
-- qua grading_company_key; vendor đổi hoa/thường giữa hai dòng cùng khoá
-- ("KeyBanc"/"Keybanc", "BGC Financial"/"BGC FInancial"), nên để chuỗi thô
-- trong fingerprint thì dòng được chọn đổi casing là sinh một version mới với
-- nội dung nghiệp vụ y hệt -- đo 2026-09-16 sau 3 ngày ingest: 34 dòng neo
-- known_from vào 09-14/09-15, chỉ 1 là sự kiện thật (MSFT Citizens).
--
-- order_by tường minh vì raw có 31 khoá trùng trong CÙNG landing file mỗi
-- run_date (FMP trả cả upgrade lẫn downgrade cùng ngày cùng hãng: AMZN
-- 2018-05-05 Argus). Tiebreaker mặc định của macro (`run_date desc,
-- landing_key desc`) hoà hoàn toàn ở đó -> Spark chọn tuỳ ý mỗi build ->
-- chuỗi version flip-flop upgrade/downgrade/upgrade, và __fam_analyst_event
-- neo từng bước vào known_from = run_date hôm đó -> analyst_upgrade_count
-- trong mart engine đếm sự kiện ma. Thứ tự theo cột nội dung không có ý nghĩa
-- nghiệp vụ (không biết dòng nào FMP "định" giữ); nó chỉ cần TẤT ĐỊNH để cùng
-- input cho cùng output qua mọi lần build.
{{ bitemporal_version_chain(
    source_cte='grades_src',
    key_cols=['symbol', 'grade_date', 'grading_company_key'],
    fingerprint_cols=['previous_grade', 'new_grade', 'action'],
    fingerprint_excluded_cols=['grading_company'],
    first_known_from_expr='grade_date',
    pit_basis='observation',
    order_by='run_date desc, landing_key desc, action desc, new_grade desc, previous_grade desc, grading_company desc'
) }}

select
    symbol,
    grade_date,
    grading_company_key,
    grading_company,
    previous_grade,
    new_grade,
    action,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

