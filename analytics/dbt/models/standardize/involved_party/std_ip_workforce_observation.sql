{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ip_workforce_observation',
        tags=['layer::standardize', 'domain::involved_party']
    )
}}

-- KNOWN BLOCKED: raw_fmp_employee_count does not exist in Iceberg yet (source endpoint
-- blocked by current FMP subscription tier -- see staging-regen-scoped memory). This model
-- will hard-fail `dbt build` (TABLE_NOT_FOUND) until that raw table lands. Accepted,
-- documented limitation -- not a bug in this model's own logic. Revisit after FMP upgrade.
with employee_count_src as (
    select
        cik,
        period_end_date,
        filing_date,
        cast(acceptance_time as date) as accepted_date,
        employee_count,
        form_type,
        filing_url,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_employee_count') }}
),

-- Bảng này có measure thật là employee_count; metadata hồ sơ (form_type,
-- filing_url) đổi mà số nhân sự không đổi thì đó không phải quan sát mới, đưa
-- chúng vào fingerprint sẽ sinh phiên bản giả mỗi lần vendor đổi URL.
{{ bitemporal_version_chain(
    source_cte='employee_count_src',
    key_cols=['cik', 'period_end_date'],
    fingerprint_cols=['employee_count', 'filing_date', 'accepted_date'],
    fingerprint_excluded_cols=['form_type', 'filing_url'],
    first_known_from_expr='coalesce(accepted_date, filing_date)',
    pit_basis='filing'
) }}

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
    pit_basis
from _bvc_versioned


