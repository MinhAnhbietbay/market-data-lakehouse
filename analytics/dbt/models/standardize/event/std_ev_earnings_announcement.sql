{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_earnings_announcement',
        tags=['layer::standardize', 'domain::event']
    )
}}

-- UPDATE (2026-08-20, Task 10): raw_fmp_earnings now exists in Iceberg with real
-- data (625 rows measured) -- the KNOWN BLOCKED note below is stale as of the
-- staging-full-fidelity rebuild and no longer describes the current state.
-- Original note (2026-08-17, spec N-27), kept for history: stg_fmp_earnings did
-- not exist in Iceberg yet (source endpoint /earnings, availability: unknown in
-- configs/sources/data_sources.yaml, not a confirmed paid_required tier
-- restriction); the model hard-failed dbt build with TABLE_NOT_FOUND until that
-- staging table landed.
with earnings_src as (
    select
        symbol,
        announcement_date,
        eps_actual,
        eps_estimated,
        revenue_actual,
        revenue_estimated,
        last_updated,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_earnings') }}
    where symbol is not null and announcement_date is not null
),

-- last_updated là timestamp metadata của vendor, KHÔNG phải measure. Đưa nó vào
-- fingerprint thì mỗi lần FMP chạm bản ghi (kể cả eps/revenue y nguyên) lại sinh
-- một phiên bản giả với known_from = run_date -- đúng chế độ hỏng mà
-- attr_fingerprint.sql:16-17 cảnh báo. Loại trừ có chủ đích, khai tường minh.
{{ bitemporal_version_chain(
    source_cte='earnings_src',
    key_cols=['symbol', 'announcement_date'],
    fingerprint_cols=['eps_actual', 'eps_estimated', 'revenue_actual', 'revenue_estimated'],
    fingerprint_excluded_cols=['last_updated'],
    first_known_from_expr='announcement_date',
    pit_basis='filing'
) }}

select
    symbol,
    announcement_date,
    eps_actual,
    eps_estimated,
    revenue_actual,
    revenue_estimated,
    last_updated,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned


