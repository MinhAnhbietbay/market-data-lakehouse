{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_cm_macro_observation',
        tags=['layer::standardize', 'domain::common']
    )
}}

-- F10a (spec 2026-08-18): 3 staging table cũ (stg_fmp_economic_{cpi,federal_funds,gdp})
-- gộp thành một raw_fmp_economic_indicators (endpoint economic_indicators, scope
-- macro_series). Không còn hợp nhiều nguồn nữa -- đọc thẳng một source, dedupe vẫn
-- cần vì cùng (series_name, observation_date) có thể có nhiều run_date.
with economic_indicators_ranked as (
    select
        series_name,
        observation_date,
        observation_value,
        run_date,
        landing_key,
        row_number() over (
            partition by series_name, observation_date, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_economic_indicators') }}
    where series_name is not null and observation_date is not null
),

economic_indicators_latest as (
    select
        series_name,
        observation_date,
        observation_value,
        run_date,
        landing_key
    from economic_indicators_ranked
    where rn = 1
),
{{ bitemporal_version_chain(
    source_cte='economic_indicators_latest',
    key_cols=['series_name', 'observation_date'],
    fingerprint_cols=['observation_value'],
    first_known_from_expr='observation_date',
    pit_basis='observation'
) }}

select
    series_name,
    observation_date,
    observation_value,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

