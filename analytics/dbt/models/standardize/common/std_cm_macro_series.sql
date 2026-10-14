{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_cm_macro_series',
        tags=['layer::standardize', 'domain::common']
    )
}}

-- F10a (spec 2026-08-18): 3 staging table cũ (stg_fmp_economic_{cpi,federal_funds,gdp})
-- gộp thành một raw_fmp_economic_indicators (endpoint economic_indicators, scope
-- macro_series). Không còn hợp nhiều nguồn nữa -- đọc thẳng một source.
select distinct series_name
from {{ source('raw', 'raw_fmp_economic_indicators') }}
where series_name is not null
