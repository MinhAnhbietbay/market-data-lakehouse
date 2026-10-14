{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_lo_exchange_venue',
        tags=['layer::standardize', 'domain::location']
    )
}}

-- analytics/dbt/models/standardize/location/std_lo_exchange_venue.sql
with codes as (
    select exchange_code from {{ source('raw', 'raw_fmp_profile') }}
    union
    select exchange_code from {{ source('raw', 'raw_fmp_sec_profile') }}
    union
    select exchange_code from {{ source('raw', 'raw_fmp_delisted_companies') }}
    union
    select exchange_code from {{ source('raw', 'raw_fmp_index_list') }}
)

select distinct exchange_code
from codes
where exchange_code is not null
