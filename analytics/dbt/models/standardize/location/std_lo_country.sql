{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_lo_country',
        tags=['layer::standardize', 'domain::location']
    )
}}

with countries as (
    select
        country_code,
        country_name,
        region
    from {{ ref('seed_iso_3166_country') }}
)

select * from countries
