{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_cm_currency',
        tags=['layer::standardize', 'domain::common']
    )
}}

with currencies as (
    select
        currency_code,
        currency_name,
        minor_unit
    from {{ ref('seed_iso_4217_currency') }}
)

select * from currencies
