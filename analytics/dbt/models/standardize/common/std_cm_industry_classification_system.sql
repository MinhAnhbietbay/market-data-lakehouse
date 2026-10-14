{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_cm_industry_classification_system',
        tags=['layer::standardize', 'domain::common']
    )
}}

with sic_codes as (
    select
        cast(sic_code as string) as sic_code,
        sic_description,
        sic_group
    from {{ ref('seed_sic_code') }}
)

select * from sic_codes
