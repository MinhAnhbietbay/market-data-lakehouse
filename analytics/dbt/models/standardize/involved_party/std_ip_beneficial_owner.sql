{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ip_beneficial_owner',
        tags=['layer::standardize', 'domain::involved_party']
    )
}}

-- Empty by design: /acquisition-of-beneficial-ownership has no raw
-- contract yet (enabled: false, paid_required).
select
    cast(null as string) as name_of_reporting_person,
    cast(null as string) as citizenship_or_place_of_organization,
    cast(null as string) as type_of_reporting_person
where false
