{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ip_corporate_insider',
        tags=['layer::standardize', 'domain::involved_party']
    )
}}

-- Empty by design: /insider-trading/search has no raw contract yet
-- (enabled: false, paid_required -- see staging-regen-scoped memory).
-- Columns match field_concept_map.yaml's Corporate Insider mapping so
-- downstream models can reference this table's shape today.
select
    cast(null as string) as reporting_cik,
    cast(null as string) as person_name
where false
