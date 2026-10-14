{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_tx_insider_transaction',
        tags=['layer::standardize', 'domain::transaction']
    )
}}

-- analytics/dbt/models/standardize/transaction/std_tx_insider_transaction.sql
-- Empty by design: /insider-trading/search has no raw contract yet
-- (enabled: false, paid_required -- N-12, see intermediate-tx-slice spec).
-- Columns match field_concept_map.yaml's Insider Transaction mapping so
-- downstream models can reference this table's shape today. Deliberately
-- excludes person_name (belongs to std_ip_corporate_insider, spec D6) and
-- has no FK on company_cik (spec D7).
select
    cast(null as string) as symbol,
    cast(null as date) as transaction_date,
    cast(null as string) as transaction_type,
    cast(null as string) as reporting_cik,
    cast(null as string) as company_cik,
    cast(null as bigint) as securities_owned,
    cast(null as string) as type_of_owner,
    cast(null as string) as acquisition_or_disposition,
    cast(null as string) as direct_or_indirect,
    cast(null as bigint) as securities_transacted,
    cast(null as decimal(18,8)) as price,
    cast(null as string) as security_name,
    cast(null as date) as filing_date,
    cast(null as string) as form_type,
    cast(null as string) as filing_url,
-- known_from PHẢI là filing_date, không phải transaction_date: thị trường biết
-- một giao dịch nội bộ vào lúc Form 4 được nộp, không phải lúc giao dịch xảy ra
-- (SEC cho phép trễ tới 2 ngày làm việc). Khai sai chỗ này mở lại look-ahead.
-- first_known_from_expr='filing_date', pit_basis='filing'. (spec 3b §5)
    cast(null as date) as source_run_date,
    cast(null as date) as known_from,
    cast(null as string) as known_from_basis,
    cast(null as string) as pit_basis
where false
