{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_insider_transaction',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact giao dịch nội bộ (insider transactions).

with src as (
    select * from {{ ref('std_tx_insider_transaction') }}
),

resolved as (
    select
        s.symbol,
        s.transaction_date,
        s.transaction_type,
        s.reporting_cik,
        s.company_cik,
        s.securities_owned,
        s.type_of_owner,
        s.acquisition_or_disposition,
        s.direct_or_indirect,
        s.securities_transacted,
        s.price,
        s.security_name,
        s.filing_date,
        s.form_type,
        s.filing_url,
        s.source_run_date,
        s.known_from,
        s.known_from_basis,
        s.pit_basis,
        _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'transaction_date') }}
)

select
    *,
    {{ latest_known_flag(['reporting_cik', 'symbol', 'transaction_date', 'transaction_type']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
