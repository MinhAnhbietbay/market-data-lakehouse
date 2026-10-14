{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_beneficial_ownership_position',
        tags=['layer::curated', 'star_table']
    )
}}

-- Fact vị thế sở hữu hưởng lợi (beneficial ownership).

with src as (
    select * from {{ ref('std_pr_beneficial_ownership_position') }}
),

resolved as (
    select
        s.symbol,
        s.cik,
        s.cusip,
        s.sole_voting_power,
        s.shared_voting_power,
        s.sole_dispositive_power,
        s.shared_dispositive_power,
        s.amount_beneficially_owned,
        s.filing_date,
        s.source_run_date,
        s.known_from,
        s.known_from_basis,
        s.pit_basis,
        _sec.share_class_figi
    from src as s
    {{ resolve_security_at('s', 'symbol', 'filing_date') }}
)

select
    *,
    {{ latest_known_flag(['symbol', 'cik']) }} as is_latest_known,
    current_timestamp as loaded_at
from resolved
