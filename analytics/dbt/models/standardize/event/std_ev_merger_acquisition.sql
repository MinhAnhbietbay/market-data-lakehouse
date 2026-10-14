{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_merger_acquisition',
        tags=['layer::standardize', 'domain::event']
    )
}}

-- analytics/dbt/models/standardize/event/std_ev_merger_acquisition.sql
-- KNOWN BLOCKED: raw_fmp_mergers_acquisitions does not exist in Iceberg yet. Source
-- endpoint switched from /mergers-acquisitions-latest to /mergers-acquisitions-search
-- (F7/F7b, spec 2026-08-18-intermediate-identity-spine-design.md §3 -- -search covers
-- both acquirer and target role and supports historical depth, -latest does not);
-- -search itself now confirmed HTTP 402 on the current FMP tier (not merely
-- "availability: unknown" as originally reported at N-27) -- accepted per R-a, 0 rows
-- until plan upgrade. This model will hard-fail `dbt build` (TABLE_NOT_FOUND) until
-- that raw table has real data. Accepted, documented limitation -- not a bug in
-- this model's own logic. Originally live-verified 2026-08-17 (spec N-27).
with mergers_acquisitions_src as (
    select
        symbol,
        cik,
        targeted_cik,
        targeted_symbol,
        coalesce(targeted_cik, targeted_symbol, '__UNKNOWN__') as targeted_party_key,
        announcement_date,
        accepted_date,
        filing_url,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_mergers_acquisitions') }}
    where symbol is not null and cik is not null and announcement_date is not null
),

{{ bitemporal_version_chain(
    source_cte='mergers_acquisitions_src',
    key_cols=['symbol', 'cik', 'announcement_date', 'targeted_party_key'],
    fingerprint_cols=['accepted_date', 'filing_url', 'targeted_cik', 'targeted_symbol'],
    first_known_from_expr='run_date',
    first_known_from_is_system_time=true,
    pit_basis='observation'
) }}

select
    symbol,
    cik,
    targeted_cik,
    targeted_symbol,
    targeted_party_key,
    announcement_date,
    accepted_date,
    filing_url,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned



