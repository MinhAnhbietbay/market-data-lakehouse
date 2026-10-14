-- Singular test: guard the number of domain symbols that fail to resolve a
-- share_class_figi. Domain = the same 3-way union std_pd_security draws from
-- (raw_fmp_profile ∪ raw_fmp_sec_profile ∪ raw_fmp_delisted_companies).
-- Resolution ladder (ISIN > CUSIP > TICKER, dedupe raw_openfigi_mapping to
-- one row per input_id_value) is NOT reimplemented here -- it comes from
-- the shared resolve_share_class_figi() macro (analytics/dbt/macros/), the
-- same one std_pd_security and std_ar_listing call, so this guard cannot
-- drift from the model it protects. Matching input_id_value against
-- isin/cusip/symbol (including the dot-suffix TICKER case, e.g. symbol
-- 'AII.TO' was queried as 'AII') is per-caller join-key responsibility by
-- the macro's own contract, not ladder logic, so it stays here.
--
-- Measured 2026-08-19: 21 of 105 domain symbols have no share_class_figi.
-- That is EXPECTED, not a bug: 6 warrants + 1 preferred series
-- (security_type = 'PUBLIC') legitimately have no share class at all (they
-- are excluded from resolution by design -- see schema.yml on
-- std_pd_security.security_type), and the remaining 14 are genuine OpenFIGI
-- "No identifier found." misses -- mostly non-US names FMP cannot serve on
-- the current plan tier, plus retired/reused tickers.
--
-- std_pd_security's share_class_figi PK is not_null: unresolved symbols stay
-- OUT of that model rather than getting a fabricated key. This test is the
-- tripwire that stops that exclusion from silently growing -- if the
-- unresolved count exceeds 21, something changed (a resolution regression,
-- a new source-domain symbol OpenFIGI can't place, etc.) and needs a human
-- to look, not a widening silent gap.
--
-- RE-MEASURED 2026-09-14: 100, after a full `down -v` rebuild. Same root
-- cause as assert_delisting_event_orphan_count.sql (co-measured the same
-- day, read that file's comment for the full story): raw_fmp_delisted_
-- companies is a `scope: paged_reference` FMP endpoint independent of
-- universe.yaml, while OpenFIGI resolution only ever runs over universe.
-- yaml's own symbol list -- so every one of the ~100 delisted symbols in
-- `domain` here is permanently unresolvable by this pipeline's design, not a
-- regression. The 21 baseline dates from when universe.yaml carried far more
-- symbols than today's dev-scale 5; shrinking the universe shrank the
-- resolved side of the ratio while the fixed delisted-symbol count stayed
-- put. Expanding universe.yaml again will NOT bring this back toward 21 --
-- delisted symbols never enter FIGI resolution regardless of universe size.
-- Bumped to 100 as the new deliberate baseline -- re-measure and bump again
-- by hand next time this fires; do not make this self-adjusting (that would
-- defeat the "a human needs to look" tripwire the original ruling wanted).
with domain as (
    select symbol
    from {{ source('raw', 'raw_fmp_profile') }}
    where symbol is not null
    union
    select symbol
    from {{ source('raw', 'raw_fmp_sec_profile') }}
    where symbol is not null
    union
    select symbol
    from {{ source('raw', 'raw_fmp_delisted_companies') }}
    where symbol is not null
),

identifiers as (
    select symbol, cusip, isin
    from (
        select
            coalesce(p.symbol, s.symbol) as symbol,
            p.cusip,
            coalesce(s.isin, p.isin) as isin
        from (
            select symbol, cusip, isin
            from (
                select
                    symbol, cusip, isin,
                    row_number() over (partition by symbol order by ingested_at desc, landing_key desc) as rn
                from {{ source('raw', 'raw_fmp_profile') }}
                where symbol is not null
            ) ranked
            where rn = 1
        ) p
        full outer join (
            select symbol, isin
            from (
                select
                    symbol, isin,
                    row_number() over (partition by symbol order by ingested_at desc, landing_key desc) as rn
                from {{ source('raw', 'raw_fmp_sec_profile') }}
                where symbol is not null
            ) ranked
            where rn = 1
        ) s on p.symbol = s.symbol
    ) combined
),

figi_resolved as (
    {{ resolve_share_class_figi() }}
),

unresolved as (
    select d.symbol
    from domain d
    left join identifiers i on i.symbol = d.symbol
    left join figi_resolved f
      on f.input_id_value in (i.isin, i.cusip, d.symbol, split_part(d.symbol, '.', 1))
    where f.input_id_value is null
)

select count(*) as unresolved_count
from unresolved
having count(*) > 100
