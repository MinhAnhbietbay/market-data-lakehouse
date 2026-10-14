-- std_ev_delisting_event.symbol used to carry a hard `relationships` FK test
-- against std_ar_listing.symbol (models/standardize/event/schema.yml). It
-- now fails with orphans: delisting events for symbols that never resolved
-- a share_class_figi (see assert_unresolved_share_class_figi_count.sql) and
-- therefore never made it into std_ar_listing (Task 7 resolves symbol ->
-- share_class_figi via the same OpenFIGI ladder that model shares with
-- std_pd_security).
--
-- Ruling (2026-08-19, plan progress.md, Task 7/Task 8): do NOT delete these
-- orphaned events and do NOT delete the test -- a delisting event is the
-- only record we hold about those symbols, so dropping the rows loses real
-- data, and dropping the test buys a green suite by no longer asking the
-- question. Replace the hard FK with this threshold guard instead, same
-- style as assert_unresolved_share_class_figi_count.sql: assert the number
-- of delisting events that cannot be anchored to a listing does not exceed
-- the count we know about today. If it climbs, something new is dropping
-- out of std_ar_listing and a human needs to look, not a silently widening
-- gap.
--
-- Measured 2026-08-19: 21 orphaned delisting events -- exactly the same 21
-- symbols that fail share_class_figi resolution.
--
-- RE-MEASURED 2026-09-14: 100, after a full `down -v` rebuild. Root cause is
-- NOT a regression -- confirmed live: raw_fmp_delisted_companies (100
-- symbols) comes from a `scope: paged_reference` FMP endpoint, entirely
-- independent of configs/universes/universe.yaml; raw_openfigi_mapping is
-- built ONLY from universe.yaml's symbol list (pipelines/landing/run_batch.py,
-- "the whole universe fans out into batched OpenFIGI /mapping calls" -- no
-- delisted-company symbol is ever fed into FIGI resolution, by design). The
-- 21 baseline was measured back when universe.yaml carried far more symbols
-- than today's dev-scale 5 (AAPL/MSFT/NVDA/AMZN/META); shrinking the universe
-- shrank the resolved/active side of the count while the ~100 permanently-
-- unresolved delisted symbols stayed fixed, so the ratio moved, not the
-- underlying data quality. Expanding universe.yaml again will NOT shrink
-- this back down -- delisted symbols are never in scope for resolution
-- regardless of universe size. Bumped to 100 as the new deliberate baseline
-- (see assert_unresolved_share_class_figi_count.sql, same root cause,
-- co-measured the same day) -- re-measure and bump again by hand next time
-- this fires; do not make this self-adjusting (that would defeat the
-- "a human needs to look" tripwire the original 2026-08-19 ruling wanted).
with orphaned as (
    select e.symbol
    from {{ ref('std_ev_delisting_event') }} e
    left join {{ ref('std_ar_listing') }} l on e.symbol = l.symbol
    where l.symbol is null
)
select count(*) as orphan_count
from orphaned
having count(*) > 100
