{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ar_listing',
        tags=['layer::standardize', 'domain::arrangement']
    )
}}

-- analytics/dbt/models/standardize/arrangement/std_ar_listing.sql
with profile_ranked as (
    select
        symbol, exchange_code, ipo_date, isin, run_date, landing_key,
        row_number() over (partition by symbol, run_date order by ingested_at desc, exchange_code) as rn
    from {{ source('raw', 'raw_fmp_profile') }}
    where symbol is not null
),

profile_latest as (
    select symbol, exchange_code, ipo_date, isin, run_date, landing_key
    from profile_ranked
    where rn = 1
),

sec_profile_ranked as (
    select
        symbol,
        exchange_code as sec_exchange_code,
        ipo_date as sec_ipo_date,
        isin as sec_isin,
        run_date,
        landing_key,
        row_number() over (partition by symbol, run_date order by ingested_at desc, exchange_code) as rn
    from {{ source('raw', 'raw_fmp_sec_profile') }}
    where symbol is not null
),

sec_profile_latest as (
    select
        symbol,
        sec_exchange_code,
        sec_ipo_date,
        sec_isin,
        run_date,
        landing_key
    from sec_profile_ranked
    where rn = 1
),

delisted_ranked as (
    select
        symbol,
        exchange_code as delisted_exchange_code,
        ipo_date as delisted_ipo_date,
        run_date,
        landing_key,
        row_number() over (partition by symbol, run_date order by ingested_at desc, delisted_date) as rn
    from {{ source('raw', 'raw_fmp_delisted_companies') }}
),

delisted_latest as (
    select
        symbol,
        delisted_exchange_code,
        delisted_ipo_date,
        run_date,
        landing_key
    from delisted_ranked
    where rn = 1
),

combined as (
    select
        coalesce(p.symbol, s.symbol, d.symbol) as symbol,
        -- sec_profile wins over profile over delisted_companies for every overlapping
        -- field (same precedence rule as std_ip_legal_entity's snapshot -- sec_profile
        -- is the more authoritative, SEC-sourced data; delisted_companies is lowest
        -- priority since it's a reference list, not a primary profile source).
        coalesce(s.sec_exchange_code, p.exchange_code, d.delisted_exchange_code) as exchange_code,
        s.sec_ipo_date,
        p.ipo_date,
        d.delisted_ipo_date,
        coalesce(s.run_date, p.run_date, d.run_date) as run_date,
        coalesce(s.landing_key, p.landing_key, d.landing_key) as landing_key,
        -- isin carried through only to resolve share_class_figi below: two of
        -- the 105 symbols (GUYGF, ORLA -- measured 2026-08-19) have no
        -- ticker-keyed OpenFIGI row at all, but do share an ISIN-keyed row
        -- with their TSX-listed cross-listing pair (GTWO.TO, OLA.TO).
        coalesce(s.sec_isin, p.isin) as isin
    from profile_latest p
    full outer join sec_profile_latest s on p.symbol = s.symbol and p.run_date = s.run_date
    full outer join delisted_latest d on coalesce(p.symbol, s.symbol) = d.symbol and coalesce(p.run_date, s.run_date) = d.run_date
),

-- Resolve symbol -> share_class_figi directly against raw_openfigi_mapping,
-- NOT through std_pd_security.symbol: that model (Task 6) now collapses
-- cross-listed pairs sharing one share class (GTWO.TO/GUYGF, OLA.TO/ORLA) to
-- a single canonical symbol, so joining Listing on it would silently drop
-- the other listing. The priority ladder itself is shared via the
-- resolve_share_class_figi() macro so it cannot drift from std_pd_security's.
figi_resolved as (
    {{ resolve_share_class_figi() }}
),

-- Join key is not always the plain symbol: TICKER-path OpenFIGI jobs were
-- sent with only the ticker prefix (e.g. symbol 'AII.TO' was queried as
-- 'AII'), so input_id_value for those rows has no exchange suffix --
-- split_part(...,'.',1) recovers the match. isin recovers the remainder that
-- has no ticker-keyed OpenFIGI row at all (GUYGF/ORLA) but does share an
-- ISIN-keyed row with their cross-listed pair. Measured 2026-08-19: this
-- resolves 84 of 105 symbols.
listing_src as (
    select
        f.share_class_figi,
        -- market_code is the country-composite level, NOT the venue. US venues
        -- (see pipelines/landing/figi_jobs.py's _US_EXCHANGES: NYSE, NASDAQ,
        -- AMEX, OTC, NYSEARCA, BATS, CBOE, US) all map to 'US'. For a
        -- dot-suffixed symbol (e.g. 'GTWO.TO'), the suffix after the dot stands
        -- in for the country composite -- this is a PROXY, not a real venue ->
        -- composite mapping (no such reference table exists in this repo yet;
        -- tracked debt). Non-US, non-dotted symbols fall back to 'US' since none
        -- exist in the current 105-symbol domain (measured 2026-08-19).
        case
            when c.exchange_code in ('NYSE','NASDAQ','AMEX','OTC','NYSEARCA','BATS','CBOE','US')
                then 'US'
            else coalesce(split_part(c.symbol, '.', 2), 'US')
        end as market_code,
        c.symbol,
        c.exchange_code,
        coalesce(c.sec_ipo_date, c.ipo_date, c.delisted_ipo_date) as ipo_date,
        c.run_date,
        c.landing_key
    from combined c
    -- figi_resolved dedupes to one row per input_id_value, NOT per symbol -- a
    -- single symbol can legitimately match up to three figi_resolved rows here
    -- (full ticker, ticker-prefix, isin), which would fan out into duplicate
    -- output rows per symbol. The ranking window below makes "one symbol -> one
    -- row" a STRUCTURAL guarantee rather than a fact about today's data, using the
    -- same ISIN > CUSIP > TICKER reliability ladder the macro itself ranks on
    -- (input_id_type), then preferring an exact symbol match over a ticker-
    -- prefix match over an isin match, then share_class_figi as a final
    -- deterministic tie-break.
    join figi_resolved f
      on f.input_id_value in (c.symbol, split_part(c.symbol, '.', 1), c.isin)
),

{{ bitemporal_version_chain(
    source_cte='listing_src',
    key_cols=['share_class_figi', 'market_code'],
    fingerprint_cols=['symbol', 'exchange_code'],
    first_known_from_expr='ipo_date',
    first_known_from_basis_label='assumed_since_ipo',
    pit_basis='observation'
) }},

versioned_intervals as (
    -- valid_from của phiên bản ĐẦU là ngày IPO (ngày niêm yết bắt đầu thật),
    -- không phải run_date đầu tiên (đó chỉ là ngày ta bắt đầu thu thập).
    -- Phiên bản sau lấy run_date của change-point: đó là thời điểm sớm nhất
    -- ta biết symbol/venue đã đổi.
    select
        share_class_figi,
        market_code,
        symbol,
        exchange_code,
        case when prev_hash is null then coalesce(ipo_date, run_date) else run_date end as valid_from,
        source_run_date,
        known_from,
        known_from_basis,
        pit_basis
    from _bvc_versioned
),

with_next_version as (
    -- Ranh giới [valid_from, valid_to) -- đóng trái, MỞ phải. Ngày một niêm
    -- yết kết thúc chính là ngày niêm yết kế tiếp bắt đầu; đóng cả hai đầu
    -- thì một sự kiện đúng ngày đó khớp HAI phiên bản.
    select
        v.*,
        lead(valid_from) over (
            partition by share_class_figi, market_code
            order by valid_from
        ) as next_valid_from
    from versioned_intervals v
),

with_delisting as (
    -- Join có ràng buộc ngày, KHÔNG phải bare symbol: một mã bị huỷ niêm yết
    -- hai lần (sàn cấp lại mã cũ) sẽ nhân bản dòng và làm vỡ chính unique
    -- test của bảng này.
    select
        w.*,
        min(e.delisted_date) as delisted_date
    from with_next_version w
    left join {{ ref('std_ev_delisting_event') }} e
      on e.symbol = w.symbol
     and e.delisted_date >= w.valid_from
     and (w.next_valid_from is null or e.delisted_date < w.next_valid_from)
    group by w.share_class_figi, w.market_code, w.symbol, w.exchange_code,
             w.valid_from, w.source_run_date, w.known_from, w.pit_basis, w.next_valid_from,
             w.known_from_basis
)

select
    share_class_figi,
    market_code,
    symbol,
    exchange_code,
    valid_from,
    coalesce(next_valid_from, delisted_date) as valid_to,
    (next_valid_from is null and delisted_date is null) as is_current,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from with_delisting
