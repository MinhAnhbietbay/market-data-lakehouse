{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_pd_security',
        tags=['layer::standardize', 'domain::product']
    )
}}

-- analytics/dbt/models/standardize/product/std_pd_security.sql
with profile_ranked as (
    select
        symbol, cik, cusip, isin, is_etf, is_adr, is_fund,
        row_number() over (partition by symbol order by ingested_at desc, landing_key desc) as rn
    from {{ source('raw', 'raw_fmp_profile') }}
    where symbol is not null
),

profile_latest as (
    select symbol, cik, cusip, isin, is_etf, is_adr, is_fund
    from profile_ranked
    where rn = 1
),

sec_profile_ranked as (
    select
        symbol,
        cik as sec_cik,
        isin as sec_isin,
        open_figi_composite,
        security_type,
        is_etf as sec_is_etf,
        is_adr as sec_is_adr,
        is_fund as sec_is_fund,
        asset_type,
        market_sector,
        price_currency,
        row_number() over (partition by symbol order by ingested_at desc, landing_key desc) as rn
    from {{ source('raw', 'raw_fmp_sec_profile') }}
    where symbol is not null
),

sec_profile_latest as (
    select
        symbol,
        sec_cik,
        sec_isin,
        open_figi_composite,
        security_type,
        sec_is_etf,
        sec_is_adr,
        sec_is_fund,
        asset_type,
        market_sector,
        price_currency
    from sec_profile_ranked
    where rn = 1
),

delisted_latest as (
    select distinct symbol
    from {{ source('raw', 'raw_fmp_delisted_companies') }}
    where symbol is not null
),

combined as (
    select
        -- symbol: join-key equality makes coalesce order a no-op when all sides
        -- match (same reasoning as std_ar_listing's own symbol coalesce) -- NOT
        -- an instance of the sec-wins rule below. delisted_latest joins in here
        -- too, purely to keep this model's symbol domain aligned with
        -- std_ar_listing's (every Listing needs a Security row, even a
        -- delisted-only one) -- raw_fmp_delisted_companies has no
        -- security-identifier columns, so it contributes only the symbol, never
        -- an attribute value.
        coalesce(p.symbol, s.symbol, d.symbol) as symbol,
        -- sec_profile wins over profile for every overlapping field (same
        -- precedence rule as std_ip_legal_entity/std_ar_listing). Symbols that
        -- exist only in delisted_latest have no p/s row, so cik/isin/cusip/
        -- open_figi_composite/security_type/is_etf/is_adr/is_fund are all NULL
        -- for them -- intended, not a bug.
        coalesce(s.sec_cik, p.cik) as cik,
        coalesce(s.sec_isin, p.isin) as isin,
        p.cusip,
        s.open_figi_composite,
        s.security_type,
        coalesce(s.sec_is_etf, p.is_etf) as is_etf,
        coalesce(s.sec_is_adr, p.is_adr) as is_adr,
        coalesce(s.sec_is_fund, p.is_fund) as is_fund,
        s.asset_type,
        s.market_sector,
        s.price_currency
    from profile_latest p
    full outer join sec_profile_latest s on p.symbol = s.symbol
    full outer join delisted_latest d on coalesce(p.symbol, s.symbol) = d.symbol
),

-- Priority ladder (ISIN > CUSIP > TICKER) lives in the shared
-- resolve_share_class_figi() macro (analytics/dbt/macros/) so std_ar_listing
-- (Task 7) can use the identical resolution without a second, driftable copy.
figi_resolved as (
    {{ resolve_share_class_figi() }}
),

-- Join key is not always the symbol: TICKER-path OpenFIGI jobs were sent with
-- only the ticker prefix (e.g. symbol 'AII.TO' was queried as 'AII'), so
-- input_id_value for those rows has no exchange suffix. split_part(...,'.',1)
-- recovers the match for the 6 affected symbols measured 2026-08-19.
matched as (
    select
        c.symbol,
        c.cik,
        c.isin,
        c.cusip,
        c.open_figi_composite,
        c.security_type,
        c.is_etf,
        c.is_adr,
        c.is_fund,
        c.asset_type,
        c.market_sector,
        c.price_currency,
        f.share_class_figi
    from combined c
    join figi_resolved f
      on f.input_id_value in (c.isin, c.cusip, c.symbol, split_part(c.symbol, '.', 1))
),

-- share_class_figi identifies a SHARE CLASS, not a listing: the same equity
-- can be cross-listed under two different symbols (e.g. measured 2026-08-19,
-- GTWO.TO/GUYGF and OLA.TO/ORLA each resolve to one shared figi), which would
-- otherwise violate the PK's uniqueness. Collapse to one canonical row per
-- share_class_figi, preferring the symbol that actually carries profile/
-- sec_profile attribute data over a delisted-only stub with none, then
-- breaking ties alphabetically for determinism.
deduped as (
    select
        share_class_figi,
        symbol,
        cik,
        isin,
        cusip,
        open_figi_composite,
        security_type,
        is_etf,
        is_adr,
        is_fund,
        asset_type,
        market_sector,
        price_currency,
        row_number() over (
            partition by share_class_figi
            order by case when cik is not null then 0 else 1 end, symbol
        ) as rn
    from matched
)

select
    share_class_figi,
    symbol,
    cik,
    isin,
    cusip,
    open_figi_composite,
    security_type,
    is_etf,
    is_adr,
    is_fund,
    asset_type,
    market_sector,
    price_currency
from deduped
where rn = 1
