{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ar_index_membership',
        tags=['layer::standardize', 'domain::arrangement']
    )
}}

-- Contract và source (raw_fmp_index_constituent) ĐÃ có cho /sp500-constituent +
-- /historical-sp500-constituent (F14, spec 2026-08-18 -- renamed from /dowjones-constituent
-- + /historical-dowjones-constituent, universe.yaml khai benchmark: SPY).
-- Bảng hiện rỗng vì endpoint trả 402 (paid_required) trên gói FMP hiện tại (enabled: false).
-- Điều kiện mở khoá: nâng gói FMP rồi chạy lại landing + dbt build.

-- Thành viên chỉ số là quan hệ (chỉ số × chứng khoán × thời gian). Bản cũ
-- thiếu hẳn cột chỉ số và mang kèm thuộc tính của công ty, khác grain hoàn toàn.

with membership_src as (
    select
        index_symbol,
        symbol,
        change_date,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_index_constituent') }}
    where index_symbol is not null
      and symbol is not null
),

{{ bitemporal_version_chain(
    source_cte='membership_src',
    key_cols=['index_symbol', 'symbol'],
    fingerprint_cols=['change_date'],
    first_known_from_expr='change_date',
    pit_basis='observation'
) }},

with_intervals as (
    select
        index_symbol,
        symbol,
        case when prev_hash is null then coalesce(change_date, run_date) else run_date end as valid_from,
        source_run_date,
        known_from,
        known_from_basis,
        pit_basis
    from _bvc_versioned
),

with_next as (
    select
        w.*,
        lead(valid_from) over (
            partition by index_symbol, symbol
            order by valid_from
        ) as next_valid_from
    from with_intervals w
)

select
    index_symbol,
    symbol,
    valid_from,
    next_valid_from as valid_to,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from with_next
