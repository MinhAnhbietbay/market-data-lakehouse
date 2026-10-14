{{
    config(
        materialized='table',
        file_format='iceberg',
        schema='curated_pit_test',
        alias='__pit_test_delisting',
        tags=['layer::curated_pit_test', 'test']
    )
}}

-- Cắt delisting: lấy ngày sớm nhất trong std_ev_delisting_event -- nguồn CHUYÊN
-- BIỆT cho sự kiện huỷ niêm yết. Bất đối xứng có chủ đích -- để mã đã chết sinh
-- dữ liệu là lỗi lọt vào backtest, còn cắt sớm một ngày chỉ mất một quan sát.
-- Cắt theo mốc NGHIỆP VỤ (delisted_date) chứ không theo known_from của sự
-- kiện, nên tin delist đến trễ sẽ NULL-hoá hồi tố [delisted_date, known_from)
-- -- freeze test coi đây là miễn trừ (b), spec §7.2.1.
with delisted as (
    select symbol, min(delisted_date) as delist_date
    from {{ ref('pit_seed_delisting_event') }}
    group by symbol
),

spine as (
    select distinct trade_date as date, symbol
    from {{ ref('pit_seed_delisting_bar') }}
),

bars as (
    select trade_date as date, symbol, close
    from {{ ref('pit_seed_delisting_bar') }}
)

select
    s.date,
    s.symbol,
    case when d.delist_date is null or s.date <= d.delist_date then b.close end as close
from spine s
left join bars b on b.date = s.date and b.symbol = s.symbol
left join delisted d on d.symbol = s.symbol
