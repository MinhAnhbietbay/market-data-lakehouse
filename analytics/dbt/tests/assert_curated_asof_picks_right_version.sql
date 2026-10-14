-- Test 3: As-of đúng phiên bản và chọn kỳ 2 bậc (spec §2, §7.2 mục 3).
-- 1. Cùng (symbol, period_end_date) có 2 version known_from khác nhau:
--    - Ngày ở giữa 2 known_from (2024-05-01) PHẢI trả bản cũ (known_from=2024-04-15, metric_value=100.0).
--    - Ngày sau known_from thứ 2 (2024-07-01) PHẢI trả bản mới (known_from=2024-06-15, metric_value=150.0).
-- 2. Bẫy chọn kỳ 2 bậc (TWO_TIER):
--    - Q1 ban đầu (known_from=2024-04-15), Q2 ban đầu (known_from=2024-07-15).
--    - Đính chính Q1 đến muộn hơn Q2 (known_from=2024-08-01).
--    - Tại ngày 2024-08-10, model PHẢI chọn Q2 (period_end_date=2024-06-30, metric_value=320.0),
--      KHÔNG được lùi kỳ về Q1 đính chính (period_end_date=2024-03-31).

with checked_cells as (
    -- Ca 1: Ô ở giữa trả bản cũ
    select
        date,
        symbol,
        period_end_date,
        known_from,
        metric_value,
        'intermediate_date_must_pick_older_version' as failure_reason
    from {{ ref('__pit_test_fundamental') }}
    where symbol = 'REV'
      and date = date '2024-05-01'
      and (known_from != date '2024-04-15' or metric_value != 100.0 or metric_value is null)

    union all

    -- Ca 2: Ô sau revision trả bản mới
    select
        date,
        symbol,
        period_end_date,
        known_from,
        metric_value,
        'post_revision_date_must_pick_newer_version' as failure_reason
    from {{ ref('__pit_test_fundamental') }}
    where symbol = 'REV'
      and date = date '2024-07-01'
      and (known_from != date '2024-06-15' or metric_value != 150.0 or metric_value is null)

    union all

    -- Ca 3: Hai bậc chọn kỳ -- không lùi kỳ về quá khứ khi đính chính kỳ cũ đến sau kỳ mới
    select
        date,
        symbol,
        period_end_date,
        known_from,
        metric_value,
        'two_tier_selection_regressed_to_old_period' as failure_reason
    from {{ ref('__pit_test_fundamental') }}
    where symbol = 'TWO_TIER'
      and date = date '2024-08-10'
      and (period_end_date != date '2024-06-30' or metric_value != 320.0 or metric_value is null)
)
select * from checked_cells
