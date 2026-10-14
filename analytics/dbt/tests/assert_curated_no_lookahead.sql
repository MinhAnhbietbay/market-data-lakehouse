-- Test 2: No-look-ahead có chủ đích.
-- Inject revision known_from ở tương lai, khẳng định ô quá khứ/hiện tại KHÔNG ĐỔI
-- và không ô nào trong __pit_test_fundamental mang giá trị từ tương lai.

with violations as (
    -- Invariant 1: không ô nào được có known_from > date
    select
        date,
        symbol,
        period_end_date,
        known_from,
        metric_value,
        'known_from_in_future_relative_to_date' as failure_reason
    from {{ ref('__pit_test_fundamental') }}
    where known_from > date

    union all

    -- Invariant 2: kiểm tra có chủ đích cho symbol FUT:
    -- Dù có revision known_from = 2099-01-01 (metric_value = 999.0),
    -- các ô ngày 2024-05-01 và 2024-07-01 PHẢI nhận metric_value = 200.0,
    -- không được nhận 999.0 hay null.
    select
        date,
        symbol,
        period_end_date,
        known_from,
        metric_value,
        'future_revision_leaked_into_past' as failure_reason
    from {{ ref('__pit_test_fundamental') }}
    where symbol = 'FUT'
      and date in (date '2024-05-01', date '2024-07-01')
      and (metric_value != 200.0 or metric_value is null or known_from != date '2024-04-15')
)
select * from violations
