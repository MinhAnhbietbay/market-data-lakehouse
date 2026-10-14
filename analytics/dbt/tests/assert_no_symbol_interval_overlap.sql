-- A symbol may belong to only ONE security at a time. This is what stops a
-- reused ticker (delisted 2023, reissued to a different company in 2025)
-- from welding two companies' histories into one continuous series -- the
-- PK (share_class_figi, market_code, valid_from) does NOT prevent that case.
-- dbt_utils has no interval-overlap test, so this is hand-rolled.
with pairs as (
    select
        a.symbol,
        a.share_class_figi as figi_a,
        b.share_class_figi as figi_b,
        a.valid_from as from_a,
        coalesce(a.valid_to, date '9999-12-31') as to_a,
        b.valid_from as from_b,
        coalesce(b.valid_to, date '9999-12-31') as to_b
    from {{ ref('std_ar_listing') }} a
    join {{ ref('std_ar_listing') }} b
      on a.symbol = b.symbol
     -- So sánh TUPLE, không so figi đơn thuần: hai khoảng chồng nhau của cùng
     -- một figi cũng là vi phạm (SCD2 tự chồng lên chính nó), và điều kiện cũ
     -- loại chúng khỏi phép so sánh.
     and (a.share_class_figi, a.valid_from) < (b.share_class_figi, b.valid_from)
)
select *
from pairs
where from_a < to_b and from_b < to_a
