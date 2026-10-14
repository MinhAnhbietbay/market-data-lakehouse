-- Một cik chỉ được có MỘT phiên bản hiệu lực tại một thời điểm. So sánh TUPLE
-- (không so cik đơn thuần) để bắt cả trường hợp SCD2 tự chồng lên chính nó --
-- đúng lỗi mà bản gốc của assert_no_symbol_interval_overlap bỏ lọt.
with pairs as (
    select
        a.cik,
        a.valid_from as from_a,
        coalesce(a.valid_to, date '9999-12-31') as to_a,
        b.valid_from as from_b,
        coalesce(b.valid_to, date '9999-12-31') as to_b
    from {{ ref('std_ip_legal_entity') }} a
    join {{ ref('std_ip_legal_entity') }} b
      on a.cik = b.cik
     and (a.cik, a.valid_from) < (b.cik, b.valid_from)
)

select *
from pairs
where from_a < to_b and from_b < to_a
