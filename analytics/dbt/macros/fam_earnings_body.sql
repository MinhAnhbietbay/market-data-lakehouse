{#
    4 field B10. SUE cần cửa sổ 12 kỳ, KHÔNG phải 8: mẫu số là stddev của SAI PHÂN
    MÙA VỤ (eps_actual[q] - eps_actual[q-4]) nên mỗi sai phân đã tốn 5 kỳ, và cần 8
    sai phân -> P[0..11] (spec 2026-08-27 §4.4, công thức gốc 2026-07-28 :842).
    Dùng n=8 sẽ chỉ có 4 sai phân đầy đủ và stddev tính trên nửa mẫu -- vẫn ra số.

    Trục kỳ ở đây là announcement_date, không phải period_end_date.
#}
{% macro fam_earnings_body(source_rel) %}

{#-
    Chỉ kỳ ĐÃ BÁO CÁO được vào cửa sổ kỳ. `std_ev_earnings_announcement` chứa cả LỊCH
    quý sau -- dòng có `announcement_date` ở tương lai, `eps_actual` NULL, và
    `known_from` là ngày cái lịch đó được công bố (basis `system_time`), nên nó hợp lệ
    về PIT: ta thật sự biết trước. Nhưng nó KHÔNG phải một kỳ báo cáo.

    Lỗi đo sống 2026-09-22: AAPL có (2026-10-29, eps_actual NULL, known_from
    2026-09-14). Từ 2026-09-14 nó là `announcement_date` lớn nhất trong các dòng đã
    biết ⇒ `period_window_body` cho nó `__rank = 0` ⇒ cả 4 field dưới đây NULL trên
    lưới ngày. Đúng 6 phiên cuối của cả 5 mã, và sẽ lặp lại ~6 tuần mỗi quý.

    Lọc bằng `eps_actual is not null`, KHÔNG phải `announcement_date <= <ngày>`: FMP
    có thể để trống actual thêm một hai ngày sau khi đã công bố, và khi đó
    `announcement_date <= ngày` vẫn cho đúng cái kỳ rỗng ấy vào __rank=0 -- cùng lỗi,
    chỉ ngắn hơn. "Có actual" mới là điều kiện thật để tính được surprise.

    Lọc ở ĐÂY chứ không trong `period_window_body`: macro đó dùng chung với
    __fam_ttm/__fam_fundamental, nơi trục kỳ là period_end_date và không có khái niệm
    "kỳ đã lên lịch". Lọc ở đó sẽ áp một luật của earnings lên hai họ khác.

    Hệ quả thứ hai, ít lộ hơn: kỳ rỗng còn ĐẨY LỆCH cả cửa sổ 12 kỳ của SUE. Mỗi thành
    viên tụt một bậc nên cặp `__rank + 4` của sai phân mùa vụ ghép sai quý, và
    `count(__sdiff) >= 8` được thoả bằng một bộ sai phân lệch mùa.
-#}
{%- set reported_rel -%}
(select * from {{ source_rel }} where eps_actual is not null) __reported
{%- endset -%}

with windowed as (
    {{ period_window_body(
        source_rel=reported_rel,
        key_col='symbol',
        axis_col='announcement_date',
        n=12
    ) }}
),

-- sai phân mùa vụ: mỗi kỳ trừ đi kỳ cách nó 4 quý trong cùng cửa sổ
seasonal_diff as (
    select
        w.symbol,
        w.__target_axis,
        w.__as_of,
        w.__rank,
        w.eps_actual,
        w.eps_estimated,
        w.revenue_actual,
        w.revenue_estimated,
        w.eps_actual - lag4.eps_actual as __sdiff
    from windowed w
    left join windowed lag4
      on  lag4.symbol = w.symbol
      and lag4.__target_axis = w.__target_axis
      and lag4.__as_of = w.__as_of
      and lag4.__rank = w.__rank + 4
)

select
    symbol,
    __target_axis                                   as announcement_date,
    __as_of                                         as known_from,

    -- 3 field chỉ cần kỳ hiện tại: actual và estimate nằm trong CÙNG một dòng
    {{ safe_div("max(case when __rank = 0 then eps_actual end) - max(case when __rank = 0 then eps_estimated end)",
                "abs(max(case when __rank = 0 then eps_estimated end))") }}      as earnings_surprise_pct,
    sign(max(case when __rank = 0 then eps_actual end)
       - max(case when __rank = 0 then eps_estimated end))                        as earnings_surprise_sign,
    {{ safe_div("max(case when __rank = 0 then revenue_actual end) - max(case when __rank = 0 then revenue_estimated end)",
                "abs(max(case when __rank = 0 then revenue_estimated end))") }}   as revenue_surprise_pct,

    -- SUE: cần đủ 12 kỳ để có 8 sai phân đầy đủ
    case when count(distinct __target_axis) >= 0
          and count(__sdiff) >= 8
         then {{ safe_div("max(case when __rank = 0 then __sdiff end)",
                          "stddev_samp(__sdiff)") }} end                          as standardized_unexpected_earnings

from seasonal_diff
group by symbol, __target_axis, __as_of

{% endmacro %}
