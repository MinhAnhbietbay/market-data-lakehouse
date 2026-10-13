{#
    Chọn n kỳ gần nhất cho MỖI (khoá, kỳ, thời điểm-biết), giữ đúng ngữ nghĩa
    bitemporal của collapse_asof nhưng ở GRAIN KỲ thay vì grain ngày.

    Vì sao không nới collapse_asof's `__collapse_rn <= n` (spec §4.1): macro đó xếp
    hạng bằng MỘT row_number gộp cả hai bậc vào cùng một order by
    (collapse_asof.sql:96-101), nên rn=1,2 có thể là hai BẢN ĐÍNH CHÍNH CỦA CÙNG MỘT
    QUÝ, không phải hai quý. Ở đây hai bậc nằm ở hai tầng riêng biệt -- đó là điều
    làm nó đúng.

    Ba tầng:
      1. versions  -- tích chéo (kỳ P) x (mọi known_from ảnh hưởng cửa sổ của P).
         Tập known_from là HỢP của cả cửa sổ, không chỉ của riêng P: nếu Q1 được đính
         chính sau khi Q2 publish, TTM của Q2 phải đổi kể từ ngày đó. Chỉ sinh dòng
         cho P theo known_from của riêng P sẽ làm đính chính biến mất khỏi lưới ngày
         cho tới khi Q3 ra (spec §4.2).
      2. resolved  -- trong mỗi (P, kf, kỳ thành viên), chọn version có known_from lớn
         nhất <= kf. Đây là bậc chọn thứ hai của collapse_asof, viết tường minh.
      3. ranked    -- xếp hạng kỳ thành viên theo axis_col desc, giữ __rank < n.

    Caller phải tự aggregate và tự chặn khuyết kỳ -- macro này KHÔNG chặn, vì ngưỡng
    khác nhau theo nhóm (dòng chảy >= 4, avg_* >= 5, B8 yoy >= 5, qoq >= 2, SUE >= 12).

    Sửa 2026-08-27 (phát hiện khi Task 5 build lần đầu trên Spark thật): CTE (1) gốc
    dùng correlated scalar subquery (`v.known_from >= (select min(...) where p2.key =
    p.key and p2.axis = p.axis)`) -- Spark báo
    [UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY], không hỗ trợ subquery tương quan trong
    điều kiện JOIN ON. Viết lại tương đương: gộp min(known_from) theo (key_col,
    axis_col) thành một CTE riêng (`__period_first_known`), rồi JOIN thay vì subquery
    tương quan -- cùng kết quả (min known_from của CHÍNH kỳ p, không phải của v), chỉ
    khác cách Spark lập kế hoạch truy vấn.
#}
{% macro period_window_body(source_rel, key_col, axis_col, n) %}

with __src as (
    select * from {{ source_rel }}
),

-- known_from sớm nhất của mỗi kỳ (key_col, axis_col) -- dùng để xác định thời điểm
-- cửa sổ của kỳ đó "mở" (thay cho correlated subquery, xem ghi chú sửa ở trên)
__period_first_known as (
    select
        {{ key_col }},
        {{ axis_col }},
        min(known_from) as __first_known_from
    from __src
    group by {{ key_col }}, {{ axis_col }}
),

-- (1) mọi cặp (kỳ đích, thời điểm biết) cần sinh ra một phiên bản TTM
__anchor as (
    select distinct
        p.{{ key_col }},
        p.{{ axis_col }}          as __target_axis,
        v.known_from              as __as_of
    from __src p
    join __period_first_known pf
      on  pf.{{ key_col }} = p.{{ key_col }}
      and pf.{{ axis_col }} = p.{{ axis_col }}
    join __src v
      on  v.{{ key_col }} = p.{{ key_col }}
      -- thành viên cửa sổ: n kỳ tính lùi từ kỳ đích, kể cả chính nó
      and v.{{ axis_col }} <= p.{{ axis_col }}
      -- known_from của bất kỳ thành viên nào cũng là một thời điểm cửa sổ đổi giá trị
      and v.known_from >= pf.__first_known_from
),

-- (2) tại mỗi __as_of, mỗi kỳ thành viên chỉ còn đúng một phiên bản
__resolved as (
    select
        a.__target_axis,
        a.__as_of,
        m.*,
        row_number() over (
            partition by a.{{ key_col }}, a.__target_axis, a.__as_of, m.{{ axis_col }}
            order by m.known_from desc
        ) as __ver_rn
    from __anchor a
    join __src m
      on  m.{{ key_col }} = a.{{ key_col }}
      and m.{{ axis_col }} <= a.__target_axis
      and m.known_from <= a.__as_of
),

-- (3) xếp hạng kỳ, giữ n kỳ gần nhất
__ranked as (
    select
        *,
        row_number() over (
            partition by {{ key_col }}, __target_axis, __as_of
            order by {{ axis_col }} desc
        ) - 1 as __rank
    from __resolved
    where __ver_rn = 1
)

select * from __ranked where __rank < {{ n }}

{% endmacro %}
