-- Vân tay phải phân biệt được hai bộ giá trị chỉ khác nhau ở RANH GIỚI TRƯỜNG.
-- Cách concat_ws('||', ...) của reference implementation KHÔNG phân biệt được:
-- ('a||b', null) và ('a', 'b') ra cùng một chuỗi. to_json giữ ranh giới bằng
-- key JSON nên không lẫn. Test này đỏ ngay nếu ai đó đổi macro sang concat_ws.
with pair as (
    select
        {{ attr_fingerprint(["cast('a||b' as string)", "cast(null as string)"]) }} as h_left,
        {{ attr_fingerprint(["cast('a' as string)", "cast('b' as string)"]) }} as h_right
)
select * from pair where h_left = h_right
