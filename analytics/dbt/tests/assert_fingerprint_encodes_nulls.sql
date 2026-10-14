-- Ghim ignoreNullFields='false' phải có tác dụng THẬT. Nếu macro quên map(...)
-- thì Spark lấy mặc định từ config phiên và LOẠI trường null khỏi JSON, khiến
-- ('x', null) và ('x') không phân biệt được -- và tệ hơn: ai đó đổi config ở
-- tầng cluster thì MỌI hash đổi cùng lúc, sinh một phiên bản giả cho cả bảng.
with pair as (
    select
        {{ attr_fingerprint(["cast('x' as string)", "cast(null as string)"]) }} as h_with_null,
        -- CÙNG một lời gọi macro, chỉ bớt cột null: giữ tên trường struct mặc định
        -- (col1, col2, ...) giống hệt bên trên. Bản gốc dùng
        -- xxhash64(to_json(struct(... as f1))) -- alias "f1" khác tên mặc định "col1"
        -- nên hai vế KHÔNG BAO GIỜ bằng nhau dù macro có hỏng: test luôn xanh, không
        -- chứng minh được gì (reviewer agy gemini-3.1-pro-high bắt được 2026-08-22).
        {{ attr_fingerprint(["cast('x' as string)"]) }} as h_without_field
)
select * from pair where h_with_null = h_without_field
