-- Bất biến SCD2: NHIỀU NHẤT một dòng is_current = true cho mỗi cặp
-- (share_class_figi, market_code).
--
-- "Nhiều nhất", KHÔNG phải "đúng một" -- đây là chỗ khác biệt đó có hậu quả
-- thật: đo 2026-08-28, 79/84 entity KHÔNG có dòng is_current nào (listing
-- không còn hoạt động). Một test "đúng một" sẽ báo đỏ 79 lần cho dữ liệu
-- hoàn toàn hợp lệ.

select
    share_class_figi,
    market_code,
    count(*) as current_rows
from {{ ref('curated_dim_listing') }}
where is_current
group by share_class_figi, market_code
having count(*) > 1
