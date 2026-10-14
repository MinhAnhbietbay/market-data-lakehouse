-- Bất biến SCD2: NHIỀU NHẤT một dòng is_current = true cho mỗi cik.
--
-- "Nhiều nhất", KHÔNG phải "đúng một": một thực thể chỉ có phiên bản đã đóng
-- là hợp lệ. Xem assert_curated_dim_listing_single_current.sql, nơi 79/84
-- entity không có dòng is_current nào.
--
-- unique trên khoá ghép (cik, valid_from) KHÔNG bắt được lỗi này: hai dòng
-- khác valid_from nhưng cùng is_current = true vẫn lọt qua khoá ghép.

select
    cik,
    count(*) as current_rows
from {{ ref('curated_dim_company') }}
where is_current
group by cik
having count(*) > 1
