-- FK ngày của curated_fact_workforce_observation KHÔNG khai relationships cứng
-- được: curated_dim_date phủ 1996-08-19..2026-08-17 (10.956 dòng), còn
-- period_end_date ở đây lùi tới 1994-06-30 -- đo 2026-08-28 có ĐÚNG 5 dòng mồ
-- côi. Đây là count guard trần 5 theo mẫu
-- assert_curated_dim_company_sector_orphan_count.sql của đợt 3a.
--
-- Trần 5 là trần TẠM. Khi dim_date mở rộng lùi qua backfill, hạ trần về 0 rồi
-- thay bằng relationships cứng (nợ N-g). Test đỏ khi số mồ côi VƯỢT 5 -- tức
-- nó bắt được cả trường hợp dữ liệu mới rơi ra ngoài dim_date.
--
-- RE-MEASURED 2026-09-14: 86, sau khi rebuild sạch từ `down -v`. Không phải
-- regression -- curated_dim_date.sql giờ chỉ phủ ~10 năm gần nhất
-- (2016-09-13..2026-09-11, đúng quyết định chốt độ sâu OHLCV về 10 năm,
-- configs/sources/data_sources.yaml ohlcv_non_split_adjusted, 2026-08-19),
-- trong khi curated_fact_workforce_observation.period_end_date vẫn lùi tới
-- 1994 như trước. Cửa sổ dim_date hẹp hơn => nhiều period_end_date cũ rơi ra
-- ngoài hơn hẳn so với hồi 2026-08-28. Bumped lên 86 làm trần mới -- đo lại
-- và bump tay lần nữa nếu độ sâu OHLCV đổi tiếp, đừng làm trần này tự tính
-- theo dim_date (mất tác dụng bắt regression thật).

with orphans as (
    select w.period_end_date
    from {{ ref('curated_fact_workforce_observation') }} as w
    left join {{ ref('curated_dim_date') }} as d
        on d.date = w.period_end_date
    where d.date is null
)

select count(*) as orphan_count
from orphans
having count(*) > 86
