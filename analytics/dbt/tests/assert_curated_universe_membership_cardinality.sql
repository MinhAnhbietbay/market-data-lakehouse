-- Fence rẻ chống regression: cardinality của in_top3000 mỗi ngày không được
-- vượt quá universe_top_n. Trên vài mã dev thì xanh hiển nhiên (không đủ mã
-- để vi phạm) -- giá trị thật của test này chỉ lộ ra khi universe mở rộng,
-- đúng kiểu fence đặt-trước như catalog_curated_guard đã làm cho gap list.
-- rank() thuần (trước đợt sửa 2026-08-26) có thể cho >N mã cùng vào top khi
-- hoà hạng ở biên; row_number()+tiebreak symbol asc đảm bảo <=N luôn đúng --
-- test này là bằng chứng sống cho bất biến đó, không chỉ tài liệu.
select date, count(*) as member_count
from {{ ref('curated_universe_membership') }}
where in_top3000
group by date
having count(*) > {{ var('universe_top_n', 3000) }}
