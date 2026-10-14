-- curated_dim_company.sector_key không thể mang relationships cứng sang
-- curated_dim_sector: từ điển đích RỖNG (0 dòng, endpoint /available-sectors
-- trả 402) trong khi cột này có 6 giá trị non-NULL. FK cứng vào bảng rỗng là
-- bất khả thi về cấu trúc, không phải chỉ đang fail.
--
-- Cùng phán quyết với assert_legal_entity_sector_orphan_count.sql ở tầng
-- standardize: KHÔNG xoá giá trị sector (đó là dữ liệu thật từ nguồn profile),
-- KHÔNG xoá test (mua màu xanh bằng cách thôi không hỏi nữa). Canh COUNT: nếu
-- nó vượt mức đã biết hôm nay, có thứ mới đang rò và cần người xem.
--
-- Khi FMP nâng gói và curated_dim_sector đầy, con số này phải về 0 và
-- relationships cứng quay lại thay thế (nợ N-5).
--
-- Đo 2026-08-28: 6 giá trị sector_key mồ côi.

select count(*) as orphan_count
from {{ ref('curated_dim_company') }} c
where c.sector_key is not null
  and not exists (
      select 1 from {{ ref('curated_dim_sector') }} r where r.sector_key = c.sector_key
  )
having count(*) > 6
