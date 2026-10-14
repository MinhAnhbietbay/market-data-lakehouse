-- Sinh đôi của assert_curated_dim_company_sector_orphan_count.sql cho
-- industry_key. Từ điển đích curated_dim_industry RỖNG (endpoint
-- /available-industries trả 402). Cùng phán quyết: canh COUNT, không xoá dữ
-- liệu, không xoá test. Khi từ điển đầy thì hạ trần về 0 và thay bằng
-- relationships cứng (nợ N-5).
--
-- Đo 2026-08-28: 6 giá trị industry_key mồ côi.

select count(*) as orphan_count
from {{ ref('curated_dim_company') }} c
where c.industry_key is not null
  and not exists (
      select 1 from {{ ref('curated_dim_industry') }} r where r.industry_key = c.industry_key
  )
having count(*) > 6
