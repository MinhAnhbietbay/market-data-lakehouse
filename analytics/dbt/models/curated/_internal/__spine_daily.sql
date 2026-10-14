{{
    config(materialized='table', partition_by=['months(date)'], file_format='iceberg',
           schema=var('env_curated'), alias='__spine_daily', tags=['layer::curated', 'internal'])
}}

-- PHÉP ĐỌC, không phải tích chéo. curated_dim_date × universe sẽ sinh dòng cho
-- những ngày TRƯỚC KHI mã IPO và SAU KHI mã huỷ niêm yết -- tự tạo lại đúng
-- loại zombie data mà spec §2 cấm, và tạo ở tầng lưới thì mọi họ phía sau đều
-- thừa hưởng.
--
-- Việc dựng grid ĐẦY ĐỦ (tích chéo, ô thiếu là null) là việc của engine
-- (quant-compute-engine.md:178). Curated cấp sự thật, engine cấp hình dạng.

select date, symbol
from {{ ref('curated_universe_membership') }}
