{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_listing',
        tags=['layer::curated', 'star_table']
    )
}}

-- Dim niêm yết, tách riêng khỏi dim_security có chủ đích: gộp bằng
-- `where is_current` sẽ vứt lịch sử niêm yết của một bảng có PIT đầy đủ.
--
-- HÌNH DẠNG THẬT, đo 2026-08-28: 84 dòng / 84 cặp (share_class_figi,
-- market_code) -- max(count(*)) group by cặp đó = 1, tức bảng CHƯA có phiên
-- bản nào. Chỉ 5/84 entity có is_current = true; 79 entity còn lại có đúng một
-- dòng và dòng đó is_current = false. Chúng KHÔNG phải lịch sử đã đóng của 5
-- cái kia -- chúng là 79 listing khác, hiện không hoạt động.
--
-- valid_from giữ trong khoá dù hiện thừa: nguồn khai schema SCD2 đầy đủ, ta
-- thiết kế theo schema chứ không theo ảnh chụp hôm nay (spec §2.1, nợ N-8).

select
    share_class_figi,
    market_code,
    symbol,
    exchange_code,
    valid_from,
    valid_to,
    is_current,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis,
    current_timestamp as loaded_at
from {{ ref('std_ar_listing') }}
