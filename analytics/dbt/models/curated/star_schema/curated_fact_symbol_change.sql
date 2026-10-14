{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_fact_symbol_change',
        tags=['layer::curated', 'star_table']
    )
}}

-- KHÔNG phân giải share_class_figi, và bảng này KHÔNG có cột đó. Dùng
-- resolve_security_at ở đây là VÒNG TRÒN ĐỊNH DANH: std_ar_listing chính là bảng
-- ghi lịch sử niêm yết và đổi mã, nên lấy nó phân giải một sự kiện đổi mã là hỏi
-- cái bảng đã biết đáp án -- và trong tương lai chính bảng này là một đầu vào
-- dựng nên lịch sử đó. Giữ old_symbol/new_symbol trần (nợ N-d, spec §4.2).

with src as (
    select * from {{ ref('std_ev_symbol_change_event') }}
)

select
    *,
    {{ latest_known_flag(['old_symbol', 'new_symbol', 'change_date']) }} as is_latest_known,
    current_timestamp as loaded_at
from src
