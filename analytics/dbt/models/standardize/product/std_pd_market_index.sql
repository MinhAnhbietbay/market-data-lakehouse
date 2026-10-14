{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_pd_market_index',
        tags=['layer::standardize', 'domain::product']
    )
}}

-- Market Index dimension (pd area) -- degenerate lookup, PK entity_name.
-- Bổ sung symbol từ raw_fmp_index_list để làm alternate key / đích trỏ FK cho
-- std_ar_index_membership.index_symbol.
-- Luật dedupe: mỗi entity_name_key chọn đúng 1 dòng; nếu nguồn xuất hiện nhiều symbol
-- cho cùng một entity_name_key, chọn tất định theo thứ tự symbol tăng dần (order by symbol asc, entity_name asc).

with normalized as (
    -- Mỗi biến thể chính tả là một "unique" mới, nên unique test trên chuỗi
    -- thô không bắt được sự phân mảnh. Chuẩn hoá CƠ HỌC (không phát minh
    -- mapping chính tả) để khoá gom các biến thể hoa/thường và khoảng trắng.
    -- Giữ tính chất distinct của danh mục gốc qua dedupe chuẩn hoá.
    select
        upper(trim(entity_name)) as entity_name_key,
        entity_name,
        symbol
    from {{ source('raw', 'raw_fmp_index_list') }}
    where entity_name is not null
      and symbol is not null
),

ranked as (
    -- order by symbol, entity_name: chọn một cách viết làm bản hiển thị chuẩn
    -- một cách tất định.
    select
        *,
        row_number() over (
            partition by entity_name_key
            order by symbol asc, entity_name asc
        ) as rn
    from normalized
)

select
    entity_name_key,
    entity_name,
    symbol
from ranked
where rn = 1
