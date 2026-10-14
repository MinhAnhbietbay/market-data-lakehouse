{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_gr_sector',
        tags=['layer::standardize', 'domain::group']
    )
}}

with normalized as (
    -- Mỗi biến thể chính tả là một "unique" mới, nên unique test trên chuỗi
    -- thô không bắt được sự phân mảnh. Chuẩn hoá CƠ HỌC (không phát minh
    -- mapping chính tả) để khoá gom các biến thể hoa/thường và khoảng trắng.
    -- Giữ tính chất distinct của danh mục gốc qua dedupe chuẩn hoá.
    select
        upper(trim(sector)) as sector_key,
        sector as sector
    from {{ source('raw', 'raw_fmp_available_sectors') }}
    where sector is not null
),

ranked as (
    -- order by tên: chọn một cách viết làm bản hiển thị chuẩn một cách tất
    -- định. Không có tiebreaker thì mỗi lần build có thể ra một cách viết
    -- khác và bảng "đổi" mà không có gì thay đổi thật.
    select
        *,
        row_number() over (
            partition by sector_key
            order by sector
        ) as rn
    from normalized
)

select
    sector_key,
    sector
from ranked
where rn = 1
