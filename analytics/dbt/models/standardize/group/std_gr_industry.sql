{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_gr_industry',
        tags=['layer::standardize', 'domain::group']
    )
}}

with normalized as (
    -- Mỗi biến thể chính tả là một "unique" mới, nên unique test trên chuỗi
    -- thô không bắt được sự phân mảnh. Chuẩn hoá CƠ HỌC (không phát minh
    -- mapping chính tả) để khoá gom các biến thể hoa/thường và khoảng trắng.
    -- Giữ tính chất distinct của danh mục gốc qua dedupe chuẩn hoá.
    select
        upper(trim(industry)) as industry_key,
        industry as industry
    from {{ source('raw', 'raw_fmp_available_industries') }}
    where industry is not null
),

ranked as (
    -- order by tên: chọn một cách viết làm bản hiển thị chuẩn một cách tất
    -- định. Không có tiebreaker thì mỗi lần build có thể ra một cách viết
    -- khác và bảng "đổi" mà không có gì thay đổi thật.
    select
        *,
        row_number() over (
            partition by industry_key
            order by industry
        ) as rn
    from normalized
)

select
    industry_key,
    industry
from ranked
where rn = 1
