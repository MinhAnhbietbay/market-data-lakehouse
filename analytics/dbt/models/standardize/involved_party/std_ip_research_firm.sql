{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ip_research_firm',
        tags=['layer::standardize', 'domain::involved_party']
    )
}}

with raw_source as (
    select
        grading_company as research_firm_name
    from {{ source('raw', 'raw_fmp_grades') }}
    where grading_company is not null
),

normalized as (
    -- Mỗi biến thể chính tả là một "unique" mới, nên unique test trên chuỗi
    -- thô không bắt được sự phân mảnh. Chuẩn hoá CƠ HỌC (không phát minh
    -- mapping chính tả) để khoá gom các biến thể hoa/thường và khoảng trắng.
    -- Giữ tính chất distinct của danh mục gốc qua dedupe chuẩn hoá.
    select
        upper(trim(research_firm_name)) as research_firm_name_key,
        research_firm_name
    from raw_source
),

ranked as (
    -- order by tên: chọn một cách viết làm bản hiển thị chuẩn một cách tất
    -- định. Không có tiebreaker thì mỗi lần build có thể ra một cách viết
    -- khác và bảng "đổi" mà không có gì thay đổi thật.
    select
        *,
        row_number() over (
            partition by research_firm_name_key
            order by research_firm_name
        ) as rn
    from normalized
)

select
    research_firm_name_key,
    research_firm_name
from ranked
where rn = 1
