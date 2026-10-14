{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_beneficial_owner',
        tags=['layer::curated', 'star_table']
    )
}}

-- RỖNG tại thời điểm dựng: endpoint ownership bị gate FMP tier. Nợ N-4.
--
-- KHOÁ YẾU: nguồn không có mã định danh nào, khoá buộc phải là
-- name_of_reporting_person -- chuỗi tên người/tổ chức tự do, dễ trùng và dễ
-- lệch chính tả giữa các kỳ nộp. Phải đo tỉ lệ trùng trên dữ liệu thật trước
-- khi cho hạ nguồn join vào (spec §6 N-4).

select
    name_of_reporting_person,
    citizenship_or_place_of_organization,
    type_of_reporting_person,
    current_timestamp as loaded_at
from {{ ref('std_ip_beneficial_owner') }}
