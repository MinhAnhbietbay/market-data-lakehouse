{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_revenue_segment',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển segment_name, chưng cất từ chính fact bằng select distinct -- dim duy
-- nhất của đợt này không có nguồn standardize riêng.
--
-- TỪ ĐIỂN QUAN SÁT ĐƯỢC, KHÔNG PHẢI TỪ ĐIỂN THẨM QUYỀN: FMP không có endpoint
-- từ điển segment. Đo 2026-08-28: 16 dòng nguồn chỉ chứa 1 giá trị phân biệt,
-- nên bảng này hiện có 1 dòng và sẽ nở ra theo dữ liệu (nợ N-h).
--
-- KHÔNG có is_latest_known: select distinct nên không mang known_from.
-- Được MIỄN test parity số dòng vì lý do đó (spec §6.2).

select
    segment_name,
    current_timestamp as loaded_at
from {{ ref('std_ac_revenue_segment_breakdown') }}
where segment_name is not null
group by segment_name
