{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_delisting_event',
        tags=['layer::standardize', 'domain::event']
    )
}}

with delisted_ranked as (
    select
        symbol,
        delisted_date,
        run_date,
        landing_key,
        row_number() over (
            partition by symbol, delisted_date, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_delisted_companies') }}
),

delisted_latest as (
    select
        symbol,
        delisted_date,
        run_date,
        landing_key
    from delisted_ranked
    where rn = 1
),

-- Không có cột nghiệp vụ nào khác ngoài chính khoá (symbol, delisted_date) -- một
-- sự kiện huỷ niêm yết không "đổi giá trị" theo thời gian như báo cáo tài chính,
-- nó chỉ ĐƯỢC BIẾT ĐẾN vào một ngày nào đó. Không dùng chuỗi
-- fingerprinted/with_previous/change_points (không có gì để so sánh). Ý định ban
-- đầu: known_from = lần đầu tiên (run_date nhỏ nhất) ta thấy cặp (symbol,
-- delisted_date). Nhưng raw_fmp_delisted_companies ghi `write.mode: replace-table`
-- (configs/tables/raw/), tức raw chỉ giữ bản chụp mới nhất -> chuỗi luôn chỉ thấy
-- MỘT run_date, và "run_date nhỏ nhất" luôn là hôm nay. Thực tế (đo 2026-09-16):
-- known_from = least(delisted_date, run_date) = delisted_date -- ta giả định biết
-- việc huỷ niêm yết đúng ngày nó xảy ra. Muốn có "lần đầu thấy" thật thì raw phải
-- chuyển sang overwrite-partitions theo run_date; chưa làm, ghi lại để không ai
-- tin nhầm comment cũ.
{{ bitemporal_version_chain(
    source_cte='delisted_latest',
    key_cols=['symbol', 'delisted_date'],
    fingerprint_cols=[],
    first_known_from_expr='delisted_date',
    pit_basis='observation'
) }}

select
    symbol,
    delisted_date,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

