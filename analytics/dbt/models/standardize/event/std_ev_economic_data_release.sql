{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_economic_data_release',
        tags=['layer::standardize', 'domain::event']
    )
}}

-- analytics/dbt/models/standardize/event/std_ev_economic_data_release.sql
-- Empty by design: /economic-calendar has no raw contract yet
-- (enabled: false, paid_required).
--
-- Cột PIT là cast(null) chứ không phải kết quả bitemporal_version_chain:
-- không có raw source thì không có version chain để chạy. Khi raw contract về,
-- thay khối này bằng:
--   bitemporal_version_chain [source_cte='economic_calendar_src', key_cols=['event_key', 'country_code'],
--        fingerprint_cols=[...], first_known_from_expr='release_date',
--        pit_basis='observation']
-- (spec 3b §5)
--
-- CẢNH BÁO cho người thay stub này bằng macro bitemporal_version_chain: CTE `ranked`
-- dedupe bằng row_number() over (partition by event_key, country_code,
-- release_date) rồi lọc rn = 1. Nó KHÔNG có known_from trong partition, nên model
-- không bao giờ phát ra được hai phiên bản của cùng một business key -- tức
-- known_from vừa thêm vào combination_of_columns hôm nay là trang trí, và ngày
-- raw contract về, dedupe này sẽ ÂM THẦM cắt mất phiên bản cũ trước khi version
-- chain kịp nhìn thấy chúng. Phải gỡ hoặc sửa `ranked` trong cùng lần thay đó.

with raw_source as (
    select
        cast(null as date) as release_date,
        cast(null as string) as country_code,
        cast(null as string) as currency_code,
        cast(null as string) as event,
        cast(null as decimal(18,8)) as previous,
        cast(null as decimal(18,8)) as estimate,
        cast(null as decimal(18,8)) as actual,
        cast(null as string) as impact,
        cast(null as string) as unit,
        cast(null as date) as source_run_date,
        cast(null as date) as known_from,
        cast(null as string) as known_from_basis,
        cast(null as string) as pit_basis
    where false
),

normalized as (
    -- Mỗi biến thể chính tả là một "unique" mới, nên unique test trên chuỗi
    -- thô không bắt được sự phân mảnh. Chuẩn hoá CƠ HỌC (không phát minh
    -- mapping chính tả) để khoá gom các biến thể hoa/thường và khoảng trắng.
    select
        release_date,
        country_code,
        currency_code,
        upper(trim(event)) as event_key,
        event,
        previous,
        estimate,
        actual,
        impact,
        unit,
        source_run_date,
        known_from,
        known_from_basis,
        pit_basis
    from raw_source
    where event is not null
),

ranked as (
    -- order by tên: chọn một cách viết làm bản hiển thị chuẩn một cách tất
    -- định. Không có tiebreaker thì mỗi lần build có thể ra một cách viết
    -- khác và bảng "đổi" mà không có gì thay đổi thật.
    select
        *,
        row_number() over (
            partition by event_key, country_code, release_date
            order by event
        ) as rn
    from normalized
)

select
    release_date,
    country_code,
    currency_code,
    event_key,
    event,
    previous,
    estimate,
    actual,
    impact,
    unit,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from ranked
where rn = 1
