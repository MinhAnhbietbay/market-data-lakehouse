{{
    config(
        materialized='table',
        partition_by=['years(period_end_date)'],
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ac_revenue_segment_breakdown',
        tags=['layer::standardize', 'domain::accounting']
    )
}}

with revenue_geographic_segmentation_ranked as (
    select
        symbol,
        fiscal_year,
        period,
        period_end_date,
        reported_currency,
        data_americas_segment,
        run_date,
        landing_key,
        row_number() over (
            partition by symbol, fiscal_year, period, run_date
            order by run_date desc, landing_key desc
        ) as rn
    from {{ source('raw', 'raw_fmp_revenue_geographic_segmentation') }}
    where symbol is not null and fiscal_year is not null and period is not null
),

revenue_geographic_segmentation_latest as (
    select
        symbol,
        fiscal_year,
        period,
        period_end_date,
        reported_currency,
        data_americas_segment,
        run_date,
        landing_key
    from revenue_geographic_segmentation_ranked
    where rn = 1
),

unpivoted as (
    select
        symbol,
        fiscal_year,
        period,
        period_end_date,
        reported_currency,
        segment_name,
        segment_revenue,
        run_date,
        landing_key
    from revenue_geographic_segmentation_latest
    -- stack(N, 'tên', giá_trị, ...) là cách xoay wide->long của Spark; không có
    -- UNPIVOT như Trino. N phải khớp đúng số cặp liệt kê sau nó -- lệch thì Spark
    -- báo lỗi lúc phân tích chứ không im lặng cắt bớt, nên đây là chỗ an toàn.
    --
    -- Hôm nay N = 1 vì nguồn mới trả duy nhất châu Mỹ. Thêm phân khúc mới =
    -- thêm một cặp ở đây và tăng N; KHÔNG đụng gì tới khoá, test, hay downstream.
    -- Đó chính là lý do đổi sang long khi bảng còn một cột.
    lateral view stack(
        1,
        'americas', data_americas_segment
    ) as segment_name, segment_revenue
),

non_null_segments as (
    -- Ở dạng wide, một phân khúc không áp dụng cho công ty đó là một ô null.
    -- Ở dạng long nó phải BIẾN MẤT chứ không thành một dòng "doanh thu null":
    -- một dòng như thế nói "phân khúc này có tồn tại và bằng null", khác nghĩa.
    select * from unpivoted where segment_revenue is not null
),

-- Mượn ngày nộp thật từ bảng anh em cùng nguồn báo cáo. revenue_segment_breakdown
-- chỉ có period_end_date; neo vào đó là lookahead 30-45 ngày (spec §3.2). Khi kỳ
-- tương ứng không có trong income_statement, lùi về period_end + FILING_LAG_DAYS --
-- hằng số dẫn từ số đo ở docs/superpowers/plans/measurements/2026-08-30-filing-lag.md,
-- KHÔNG phải một con số tròn chọn bừa.
filing_dates as (
    select distinct symbol, period_end_date, filing_date
    from {{ ref('std_ac_income_statement') }}
    where filing_date is not null
),

with_filing as (
    select
        e.*,
        coalesce(
            f.filing_date,
            date_add(e.period_end_date, {{ var('filing_lag_days') }})
        ) as anchor_filing_date
    from non_null_segments e
    left join filing_dates f
      on f.symbol = e.symbol and f.period_end_date = e.period_end_date
),

{{ bitemporal_version_chain(
    source_cte='with_filing',
    key_cols=['symbol', 'fiscal_year', 'period', 'segment_name'],
    fingerprint_cols=['period_end_date', 'reported_currency', 'segment_revenue'],
    first_known_from_expr='anchor_filing_date',
    pit_basis='filing'
) }}

select
    symbol,
    fiscal_year,
    period,
    period_end_date,
    reported_currency,
    segment_name,
    segment_revenue,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned




