{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_cm_treasury_rate',
        tags=['layer::standardize', 'domain::common']
    )
}}

with treasury_rates_src as (
    select
        observation_date,
        run_date,
        landing_key,
        month_1
        , month_2
        , month_3
        , month_6
        , year_1
        , year_2
        , year_3
        , year_5
        , year_7
        , year_10
        , year_20
        , year_30
    from {{ source('raw', 'raw_fmp_treasury_rates') }}
    where observation_date is not null
),

unpivoted as (
    -- stack(N, 'tên', giá_trị, ...) là cách xoay wide->long của Spark; không có
    -- UNPIVOT như Trino. N phải khớp đúng số cặp liệt kê sau nó -- lệch thì
    -- Spark báo lỗi lúc phân tích chứ không im lặng cắt bớt.
    select
        observation_date,
        run_date,
        landing_key,
        tenor,
        rate
    from treasury_rates_src
    lateral view stack(
        12,
        'month_1', month_1
        , 'month_2', month_2
        , 'month_3', month_3
        , 'month_6', month_6
        , 'year_1', year_1
        , 'year_2', year_2
        , 'year_3', year_3
        , 'year_5', year_5
        , 'year_7', year_7
        , 'year_10', year_10
        , 'year_20', year_20
        , 'year_30', year_30
    ) as tenor, rate
),

non_null_tenors as (
    -- Ở dạng wide, một kỳ hạn không áp dụng hoặc không công bố là một ô null.
    -- Ở dạng long nó phải BIẾN MẤT chứ không thành một dòng "rate null" --
    -- dòng như thế nói "kỳ hạn này tồn tại và bằng null".
    select * from unpivoted where rate is not null
),

{{ bitemporal_version_chain(
    source_cte='non_null_tenors',
    key_cols=['observation_date', 'tenor'],
    fingerprint_cols=['rate'],
    first_known_from_expr='observation_date',
    pit_basis='observation'
) }}

select
    observation_date,
    tenor,
    rate,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned

