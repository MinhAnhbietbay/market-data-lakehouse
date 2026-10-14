{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_gr_security_rating_score',
        tags=['layer::standardize', 'domain::group']
    )
}}

-- Tách khỏi std_gr_security_rating: hạng (rating) là một giá trị vô hướng của
-- (symbol, ngày); 7 điểm thành phần là một chuỗi quan sát cùng grain nhưng
-- khác chiều. Ở dạng wide, thêm một tiêu chí chấm điểm = đổi lược đồ và mọi
-- downstream; ở dạng long = thêm một dòng.

with ratings_src as (
    select
        symbol,
        rating_date,
        overall_score,
        discounted_cash_flow_score,
        return_on_equity_score,
        return_on_assets_score,
        debt_to_equity_score,
        price_to_earnings_score,
        price_to_book_score,
        run_date,
        landing_key
    from {{ source('raw', 'raw_fmp_ratings_historical') }}
    where symbol is not null and rating_date is not null
),

unpivoted as (
    -- stack(N, 'tên', giá_trị, ...) là cách xoay wide->long của Spark; không có
    -- UNPIVOT như Trino. N phải khớp đúng số cặp liệt kê sau nó -- lệch thì
    -- Spark báo lỗi lúc phân tích chứ không im lặng cắt bớt.
    select
        symbol,
        rating_date,
        run_date,
        landing_key,
        score_name,
        score_value
    from ratings_src
    lateral view stack(
        7,
        'overall_score', overall_score,
        'discounted_cash_flow_score', discounted_cash_flow_score,
        'return_on_equity_score', return_on_equity_score,
        'return_on_assets_score', return_on_assets_score,
        'debt_to_equity_score', debt_to_equity_score,
        'price_to_earnings_score', price_to_earnings_score,
        'price_to_book_score', price_to_book_score
    ) as score_name, score_value
),

non_null_scores as (
    -- Ở dạng wide, một điểm không áp dụng hoặc không có là một ô null.
    -- Ở dạng long nó phải BIẾN MẤT chứ không thành một dòng "score_value null":
    -- một dòng như thế nói "điểm này tồn tại và bằng null".
    select * from unpivoted where score_value is not null
),

{{ bitemporal_version_chain(
    source_cte='non_null_scores',
    key_cols=['symbol', 'rating_date', 'score_name'],
    fingerprint_cols=['score_value'],
    first_known_from_expr='rating_date',
    pit_basis='observation'
) }}

select
    symbol,
    rating_date,
    score_name,
    score_value,
    source_run_date,
    known_from,
    known_from_basis,
    pit_basis
from _bvc_versioned
