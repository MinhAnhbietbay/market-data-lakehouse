{{
    config(
        materialized='table',
        partition_by=['months(date)'],
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_analyst',
        tags=['layer::curated', 'internal']
    )
}}

-- 4 field estimates theo QUÝ là gap: endpoint /analyst-estimates cấu hình
-- period: annual (data_sources.yaml:251) nên không có dòng theo quý để lọc.
-- eps_estimate_cur_y / next_y thì có.

with ratings_long as (
    {{ collapse_asof(
        source_rel=ref('std_bd_analyst_rating_distribution'),
        key_cols=['symbol', 'rating_bucket'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=90
    ) }}
),

-- Nguồn ở dạng DỌC (rating_bucket, analyst_count) -- pivot sang 5 cột ở đây,
-- sau khi collapse. Pivot trước collapse sẽ trộn version giữa các bucket.
ratings_wide as (
    select
        date,
        symbol,
        max(case when rating_bucket = 'strong_buy'  then analyst_count end) as rating_strong_buy,
        max(case when rating_bucket = 'buy'         then analyst_count end) as rating_buy,
        max(case when rating_bucket = 'hold'        then analyst_count end) as rating_hold,
        max(case when rating_bucket = 'sell'        then analyst_count end) as rating_sell,
        max(case when rating_bucket = 'strong_sell' then analyst_count end) as rating_strong_sell,
        sum(analyst_count)                                                  as num_analysts_rating,
        max(known_from)                                                     as known_from
    from ratings_long
    group by date, symbol
),

targets as (
    {{ collapse_asof(
        source_rel=ref('std_bd_price_target_consensus'),
        key_cols=['symbol'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=90
    ) }}
),

estimates_collapsed as (
    {{ collapse_asof(
        source_rel=ref('std_bd_analyst_estimate'),
        key_cols=['symbol', 'estimate_period_end_date'],
        spine_rel=ref('__spine_daily'),
        shape='A',
        max_staleness_days=90
    ) }}
),

-- num_analysts_eps đi qua CHÍNH pipeline cur_y/fy_rank=1, không phải một
-- collapse_asof riêng theo max(estimate_period_end_date) -- kỳ ước tính là
-- FORWARD-LOOKING, nên 'kỳ lớn nhất' là NĂM XA NHẤT, không phải năm hiện tại.
-- Bug thật, phát hiện ở lượt review cuối plan 2026-08-25, xác nhận trên dữ
-- liệu thật: AAPL FY2030 có 9 analyst, FY2026 có 29.
estimates_ranked as (
    select
        date, symbol, eps_avg, num_analysts_eps, estimate_period_end_date, known_from,
        row_number() over (
            partition by date, symbol
            order by estimate_period_end_date asc
        ) as fy_rank
    from estimates_collapsed
    where estimate_period_end_date >= date
),

cur_y as (
    select date, symbol, eps_avg, num_analysts_eps, known_from
    from estimates_ranked
    where fy_rank = 1
),

next_y as (
    select date, symbol, eps_avg, known_from
    from estimates_ranked
    where fy_rank = 2
)

select
    s.date,
    s.symbol,

    -- =========================================================================
    -- Analyst Estimates & Ratings (12 in-scope fields)
    -- =========================================================================
    cy.num_analysts_eps,
    cy.eps_avg                                                              as eps_estimate_cur_y,
    ny.eps_avg                                                              as eps_estimate_next_y,
    r.num_analysts_rating,
    r.rating_strong_buy,
    r.rating_buy,
    r.rating_hold,
    r.rating_sell,
    r.rating_strong_sell,
    t.target_consensus                                                      as price_target_mean,
    t.target_high                                                           as price_target_high,
    t.target_low                                                            as price_target_low,

    greatest(r.known_from, t.known_from, cy.known_from, ny.known_from)      as known_from,
    current_timestamp                                                       as loaded_at
from {{ ref('__spine_daily') }} s
left join ratings_wide r on r.date = s.date and r.symbol = s.symbol
left join targets     t  on t.date = s.date and t.symbol = s.symbol
left join cur_y       cy on cy.date = s.date and cy.symbol = s.symbol
left join next_y      ny on ny.date = s.date and ny.symbol = s.symbol
