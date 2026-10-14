{{
    config(
        materialized='table',
        file_format='iceberg',
        schema='curated_pit_test',
        alias='__pit_test_analyst',
        tags=['layer::curated_pit_test', 'test']
    )
}}

-- Proves: num_analysts_eps is sourced from the nearest forward-looking estimate
-- period (fy_rank = 1 via cur_y), NOT from a max(estimate_period_end_date)
-- collapse which would wrongly pick the farthest-future projection.

with estimates_collapsed as (
    {{ collapse_asof(
        source_rel=ref('pit_seed_analyst_estimate'),
        key_cols=['symbol', 'estimate_period_end_date'],
        spine_rel=ref('pit_seed_analyst_spine'),
        shape='A',
        max_staleness_days=90
    ) }}
),

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
)

select
    s.date,
    s.symbol,
    cy.eps_avg as eps_estimate_cur_y,
    cy.num_analysts_eps
from {{ ref('pit_seed_analyst_spine') }} s
left join cur_y cy on cy.date = s.date and cy.symbol = s.symbol
