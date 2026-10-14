{{
    config(
        materialized='table',
        partition_by=['months(date)'],
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_macro_daily',
        tags=['layer::curated', 'engine_table']
    )
}}

-- Grain là date, KHÔNG nhân bản per-symbol (~3000× lãng phí). Engine
-- broadcast-join trên date, một chiều (date) -> (date, symbol).
--
-- Macro bị revise thường xuyên, nên as-of theo known_from <= date là bắt buộc:
-- một bản revise phải VÔ HÌNH với mọi ngày trước ngày nó được công bố.

with spine as (
    select date from {{ ref('curated_dim_date') }}
),

observations as (
    {{ collapse_asof(
        source_rel=ref('std_cm_macro_observation'),
        key_cols=['series_name'],
        spine_rel=ref('curated_dim_date'),
        shape='A',
        max_staleness_days=90
    ) }}
),

treasury as (
    {{ collapse_asof(
        source_rel=ref('std_cm_treasury_rate'),
        key_cols=['tenor'],
        spine_rel=ref('curated_dim_date'),
        shape='A',
        max_staleness_days=90
    ) }}
)

select
    s.date,
    max(case when o.series_name = 'federalFunds'        then o.observation_value end) as fed_funds_rate,
    max(case when o.series_name = 'CPI'                 then o.observation_value end) as us_cpi_all,
    max(case when o.series_name = 'GDP'                 then o.observation_value end) as us_gdp_nominal,
    max(case when o.series_name = 'realGDP'             then o.observation_value end) as us_gdp_real_growth,
    max(case when o.series_name = 'unemploymentRate'    then o.observation_value end) as us_unemployment,
    max(case when o.series_name = 'totalNonfarmPayroll' then o.observation_value end) as us_nonfarm_payroll,
    max(case when o.series_name = 'initialClaims'       then o.observation_value end) as us_initial_claims,
    max(case when o.series_name = 'retailSales'         then o.observation_value end) as us_retail_sales,
    max(case when t.tenor = 'month_1' then t.rate end) as yield_1m,
    max(case when t.tenor = 'month_3' then t.rate end) as yield_3m,
    max(case when t.tenor = 'year_2'  then t.rate end) as yield_2y,
    max(case when t.tenor = 'year_5'  then t.rate end) as yield_5y,
    max(case when t.tenor = 'year_10' then t.rate end) as yield_10y,
    max(case when t.tenor = 'year_30' then t.rate end) as yield_30y,
    current_timestamp as loaded_at
from spine s
left join observations o on o.date = s.date
left join treasury     t on t.date = s.date
group by s.date
