{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_ratios',
        tags=['layer::curated', 'internal']
    )
}}

{{ fam_ratios_body(
    fundamental_rel=ref('__fam_fundamental'),
    ttm_rel=ref('__fam_ttm'),
    price_rel=ref('__fam_price'),
    analyst_rel=ref('__fam_analyst'),
    earnings_rel=ref('__fam_earnings'),
    group_rel=ref('__fam_group')
) }}
