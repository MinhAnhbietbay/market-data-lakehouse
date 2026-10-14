{{
    config(
        materialized='table',
        file_format='iceberg',
        schema='curated_pit_test',
        alias='__pit_test_ratios',
        tags=['layer::curated_pit_test', 'test']
    )
}}

{{ fam_ratios_body(
    fundamental_rel=ref('pit_seed_ratios_fundamental'),
    ttm_rel=ref('pit_seed_ratios_ttm'),
    price_rel=ref('pit_seed_ratios_price'),
    analyst_rel=ref('pit_seed_ratios_analyst'),
    earnings_rel=ref('pit_seed_ratios_earnings'),
    group_rel=ref('pit_seed_ratios_group')
) }}
