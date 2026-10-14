{{
    config(
        materialized='table',
        file_format='iceberg',
        schema='curated_pit_test',
        alias='__pit_test_ttm',
        tags=['layer::curated_pit_test', 'test']
    )
}}

{{ fam_ttm_body(
    income_rel=ref('pit_seed_ttm_income'),
    balance_rel=ref('pit_seed_ttm_balance'),
    cashflow_rel=ref('pit_seed_ttm_cashflow')
) }}
