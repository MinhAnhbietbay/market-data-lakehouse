{{
    config(
        materialized='table',
        file_format='iceberg',
        schema='curated_pit_test',
        alias='__pit_test_period_select',
        tags=['layer::curated_pit_test', 'test']
    )
}}

{{ collapse_asof(
    source_rel=ref('pit_seed_period_select_metric'),
    key_cols=['symbol'],
    spine_rel=ref('pit_seed_period_select_spine'),
    shape='A',
    max_staleness_days=180,
    period_col='period_end_date'
) }}
