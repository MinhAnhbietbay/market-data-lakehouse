{{
    config(
        materialized='table',
        file_format='iceberg',
        schema='curated_pit_test',
        alias='__pit_test_earnings',
        tags=['layer::curated_pit_test', 'test']
    )
}}

{{ fam_earnings_body(source_rel=ref('pit_seed_earnings')) }}
