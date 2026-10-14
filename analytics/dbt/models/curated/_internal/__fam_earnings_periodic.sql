{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_earnings_periodic',
        tags=['layer::curated', 'internal']
    )
}}

{{ fam_earnings_body(source_rel=ref('std_ev_earnings_announcement')) }}
