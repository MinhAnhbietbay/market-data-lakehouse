{{
    config(
        materialized='table',
        partition_by=['months(date)'],
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_universe_membership',
        tags=['layer::curated', 'engine_table']
    )
}}

{{ universe_membership_body(
    price_rel=ref('__fam_price'),
    dim_date_rel=ref('curated_dim_date'),
    window_days=var('universe_adv_window_days', 63),
    top_n=var('universe_top_n', 3000)
) }}
