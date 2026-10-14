{{
    config(materialized='table', partition_by=['months(date)'], file_format='iceberg',
           schema=var('env_curated'), alias='__fam_group', tags=['layer::curated', 'internal'])
}}

{{ fam_group_body(
    listing_rel=ref('std_ar_listing'),
    spine_rel=ref('__spine_daily'),
    security_rel=ref('std_pd_security'),
    legal_entity_rel=ref('std_ip_legal_entity'),
    workforce_rel=ref('std_ip_workforce_observation')
) }}
