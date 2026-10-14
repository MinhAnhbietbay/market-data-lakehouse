{{
    config(materialized='table', file_format='iceberg', schema='curated_pit_test',
           alias='__pit_test_group', tags=['layer::curated_pit_test', 'test'])
}}

{{ fam_group_body(
    listing_rel=ref('pit_seed_ar_listing'),
    spine_rel=ref('pit_seed_group_spine'),
    security_rel=ref('pit_seed_pd_security'),
    legal_entity_rel=ref('pit_seed_ip_legal_entity'),
    workforce_rel=ref('pit_seed_workforce')
) }}
