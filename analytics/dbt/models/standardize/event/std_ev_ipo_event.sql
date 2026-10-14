{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_ev_ipo_event',
        tags=['layer::standardize', 'domain::event']
    )
}}

-- Empty by design: /ipos-calendar và /ipos-disclosure đều chưa có raw contract
-- (enabled: false, paid_required).
--
-- Cột PIT là cast(null) chứ không phải kết quả bitemporal_version_chain:
-- không có raw source thì không có version chain để chạy. Khi raw contract về,
-- thay khối này bằng:
--   bitemporal_version_chain [source_cte='ipos_src', key_cols=['symbol'],
--        fingerprint_cols=[...], first_known_from_expr='effectiveness_date',
--        pit_basis='filing']
-- (spec 3b §5)
select
    cast(null as string) as symbol,
    cast(null as date) as effectiveness_date,
    cast(null as date) as source_run_date,
    cast(null as date) as known_from,
    cast(null as string) as known_from_basis,
    cast(null as string) as pit_basis
where false
