{{
    config(materialized='table', file_format='iceberg', schema='curated_pit_test',
           alias='__pit_test_universe', tags=['layer::curated_pit_test', 'test'])
}}

-- window_days=3, top_n=3: hằng số NHỎ để fixture hand-computable, KHÔNG phải
-- hằng số thật (63/3000) -- cùng lý do fam_price_body.sql/fam_group_body.sql
-- dùng seed nhỏ thay vì dữ liệu thật. Logic universe_membership_body giữ
-- NGUYÊN, chỉ đổi tham số -- không còn mirror tay (Quyết định 2026-08-26,
-- đóng nốt backlog #3 sau khi price/group đã tách macro).
{{ universe_membership_body(
    price_rel=ref('pit_seed_universe_bars'),
    dim_date_rel=ref('pit_seed_universe_trading_days'),
    window_days=3,
    top_n=3,
    include_adv_debug_columns=true
) }}
