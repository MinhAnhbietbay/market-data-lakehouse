{{
    config(materialized='table', file_format='iceberg', schema='curated_pit_test',
           alias='__pit_test_price', tags=['layer::curated_pit_test', 'test'])
}}

{{ fam_price_body(
    bars_rel=ref('pit_seed_price_bar'),
    split_rel=ref('pit_seed_stock_split'),
    dividend_rel=ref('pit_seed_cash_dividend'),
    next_trading_day_rel=ref('pit_seed_next_trading_day')
) }}
