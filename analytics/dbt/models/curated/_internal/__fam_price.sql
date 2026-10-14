{{
    config(
        materialized='table',
        partition_by=['months(date)'],
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_price',
        tags=['layer::curated', 'internal']
    )
}}

{{ fam_price_body(
    bars_rel=ref('std_tx_daily_price_bar'),
    split_rel=ref('std_ev_stock_split'),
    dividend_rel=ref('std_ev_cash_dividend'),
    next_trading_day_rel=ref('std_cm_next_trading_day')
) }}
