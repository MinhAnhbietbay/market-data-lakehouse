{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_cm_trading_day',
        tags=['layer::standardize', 'domain::common']
    )
}}

select distinct trade_date
from {{ ref('std_tx_daily_price_bar') }}
where trade_date is not null
