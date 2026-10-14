{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_standardize'),
        alias='std_tx_insider_transaction_type',
        tags=['layer::standardize', 'domain::transaction']
    )
}}

select distinct transaction_type
from {{ source('raw', 'raw_fmp_insider_trading_transaction_type') }}
where transaction_type is not null
