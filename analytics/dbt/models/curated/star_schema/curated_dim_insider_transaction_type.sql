{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='curated_dim_insider_transaction_type',
        tags=['layer::curated', 'star_table']
    )
}}

-- Từ điển mã loại giao dịch nội bộ. Fact dùng nó (fact_insider_transaction)
-- thuộc đợt 3b, chưa tồn tại -- dim này hiện chưa có fact nào trỏ tới. 18 dòng.

select
    transaction_type,
    current_timestamp as loaded_at
from {{ ref('std_tx_insider_transaction_type') }}
