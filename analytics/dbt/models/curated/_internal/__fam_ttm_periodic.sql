{{
    config(
        materialized='table',
        file_format='iceberg',
        schema=var('env_curated'),
        alias='__fam_ttm_periodic',
        tags=['layer::curated', 'internal']
    )
}}

{{ fam_ttm_body(
    income_rel=ref('std_ac_income_statement'),
    balance_rel=ref('std_ac_balance_sheet_statement'),
    cashflow_rel=ref('std_ac_cash_flow_statement'),
    owner_earnings_rel=ref('std_ac_owner_earnings')
) }}
