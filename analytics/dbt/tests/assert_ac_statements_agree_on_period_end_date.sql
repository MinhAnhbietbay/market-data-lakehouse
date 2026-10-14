-- Singular cross-model test: the 3 quarterly financial-statement tables
-- (income_statement, balance_sheet_statement, cash_flow_statement) must
-- report the SAME period_end_date for the same (symbol, fiscal_year, period)
-- triple. This is the one guarantee spec D1 gave up by not building a
-- shared Fiscal Period dimension -- this test recovers it without needing
-- dbt_utils or a composite-key relationships test.
with combined as (
    select symbol, fiscal_year, period, period_end_date, 'income_statement' as source_table
    from {{ ref('std_ac_income_statement') }}
    union all
    select symbol, fiscal_year, period, period_end_date, 'balance_sheet_statement' as source_table
    from {{ ref('std_ac_balance_sheet_statement') }}
    union all
    select symbol, fiscal_year, period, period_end_date, 'cash_flow_statement' as source_table
    from {{ ref('std_ac_cash_flow_statement') }}
)

select
    symbol,
    fiscal_year,
    period,
    count(distinct period_end_date) as distinct_period_end_dates
from combined
group by symbol, fiscal_year, period
having count(distinct period_end_date) > 1
