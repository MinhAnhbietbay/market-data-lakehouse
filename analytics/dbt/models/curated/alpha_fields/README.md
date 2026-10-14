# Curated: Alpha Fields

## Purpose
Field-oriented data consumed by `alpha_dsl`, `alpha_engine`, and `backtester`. This belongs in curated because it is optimized for alpha serving, not canonical enterprise modeling.

## What belongs here
- field groups named `alpha_<field_group>.sql`
- date/symbol aligned fields
- coverage and missing-data handling

## What does NOT belong here
- 3NF canonical entities
- source-specific raw cleanup
- portfolio simulation code

## Inputs
Standardize and star schema models.

## Outputs
Alpha-ready fields for expression evaluation and simulation.

## Naming conventions
`alpha_<field_group>.sql` for alpha-engine-facing field groups.

## Ownership
dbt / data modeling owner with alpha engine owner

## Examples
- `alpha_price_fields.sql`: close, volume, returns, vwap
- `alpha_liquidity_fields.sql`: adv20, adv60, turnover
- `alpha_security_fields.sql`: market_cap, sector, industry

## Expected Grain

Most daily alpha field models should use `trade_date + instrument_id`. Optional user-facing aliases such as `symbol` may be included, but `instrument_id` should be the stable join key.
