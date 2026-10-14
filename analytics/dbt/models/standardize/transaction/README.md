# Standardize Transaction

## Purpose
Canonical Transaction-concept (`tx`) market data entities such as price observations, corporate actions, trading calendars, and benchmarks.

## What belongs here
- Market event/fact entities
- adjustment-aware prices
- corporate action normalization

## What does NOT belong here
- source-specific raw parsing
- star schema facts
- alpha field matrices

## Inputs
raw models and core entities.

## Outputs
Reusable transaction entities for curated models.

## Naming conventions
`std_tx_<entity>.sql` for canonical integrated Transaction-concept entities.

## Ownership
dbt / data modeling owner

## Examples
- `std_tx_daily_price_bar.sql`
- `std_tx_market_cap_observation.sql`
- `std_tx_insider_transaction_type.sql`
- `std_tx_insider_activity_summary.sql`
- `std_tx_insider_transaction.sql`
