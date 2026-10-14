# dbt Models: Curated

## Purpose
Serving models for analytics, APIs, alpha engine, and backtesting. This layer contains `star_schema` and `alpha_fields`.

## What belongs here
- dimensional tables
- facts
- alpha field-oriented datasets

## What does NOT belong here
- canonical 3NF modeling
- source API parsing
- business logic hidden in services

## Inputs
Standardize canonical models.

## Outputs
Serving tables for API/web/alpha/backtester consumers.

## Ownership
dbt / data modeling owner

## Examples
- `star_schema/dim_security.sql`
- `alpha_fields/alpha_price_fields.sql`
