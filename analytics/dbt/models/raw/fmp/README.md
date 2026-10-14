# dbt Raw: FMP

## Purpose
FMP-specific dbt raw models over Spark-produced FMP raw tables.

## What belongs here
- `raw_fmp_<entity>.sql` models
- source-aligned tests and docs

## What does NOT belong here
- source-independent canonical models
- curated tables
- FMP API clients

## Inputs
FMP raw Iceberg/Parquet tables created by Spark.

## Outputs
FMP dbt raw views/models for standardize models.

## Naming conventions
`raw_<source>_<entity>.sql` for source-aligned dbt raw models.

## Ownership
dbt / data modeling owner

## Examples
- `raw_fmp_historical_price.sql`
- `raw_fmp_income_statement.sql`
