# dbt Models: Raw

## Purpose
Lightweight source-aligned dbt models/views over Spark-produced raw Iceberg tables. Source-specific raw models live under subfolders such as `fmp/`.

## What belongs here
- Thin cleanup SQL
- source-aligned column selection
- minor SQL-facing renames

## What does NOT belong here
- canonical entity modeling
- business marts
- Spark parsing logic

## Inputs
Spark raw tables.

## Outputs
dbt raw models named `raw_<source>_<entity>.sql`.

## Naming conventions
`raw_<source>_<entity>.sql` for source-aligned dbt raw models.

## Ownership
dbt / data modeling owner

## Examples
- `fmp/raw_fmp_prices.sql`
- `fmp/raw_fmp_profile.sql`
