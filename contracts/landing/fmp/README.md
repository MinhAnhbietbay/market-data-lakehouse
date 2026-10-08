# Landing Contracts: FMP

## Purpose
Source-level contracts for raw FMP API JSON payloads.

## What belongs here
- FMP endpoint schemas
- source payload examples
- upstream change notes

## What does NOT belong here
- Spark table schemas
- dbt model contracts

## Inputs
FMP API responses.

## Outputs
Validation expectations for FMP landing files.

## Ownership
Data ingestion / Spark owner

## Examples
- `historical_price.schema.json`
- `income_statement.schema.json`
