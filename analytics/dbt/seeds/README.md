# dbt Seeds

## Purpose
Small static reference datasets used by dbt models.

## What belongs here
- CSV mappings
- small calendars
- exchange/sector reference maps

## What does NOT belong here
- large market data
- raw FMP payloads
- private secrets

## Inputs
Versioned static files.

## Outputs
Seed tables available to dbt models.

## Ownership
dbt / data modeling owner

## Examples
- `exchange_map.csv`
- `sector_map.csv`
