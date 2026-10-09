# Trino Infra

## Purpose
Trino query engine configuration for dbt and ad-hoc SQL over lakehouse tables.

## What belongs here
- catalog config
- coordinator config
- JVM/node config

## What does NOT belong here
- dbt models
- API query code

## Inputs
Lakehouse storage/catalog metadata.

## Outputs
SQL query service for dbt/API/analytics.

## Ownership
Infrastructure owner

## Examples
- `catalog/lakehouse.properties`
