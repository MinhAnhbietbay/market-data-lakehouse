# dbt Tests

## Purpose
Custom dbt data tests beyond schema YAML tests.

## What belongs here
- singular SQL tests
- cross-model integrity checks

## What does NOT belong here
- pytest tests
- API tests
- pipeline implementation

## Inputs
dbt models.

## Outputs
dbt test results for data quality gates.

## Ownership
dbt / data modeling owner

## Examples
- `assert_no_future_publish_dates.sql`
