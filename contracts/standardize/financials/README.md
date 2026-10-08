# Standardize Contracts: Financials

## Purpose
Contracts for financial statement and fundamental entities.

## What belongs here
- statement grains
- line-item rules
- publish-date constraints

## What does NOT belong here
- raw JSON payload specs
- UI metrics formatting

## Inputs
Raw financial data and core issuer entities.

## Outputs
Financial entity contracts.

## Ownership
dbt / data modeling owner

## Examples
- `financial_statement.md`
- `financial_statement_line_item.md`
