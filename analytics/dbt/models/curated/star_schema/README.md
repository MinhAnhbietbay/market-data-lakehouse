# Curated: Star Schema

## Purpose
Dimensional serving tables for analytics, BI, monitoring, and API queries.

## What belongs here
- dimensions named `dim_<entity>.sql`
- facts named `fact_<business_process>.sql`
- bridges named `bridge_<relationship>.sql`

## What does NOT belong here
- alpha expression field stores
- canonical standardize entities

## Inputs
Standardize canonical models.

## Outputs
Star schema tables consumed by API, BI, and reports.

## Naming conventions
`curated_dim_<entity>.sql`, `curated_fact_<business_process>.sql` và `curated_bridge_<relationship>.sql`.

Tiền tố `curated_` là bắt buộc: mọi bảng curated đã ship đều mang nó, và ERD,
schema.yml, guard test cùng spec engine đều gọi bảng theo tên có tiền tố.

## Ownership
dbt / data modeling owner

## Examples
- `curated_dim_security.sql`
- `curated_dim_company.sql`
- `curated_fact_analyst_grade_action.sql`
- `curated_bridge_index_membership.sql`
