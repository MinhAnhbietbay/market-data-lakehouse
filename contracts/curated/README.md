# Curated Contracts

Curated contracts describe consumer-facing grains and quality expectations for facts, dimensions, APIs, BI, and future alpha-field consumers.

Current state: `fact_price_daily` expectations are enforced through `analytics/dbt/models/curated/star_schema/schema.yml` and singular dbt tests. No separate Curated YAML contract is implemented yet.

Add explicit contracts here when a non-dbt consumer needs a stable interface. Do not duplicate dbt metadata without a concrete consumer need.
