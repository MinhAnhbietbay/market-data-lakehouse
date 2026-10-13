# dbt Analytics

dbt uses Trino to transform physical Iceberg Raw tables into canonical Standardize models and consumer-facing Curated models.

## Implemented Lineage

```text
source("raw", "raw_fmp_ohlcv_full")
+ source("raw", "raw_fmp_ohlcv_dividend_adjusted")
-> lakehouse.standardize.std_tx_daily_price_bar
-> lakehouse.curated.fact_price_daily
```

`std_tx_daily_price_bar` selects the latest record per `(symbol, price_date)` from each Raw source using ingestion lineage timestamps/keys before joining raw and adjusted prices. This protects the canonical grain from repeated Landing/Raw runs.

`fact_price_daily` exposes the serving grain `(symbol, price_date)` with stable date/security keys and price fields.

## Directory Responsibilities

| Location | Responsibility |
|---|---|
| `models/sources/raw.yml` | Declares physical Spark-created Raw tables and source tests |
| `models/standardize/` | Canonical source-independent integration/dedup logic |
| `models/curated/` | Consumer-facing facts/dimensions/alpha fields |
| `tests/` | Singular cross-row/business-grain tests |
| `macros/generate_schema_name.sql` | Maps dbt custom schemas to `lakehouse.standardize` and `lakehouse.curated` |

## Canonical Docker Commands

```bash
docker compose --env-file .env -f infra/docker-compose.yml run --rm dbt parse
docker compose --env-file .env -f infra/docker-compose.yml run --rm dbt run --select +fact_price_daily
docker compose --env-file .env -f infra/docker-compose.yml run --rm dbt test --select +fact_price_daily
```

The `+fact_price_daily` selector includes upstream Standardize dependencies and relevant tests.

## Quality Expectations

- Raw source lineage fields required for deterministic deduplication are non-null.
- Standardize and Curated `(symbol, price_date)` grains are unique.
- Required serving keys and dates are non-null.
- Volume is non-negative.
- A model change is not complete until `dbt run` and `dbt test` pass against the local Trino runtime.

## Boundaries

- Do not parse raw landing JSON in dbt.
- Do not reproduce Spark raw mapping in SQL.
- Do not hard-code development schema suffixes; use the schema-name macro/project config.
- New models should state their grain and add tests proportional to consumer risk.
