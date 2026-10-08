# Raw Contracts

Raw contracts are the logical source of truth for converting one landing endpoint into one typed, source-aligned Iceberg table.

## Required Shape

Each `*.schema.yaml` declares at least:

- `source` and `source_endpoint`: linkage to `configs/sources/data_sources.yaml`.
- `target_table`: unique fully qualified Iceberg table.
- `payload`: root/explode rules for navigating the raw response.
- `columns`: target names, source fields, types, and nullability.
- `metadata_columns`: lineage/audit fields added during mapping.
- `primary_key`: expected technical/business grain columns.

## Runtime Flow

```text
contract
-> locate landing files for source/endpoint/run_date
-> mapper extracts and casts payload columns
-> mapper adds metadata columns
-> validation checks non-nullable fields and empty-data policy
-> Iceberg writer applies matching table config
```

A contract owns shape and mapping, not partition specs or dbt business transformations.

## Primary Key Nullability

A `primary_key` column may be `nullable: true` — `primary_key` here documents intended
uniqueness, not a database-level NOT NULL constraint, and PK uniqueness itself is not enforced
anywhere in this pipeline. Only mark a PK column `nullable: false` when you have verified,
against real landed data, that it is genuinely never null for this endpoint — `nullable: false`
IS enforced at raw load time (`pipelines/raw/validation.py`) and will hard-fail the
whole load on the first null value found, including one that shows up in data landed after the
contract was written.

## Add Or Change A Contract

1. Confirm the endpoint exists in source config.
2. Derive the contract's column list from `contracts/vocabulary/field_concept_map.yaml` —
   nothing extra, nothing missing. The vocabulary is the source of truth for which fields
   belong to a contract, not the raw FMP payload or prior habit.
3. Add/update the contract without changing generic mapper behavior unless the payload pattern is genuinely new.
4. Add a table config only when physical behavior must be explicit.
5. Run metadata and unit tests.
6. Run one Landing -> Raw slice and verify through Trino.

```bash
python scripts/dev/metadata/validate_metadata.py
python -m pytest tests/unit/metadata tests/unit/raw
```
