# Contracts

Contracts define logical data expectations. They are versioned metadata consumed by validation and pipelines; they do not contain executable transformation logic.

## Implemented Contract Types

| Location | Responsibility | Consumer |
|---|---|---|
| `landing/raw_api_payload.schema.yaml` | Raw API envelope and audit metadata | Landing conventions/tests |
| `raw/<source>/*.schema.yaml` | Payload traversal, source mapping, typed target columns, metadata columns, primary key | Spark Raw pipeline |
| `observability/run_manifest.schema.yaml` | Final pipeline run metadata fields and semantic rules | Run-manifest validator |
| `standardize/`, `curated/` | Future explicit cross-tool contracts | dbt/data modeling work |

## Raw Contract Linkage

Every raw contract must link to:

```text
source + source_endpoint -> configs/sources/data_sources.yaml
target_table -> unique physical raw table
optional matching target_table -> configs/tables/raw/*.yaml
```

The metadata validator checks required fields, source/endpoint existence, duplicate target tables/columns, typed columns, and primary-key references.

## Boundary Rules

- Source API request behavior belongs in source config.
- Physical partition/write behavior belongs in table config.
- SQL integration and serving logic belongs in dbt.
- Contracts should describe stable expectations that can be validated before a pipeline starts.

Run all metadata linkage checks with:

```bash
python scripts/dev/metadata/validate_metadata.py
```
