# Raw Pipeline

Raw converts raw landing envelopes into typed, source-aligned Iceberg tables. Generic Spark runtime behavior is driven by raw contracts and optional table configs.

## Metadata And Runtime Flow

```text
contracts/raw/<source>/<table>.schema.yaml
  owns payload traversal, mapping, types, keys, metadata columns

configs/tables/raw/<table>.yaml
  owns write mode, partition spec, Iceberg properties

-> pipelines.raw.run_batch
-> mapper + validation
-> Iceberg writer
-> lakehouse.raw.*
-> raw_batch run manifest
```

Only landing envelopes with `response_status = 200` are loaded. `landing_key` preserves raw-file lineage.

## Canonical Batch Command

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec -T spark-master \
  spark-submit --master spark://spark-master:7077 /workspace/pipelines/raw/run_batch.py \
    --contracts-root /workspace/contracts/raw \
    --tables-root /workspace/configs/tables \
    --source fmp \
    --endpoint ohlcv_full \
    --endpoint ohlcv_dividend_adjusted \
    --run-date 2026-06-04
```

The runner discovers contracts and table configs, rejects unmatched filters, runs selected contracts in one Spark application, and always writes a final run manifest to the shared host directory `artifacts/run-manifests`.

Use `--skip-missing-landing` when intentionally running a broad contract set for a date where not every endpoint landed. Use `--allow-empty` only when an empty successful payload is expected.

## Write Behavior

```text
--write-mode override
> matching table config write.mode
> overwrite-partitions default
```

- `overwrite-partitions`: repeatable daily default.
- `append`: adds rows without rerun protection.
- `replace-table`: rebuilds the complete table.

Tables without a matching config fall back to Iceberg format version 2 and `identity(run_date)`. New production tables should normally receive explicit table configs.

## Single-Contract Debugging

```bash
docker compose --env-file .env -f infra/docker-compose.yml exec -T spark-master \
  spark-submit --master spark://spark-master:7077 /workspace/pipelines/raw/load_to_raw.py \
    --contract /workspace/contracts/raw/fmp/raw_fmp_ohlcv_full.schema.yaml \
    --table-config /workspace/configs/tables/raw/raw_fmp_ohlcv_full.yaml \
    --run-date 2026-06-04
```

## Verification And Invariants

- Metadata validation must pass before runtime.
- Same-date `overwrite-partitions` reruns must keep row counts stable.
- Contract schema changes and Iceberg partition evolution are separate concerns.
- Airflow should call `run_batch.py`; it must not reproduce contract discovery or mapping logic.

See `docs/ohlcv_e2e_runbook.md` for Trino snapshot/partition verification.

## NEVER Delete `run_date` Partitions

Each `run_date` partition is an immutable snapshot of what the source returned on that run. This sequence is the **only transaction-time substrate** in the platform: the standardize layer derives its version history (`known_from`, `valid_from`/`valid_to`) directly from these partitions, and the curated layer's invariant that "a past day's cell never changes across rebuilds" rests upon it.

Deleting an old `run_date` partition does not merely save space — it permanently destroys the ability to rebuild version history for that period, and makes point-in-time backtests over that period silently wrong.

Constraint D10, `docs/superpowers/specs/2026-08-20-pit-bitemporal-spark-migration-design.md`.
