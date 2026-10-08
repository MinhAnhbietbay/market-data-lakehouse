# Table Configs

Table configs define physical Iceberg behavior separately from logical raw contracts.

## Responsibility Split

| Metadata | Owns |
|---|---|
| Raw contract | Source mapping, target columns/types, nullability, primary key, metadata columns |
| Table config | Target table linkage, write mode, partition spec, Iceberg table properties |

Current configs:

- `raw/raw_fmp_ohlcv_full.yaml`
- `raw/raw_fmp_ohlcv_dividend_adjusted.yaml`

The raw batch runner discovers table configs recursively and links them to contracts by exact `target_table`. Metadata validation rejects duplicate configs, invalid transforms, missing partition columns, or target-table mismatches.

## Write Modes

- `overwrite-partitions`: repeatable daily loads; replaces only partitions present in the DataFrame.
- `append`: adds rows and can duplicate data when rerun.
- `replace-table`: rebuilds the complete table and should be used deliberately.

CLI `--write-mode` overrides config for one run.

## Partition Specs

Supported transforms are validated before runtime. The current OHLCV tables use `identity(run_date)` so same-date reruns are idempotent while older run dates remain available.

Changing YAML does not evolve an already-created Iceberg table partition spec. Partition evolution must be an explicit migration verified through Iceberg metadata tables.

## Verification

```bash
python scripts/dev/metadata/validate_metadata.py

docker compose --env-file .env -f infra/docker-compose.yml exec -T trino trino \
  --execute 'SELECT partition, record_count, file_count FROM lakehouse.raw."raw_fmp_ohlcv_full$partitions"'
```
