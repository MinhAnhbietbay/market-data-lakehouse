# Landing Ingestion Pipeline

Landing performs config-driven API ingestion and stores auditable raw response envelopes. Adding a normal endpoint should require source config, not a new pipeline implementation.

## Source Of Truth And Flow

```text
configs/sources/data_sources.yaml
  -> source/endpoint selection
  -> auth environment reference + request params
  -> source API
  -> contracts/landing/raw_api_payload.schema.yaml envelope
  -> s3://lakehouse/landing/<source>/<endpoint>/dt=<run_date>/<partition_key>.json
  -> landing_batch run manifest
```

The raw envelope records request metadata, masked secret parameters, response status, raw payload, and ingestion timing. HTTP error responses are still written for auditability; Raw only loads successful response payloads.

## Canonical Batch Command

```bash
python -m pipelines.landing.run_batch \
  --source fmp \
  --endpoint ohlcv_full \
  --endpoint ohlcv_dividend_adjusted \
  --symbol AAPL \
  --run-date 2026-06-04 \
  --write-target minio
```

The batch runner validates selected source/endpoint names, skips disabled endpoints unless `--include-disabled` is supplied, and emits a final manifest for success or handled failure. Use `--continue-on-error` only when independent endpoints should continue after one fails.

By default manifests are written to:

```text
artifacts/run-manifests/landing_batch/<run_date>/<run_id>.json
```

`--manifest-root` changes the root; `--manifest-path` is an explicit one-run override.

## Single-Endpoint Debugging

```bash
python -m pipelines.landing.ingest \
  --source fmp --endpoint profile --symbol AAPL --write-target local
```

Use the single endpoint command for debugging. Airflow should call the batch runner instead.

## One-Off Deep Backfill

The `windows:` block in `data_sources.yaml` sets the depth of the **recurring** run. A
deep historical backfill is a different job: it costs far more requests than one nightly
run may spend, so it is passed per-invocation instead of being written into config.

```bash
# 10 years of Treasury yields: 42 requests (FMP truncates at 90 calendar days/request)
python -m pipelines.landing.run_batch   --source fmp --endpoint treasury_rates --run-date <today>   --window-from 2016-09-22 --window-to 2026-09-21 --window-split-every=-89d

# same for macro series -- one pass per series, so pass only the ones you need
python -m pipelines.landing.run_batch   --source fmp --endpoint economic_indicators --run-date <today>   --series-id federalFunds --series-id CPI   --window-from 2016-09-22 --window-to 2026-09-21 --window-split-every=-89d
```

Three things that are easy to get wrong here:

- **`--window-split-every=-89d` needs the `=`.** argparse reads a bare `-89d` as an
  option name and fails. `89d` without the minus works as a plain argument.
- **Never widen the chunk past 90 days.** FMP's `/treasury-rates` and
  `/economic-indicators` return only 90 calendar days counting back from `to`, and they
  truncate *silently* -- measured 2026-09-22, `from=2020-01-01&to=2020-04-30` returns 63
  rows whose oldest is 2020-01-31, so January vanishes with a 200 status. `-3m` is not
  safe either: the Mar-May and Jul-Sep quarters are 92 days.
- **Staging across days is fine.** Landing writes under `dt=<run-date>`, raw appends a
  new `run_date` partition, and the standardize models read the *whole* raw table and
  resolve versions bitemporally. So a backfill split over several days accumulates into
  one continuous history instead of replacing the previous day's slice. Run the stages on
  whatever days the vendor quota allows; `run_batch` stops the run cleanly and records it
  in the manifest when the vendor answers "Limit Reach".

Only endpoints that already declare `windows:` are affected by the override -- an endpoint
without one has no `{from}` in its `partition_key`, so many windows would collapse onto a
single landing object (`validate_window_partition_keys` is the guard for that).

## Required Environment

- FMP: `FMP_API_KEY`
- MinIO/S3: `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`

Secrets are supplied through environment variables and must never be committed to source config.

## Invariants

- Source/endpoint/path/scope/auth references come from source config.
- Landing objects are raw and replayable; no business modeling occurs here.
- Secret values are masked in stored request metadata.
- A failed selection or endpoint execution exits non-zero and produces a failed run manifest.
