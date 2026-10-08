# Observability Contracts

`run_manifest.schema.yaml` documents the stable file-based run metadata emitted by Landing and Staging batch runners.

Runtime validation is implemented in `packages/common/run_manifest.py`. Final manifests must have schema version `1`, valid timing fields, a final success/failed status, and consistent failure semantics.

```bash
python scripts/dev/observability/run_manifests.py validate
```

Airflow and future metadata databases should consume/correlate this contract rather than inventing a second run-status format.
