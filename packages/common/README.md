# Common Package

`packages/common` contains small shared utilities that are independent of domain-heavy alpha logic and deployable services.

## Implemented Utility: Run Manifests

`run_manifest.py` owns the stable file-based execution metadata contract used by Landing and Raw batch runners:

- create run UUID and UTC timing;
- record inputs, outputs, metrics, and structured failures;
- finalize success/failed status and duration;
- atomically write JSON manifests;
- discover, load, and semantically validate manifests.

The human-readable contract is `contracts/observability/run_manifest.schema.yaml`. Operational inspection is provided by `scripts/dev/observability/run_manifests.py`.

## Boundary Rules

- Keep utilities small and reusable.
- Do not put pipeline orchestration or source-specific behavior here.
- Changes to the run-manifest fields require a schema-version decision, contract update, producer update, CLI update, and tests.
