# Source Config

`data_sources.yaml` is the source of truth for external API ingestion metadata.

It defines sources, endpoints, request paths/parameters, endpoint scope, authentication environment references, enabled state, landing object naming, and shared runtime sets. It must not contain real secret values.

The generic Landing pipeline consumes this file. A normal new endpoint should be onboarded here without creating a new ingestion module.

After any change:

```bash
python scripts/dev/metadata/validate_metadata.py
python -m pytest tests/unit/metadata tests/unit/landing
```

Add a matching Staging contract only when the endpoint should produce a typed Staging table.
