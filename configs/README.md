# Configs

Versioned, non-secret runtime metadata lives here. Config files describe what the platform should run; executable behavior remains in pipelines, packages, analytics, and infra.

## Structure And Consumers

| Directory | Responsibility | Primary consumer |
|---|---|---|
| `sources/` | API sources, endpoints, scopes, auth references, landing paths, runtime sets | `pipelines/landing` |
| `tables/` | Physical Iceberg write mode, partition spec, and properties | `pipelines/raw/iceberg_writer.py` |
| `environments/` | Environment-level defaults | Runtime/bootstrap tooling |
| `observability/` | Logging and monitoring settings | Shared/runtime tooling |
| `universes/` | Research symbol universes | Future scheduling/research workflows |

## Metadata Precedence

```text
source config -> how and where raw payloads are fetched/written
raw contract -> how landing payloads become typed columns
raw table config -> how the physical Iceberg table is written
CLI override -> explicit one-run operational override only
```

For Raw write mode, the precedence is:

```text
--write-mode > table config write.mode > overwrite-partitions default
```

## Onboarding Rule

To onboard an endpoint, first add it to `sources/data_sources.yaml`, then add a raw contract when structured output is required. Add a table config only when the physical table behavior must be explicit. Run:

```bash
python scripts/dev/metadata/validate_metadata.py
```

## Boundaries

- Never store real API keys, passwords, or tokens here; configs reference environment variable names.
- Do not put schema mapping into source config; it belongs in contracts.
- Do not put partition specs into raw contracts; they belong in table configs.
- Do not put executable business logic in YAML.
