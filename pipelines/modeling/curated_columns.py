from __future__ import annotations

import os

# dbt's models/curated/* config hardcodes `+schema: curated`
# (analytics/dbt/dbt_project.yml), and generate_schema_name.sql
# unconditionally returns that custom_schema_name when set, ignoring
# target.schema entirely. So curated tables always live in Trino schema
# "curated" regardless of what TRINO_SCHEMA/target.schema is configured to
# (e.g. "dbt_dev" in dev). Querying information_schema.columns for curated
# tables must use this constant, not the TRINO_SCHEMA env var.
CURATED_SCHEMA = "curated"


def read_curated_columns(names: list[str]) -> dict[str, set[str]]:
    """Read the live column set of each named curated table from Trino.

    Connects using the same env vars already wired onto the Airflow service
    (infra/docker-compose.yml): TRINO_HOST, TRINO_PORT, TRINO_USER,
    TRINO_CATALOG. Returns {table_name: {column_name, ...}},
    matching the shape `packages.common.catalog_curated_guard.check()` expects.

    The `information_schema.columns` query is filtered to CURATED_SCHEMA
    ("curated"), not TRINO_SCHEMA — see the module-level comment on
    CURATED_SCHEMA for why: dbt's generate_schema_name macro always routes
    curated/* models to the "curated" schema, independent of target.schema.

    Note on import style: `trino` is imported lazily (inside the function,
    not at module top), matching the sibling stamp_build_id.py's pattern of
    lazy-importing its runtime dependency (pyiceberg) even though it's a
    guaranteed-present dependency in the Airflow image — this keeps both
    `pipelines/modeling/*` modules importable (e.g. for unrelated unit tests)
    in environments where `trino`/`pyiceberg` aren't installed, at the cost
    of deferring the ImportError to call time instead of import time.
    """
    import trino

    host = os.environ["TRINO_HOST"]
    port = int(os.environ["TRINO_PORT"])
    user = os.environ.get("TRINO_USER", "alpha")
    catalog = os.environ.get("TRINO_CATALOG", "lakehouse")
    schema = CURATED_SCHEMA

    if not names:
        return {}

    conn = trino.dbapi.connect(host=host, port=port, user=user, catalog=catalog, schema=schema)
    try:
        cur = conn.cursor()
        placeholders = ", ".join(["?"] * len(names))
        cur.execute(
            f"""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_catalog = ?
              AND table_schema = ?
              AND table_name IN ({placeholders})
            """,
            [catalog, schema, *names],
        )
        result: dict[str, set[str]] = {name: set() for name in names}
        for table_name, column_name in cur.fetchall():
            result.setdefault(table_name, set()).add(column_name)
        return result
    finally:
        conn.close()
