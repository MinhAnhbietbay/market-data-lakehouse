"""`data_build_id` stamper — the cross-table consistency anchor (Task 7).

Iceberg gives snapshot isolation per TABLE; there is no multi-table
transaction across `curated_alpha_fields_daily`, `curated_macro_daily` and
`curated_universe_membership`. The Quant Compute Engine therefore does not pin
each table's `current_snapshot()` independently (which could pair a `close`
from build N with a `vix` from build N-1); it pins every table to one shared
`data_build_id` written by a single modeling run. See
docs/superpowers/specs/quant-compute-engine.md §3.2 and
docs/superpowers/specs/ingestion-pipeline.md §3.6.1.

HOW THE STAMP IS CARRIED — and why it is not what the plan sketched.
The plan's snippet called `tx.set_snapshot_summary_property(...)`. That method
DOES NOT EXIST on `pyiceberg.table.Transaction` (verified against the pinned
pyiceberg 0.11.1 in infra/airflow/Dockerfile — the full public surface is
add_files, append, commit_transaction, delete, dynamic_partition_overwrite,
overwrite, remove_properties, set_properties, table_metadata, update_location,
update_schema, update_snapshot, update_sort_order, update_spec,
update_statistics, upgrade_table_version, upsert). Calling it raised
AttributeError on every modeling run.

More fundamentally, no such method could exist: an Iceberg snapshot summary is
written when the snapshot is CREATED and the snapshot is immutable thereafter.
The format has no "mutate the current snapshot's summary" operation. The only
API that writes summary properties is `update_snapshot(snapshot_properties=...)`,
which attaches them to a NEW snapshot.

So the stamp is an EMPTY `fast_append` carrying the property. A fast append
with zero new data files re-uses the parent snapshot's manifests verbatim, so
the new snapshot has byte-identical content to the dbt build it stamps — it
adds a summary label, never data. This keeps the engine's documented contract
intact:

  * The value lives in the SNAPSHOT SUMMARY, which is what
    `snapshot_with_property(table, "data_build_id", build_id)` (engine §3.2)
    reads.
  * Snapshot summaries are HISTORICAL. Scanning them lets the engine's
    `latest_common_build_id` fall back to the newest build present in ALL
    tables when one curated lags behind — engine spec §3.5 test 13 depends on
    exactly this, and `iceberg_maintenance`'s synchronized ≥14-day retention
    exists to keep that history intact.
  * Each stamped build_id maps to a concrete `snapshot_id` per table, which
    the engine records in `SimulationResult.source_snapshot_ids` (engine §3.7).

REJECTED ALTERNATIVE — the plan's own fallback (line 507: "fall back to a
table property `data_build_id`", i.e. `tx.set_properties(...)`). It is a real
API and it does commit, but a table property holds only its CURRENT value:
no history, and no build_id -> snapshot_id mapping. `latest_common_build_id`
would degrade from "newest build present in every table" to "current build
only" — and in the very scenario it exists for (curated A at build N, curated B
still at N-1) it would find no common value at all and fail the run instead of
anchoring both tables to N-1. The fallback was written for the case where the
installed PyIceberg "lacks a direct snapshot-summary setter"; 0.11.1 does not
lack one, it just isn't the method name the plan guessed. So the snapshot-
summary carrier stands, and the engine plan needs no contract change.

COST: one extra, empty snapshot per engine curated per modeling run. That is
deliberate and cheap (no data files, one small manifest list), and it is the
Iceberg-native way to label a build. `iceberg_maintenance`'s `expire_snapshots`
already bounds the accumulation with a uniform retention window.
"""
from __future__ import annotations

import datetime as dt

# The summary key the Quant Compute Engine reads (quant-compute-engine §3.2).
# Changing this string breaks the engine's anchor — it is a published contract,
# not an implementation detail.
DATA_BUILD_ID_PROPERTY = "data_build_id"


def new_build_id(now: dt.datetime | None = None) -> str:
    """Generate a sortable UTC timestamp string for build_id.

    Sortable is a hard requirement, not a preference: the engine's
    `latest_common_build_id` takes the MAXIMUM build_id present in every
    table, so lexical order must equal chronological order. Hashes/UUIDs are
    forbidden — they would make "latest" meaningless without erroring.

    Args:
        now: Optional datetime object (must be UTC-aware). Defaults to current UTC time.

    Returns:
        A sortable timestamp string in format YYYY-MM-DDTHH-MM-SSZ
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")


def stamp(tables: list[str], build_id: str, catalog=None) -> None:
    """Write `data_build_id=<build_id>` into each table's snapshot summary.

    Commits one empty `fast_append` per table, carrying the build_id as a
    snapshot summary property — see the module docstring for why this, and
    not a table property or a post-hoc summary mutation. Content is unchanged:
    the new snapshot re-uses the parent's manifests.

    Every table is stamped with the SAME build_id in one call; that shared
    label is the whole point. Each table still commits independently (Iceberg
    has no multi-table transaction) — the engine tolerates a partial stamp by
    anchoring to the newest build_id common to all tables.

    Args:
        tables: Iceberg table identifiers to stamp (e.g. "curated.curated_macro_daily").
        build_id: The shared, sortable build_id (see `new_build_id`).
        catalog: Optional PyIceberg catalog instance. Defaults to loading 'default'.
    """
    from pyiceberg.catalog import load_catalog

    cat = catalog or load_catalog("default")
    for name in tables:
        tbl = cat.load_table(name)
        with tbl.transaction() as tx:
            tx.update_snapshot(
                snapshot_properties={DATA_BUILD_ID_PROPERTY: build_id}
            ).fast_append().commit()
