import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession

from packages.common.run_manifest import RunManifest, resolve_manifest_path
from pipelines.raw.contract import get_target_table, load_contract, select_contract_paths
from pipelines.raw.load_to_raw import load_contract_to_raw
from pipelines.raw.table_config import discover_table_configs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run multiple raw contracts in one Spark application.")
    parser.add_argument("--contracts-root", default="/workspace/contracts/raw")
    parser.add_argument("--tables-root", default="/workspace/configs/tables")
    parser.add_argument("--contract", action="append", default=[])
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--endpoint", action="append", default=[])
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--skip-missing-landing", action="store_true")
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument(
        "--create-missing-as-empty",
        action="store_true",
        help="Landing thiếu và bảng chưa tồn tại thì dựng bảng rỗng từ schema contract.",
    )
    parser.add_argument(
        "--write-mode",
        choices=["overwrite-partitions", "append", "replace-table"],
        default=None,
    )
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--manifest-path")
    parser.add_argument("--manifest-root", default=os.getenv("RUN_MANIFEST_ROOT", "/workspace/artifacts/run-manifests"))
    return parser.parse_args()


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("landing_to_raw_batch").getOrCreate()


def discover_contract_paths(root: str, explicit_contracts: list[str]) -> list[str]:
    if explicit_contracts:
        return explicit_contracts
    return [str(path) for path in sorted(Path(root).glob("*/*.schema.yaml"))]


def finalize_manifest(manifest: RunManifest, manifest_path: str | None, manifest_root: str) -> None:
    manifest.metrics["loaded_contracts"] = sum(1 for output in manifest.outputs if output["status"] == "loaded")
    manifest.metrics["skipped_contracts"] = sum(1 for output in manifest.outputs if output["status"] == "skipped")
    manifest.metrics["total_rows"] = sum(int(output.get("row_count", 0)) for output in manifest.outputs)
    manifest.metrics["failed_contracts"] = len(manifest.failures)
    manifest.finish()
    written_path = manifest.write(resolve_manifest_path(manifest, manifest_path, manifest_root))
    print(f"manifest: {written_path}")


def main() -> None:
    args = parse_args()
    manifest = RunManifest(
        run_type="raw_batch",
        run_date=args.run_date,
        inputs={
            "contracts_root": args.contracts_root,
            "tables_root": args.tables_root,
            "contracts": args.contract,
            "sources": args.source,
            "endpoints": args.endpoint,
            "write_mode_override": args.write_mode,
        },
    )
    try:
        contract_paths = select_contract_paths(
            discover_contract_paths(args.contracts_root, args.contract),
            args.source,
            args.endpoint,
        )
    except ValueError as exc:
        manifest.add_failure(stage="selection", error=str(exc))
        finalize_manifest(manifest, args.manifest_path, args.manifest_root)
        raise SystemExit(f"error: {exc}") from None
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    loaded = 0
    skipped = 0
    failures: list[tuple[str, str]] = []
    table_configs = discover_table_configs(args.tables_root)
    try:
        for contract_path in contract_paths:
            contract = load_contract(contract_path)
            try:
                row_count = load_contract_to_raw(
                    spark=spark,
                    contract_path=contract_path,
                    run_date=args.run_date,
                    skip_missing_landing=args.skip_missing_landing,
                    allow_empty=args.allow_empty,
                    write_mode=args.write_mode,
                    create_missing_as_empty=args.create_missing_as_empty,
                    table_config_path=table_configs.get(get_target_table(contract)),
                )
                if row_count is None:
                    skipped += 1
                    manifest.add_output(
                        contract=contract_path,
                        target_table=get_target_table(contract),
                        status="skipped",
                    )
                else:
                    loaded += 1
                    manifest.add_output(
                        contract=contract_path,
                        target_table=get_target_table(contract),
                        status="loaded",
                        row_count=row_count,
                    )
            except Exception as exc:
                failures.append((contract_path, str(exc)))
                manifest.add_failure(contract=contract_path, error=str(exc))
                print(f"failed: {contract_path}: {exc}")
                if not args.continue_on_error:
                    finalize_manifest(manifest, args.manifest_path, args.manifest_root)
                    raise
    finally:
        spark.stop()

    finalize_manifest(manifest, args.manifest_path, args.manifest_root)
    print(f"completed: loaded={loaded}, skipped={skipped}, failed={len(failures)}")
    if failures:
        for contract_path, error in failures:
            print(f"failure: {contract_path}: {error}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
