import argparse

from pyspark.sql import SparkSession

from pipelines.raw.contract import (
    contract_schema_ddl,
    get_source,
    get_source_endpoint,
    get_target_table,
    load_contract,
)
from pipelines.raw.iceberg_writer import table_exists, write_iceberg_table
from pipelines.raw.landing_reader import landing_path_exists, read_landing_envelopes
from pipelines.raw.mapper import build_raw_dataframe
from pipelines.raw.table_config import ensure_table_config_matches, get_write_mode, load_table_config
from pipelines.raw.validation import validate_contract, validate_raw_dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load landing API envelopes into raw Iceberg tables.")
    parser.add_argument("--contract", required=True)
    parser.add_argument("--table-config")
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
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("landing_to_raw")
        .getOrCreate()
    )


def load_contract_to_raw(
    spark: SparkSession,
    contract_path: str,
    run_date: str,
    skip_missing_landing: bool = False,
    allow_empty: bool = False,
    write_mode: str | None = None,
    table_config_path: str | None = None,
    create_missing_as_empty: bool = False,
) -> int | None:
    contract = load_contract(contract_path)
    validate_contract(contract)

    source = get_source(contract)
    endpoint = get_source_endpoint(contract)
    target_table = get_target_table(contract)
    table_config = load_table_config(table_config_path) if table_config_path else None
    if table_config:
        ensure_table_config_matches(table_config, target_table)
    resolved_write_mode = get_write_mode(table_config, override=write_mode)

    if not landing_path_exists(spark=spark, source=source, endpoint=endpoint, run_date=run_date):
        message = f"missing landing path for {source}.{endpoint} on {run_date}"
        # Bảng CHƯA tồn tại thì dựng rỗng từ schema contract: endpoint khoá theo gói (402) chỉ
        # thiếu dữ liệu, không thiếu schema. Bảng ĐÃ tồn tại thì tuyệt đối không đụng -- landing
        # thiếu một ngày không được phép xoá dữ liệu đã có, đó sẽ là mất mát im lặng.
        if create_missing_as_empty and not table_exists(spark, target_table):
            write_iceberg_table(
                df=spark.createDataFrame([], contract_schema_ddl(contract)),
                table_name=target_table,
                write_mode="replace-table",
                table_config=table_config,
            )
            print(f"created empty: {target_table} ({message})")
            return 0
        if skip_missing_landing:
            print(f"skipped: {message}")
            return None
        raise FileNotFoundError(message)

    landing_df = read_landing_envelopes(
        spark=spark,
        source=source,
        endpoint=endpoint,
        run_date=run_date,
    )
    raw_df = build_raw_dataframe(landing_df=landing_df, contract=contract)
    row_count = validate_raw_dataframe(
        df=raw_df,
        contract=contract,
        fail_on_empty=not allow_empty,
    )

    write_iceberg_table(
        df=raw_df,
        table_name=target_table,
        write_mode=resolved_write_mode,
        table_config=table_config,
    )

    print(f"source: {source}")
    print(f"endpoint: {endpoint}")
    print(f"target_table: {target_table}")
    print(f"table_config: {table_config_path or 'DEFAULT'}")
    print(f"write_mode: {resolved_write_mode}")
    print(f"rows: {row_count}")
    return row_count


def main() -> None:
    args = parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        load_contract_to_raw(
            spark=spark,
            contract_path=args.contract,
            run_date=args.run_date,
            skip_missing_landing=args.skip_missing_landing,
            allow_empty=args.allow_empty,
            write_mode=args.write_mode,
            create_missing_as_empty=args.create_missing_as_empty,
            table_config_path=args.table_config,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
