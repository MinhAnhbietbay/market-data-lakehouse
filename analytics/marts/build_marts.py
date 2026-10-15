#!/usr/bin/env python3
import sys
import time
from pathlib import Path
import trino

TRINO_HOST = "localhost"
TRINO_PORT = 8185
TRINO_USER = "alpha"
TRINO_CATALOG = "lakehouse"
TARGET_SCHEMA = "mart"
SQL_DIR = Path(__file__).resolve().parent / "sql"


def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
    )


def ensure_mart_schema(cursor):
    # Schema mart location matches lakehouse.curated structure: s3://lakehouse/warehouse/mart.db
    cursor.execute(
        f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TARGET_SCHEMA} "
        f"WITH (location = 's3://lakehouse/warehouse/{TARGET_SCHEMA}.db')"
    )
    cursor.fetchall()


def build_mart_table(cursor, sql_file: Path):
    stem = sql_file.stem
    table_name = f"mart_{stem}"
    full_table = f"{TRINO_CATALOG}.{TARGET_SCHEMA}.{table_name}"
    sql_query = sql_file.read_text(encoding="utf-8").strip()

    ctas_query = f"CREATE OR REPLACE TABLE {full_table} AS\n{sql_query}"

    start_time = time.perf_counter()
    cursor.execute(ctas_query)
    cursor.fetchall()
    elapsed = time.perf_counter() - start_time

    cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
    row_count = cursor.fetchall()[0][0]

    print(f"{table_name}: {row_count} rows ({elapsed:.2f}s)")
    return table_name, row_count, elapsed


def main():
    if not SQL_DIR.exists():
        print(f"Error: SQL directory not found at {SQL_DIR}", file=sys.stderr)
        sys.exit(1)

    all_sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not all_sql_files:
        print(f"Error: No SQL files found in {SQL_DIR}", file=sys.stderr)
        sys.exit(1)

    # Filter target files if CLI arguments are provided
    if len(sys.argv) > 1:
        requested = [
            arg.removesuffix(".sql").removeprefix("mart_") for arg in sys.argv[1:]
        ]
        target_files = []
        for req in requested:
            matching = [f for f in all_sql_files if f.stem == req]
            if not matching:
                print(
                    f"Error: Mart '{req}' not found. Available: {[f.stem for f in all_sql_files]}",
                    file=sys.stderr,
                )
                sys.exit(1)
            target_files.extend(matching)
    else:
        target_files = all_sql_files

    conn = get_connection()
    cursor = conn.cursor()

    try:
        ensure_mart_schema(cursor)
        for sql_file in target_files:
            try:
                build_mart_table(cursor, sql_file)
            except Exception as e:
                print(
                    f"Error: build mart_{sql_file.stem} thất bại: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
