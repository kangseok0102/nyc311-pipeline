"""Load Parquet partitions into the DuckDB raw table.

Requirements
- Loading the same date multiple times must give the same result (idempotent),
  so re-runs, backfills and Airflow retries are safe.
- New columns appearing in the source must not break the load (raw is all VARCHAR).
- Every load is recorded in raw._load_log, later used for dbt source freshness / monitoring.

Usage
    python -m nyc311.load                                     # yesterday, updated window
    python -m nyc311.load --date 2026-10-18 --window created
    python -m nyc311.load --start 2026-09-01 --end 2026-09-30 --window created
    python -m nyc311.load --all                               # reload every partition under data/raw

Tests: tests/test_pipeline.py  →  pytest -q
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from nyc311 import config
from nyc311.extract import WINDOW_FIELDS, daterange, partition_path, yesterday_nyc

log = logging.getLogger(__name__)

RAW = f"{config.RAW_SCHEMA}.{config.RAW_TABLE}"
LOAD_LOG = f"{config.RAW_SCHEMA}.{config.LOAD_LOG_TABLE}"


def connect(db_path: Path = config.DUCKDB_PATH):
    import duckdb

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def _sql_str(s: str) -> str:
    """Quote a string as a SQL literal (escapes single quotes). Use it to pass paths to read_parquet()."""
    return "'" + s.replace("'", "''") + "'"


def ensure_tables(con) -> None:
    """Create the raw schema, the raw table and the load-log table if they do not exist.

    Things to consider
    - You don't need to know the source columns up front. Start with the metadata columns only:
        from the file:   _window, _partition_date, _extracted_at
        added at load:   _source_file, _loaded_at
    - What does the log table need so that freshness can be judged?
      (Also check how many log rows the tests expect.)
    - Must be safe to call any number of times.
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {config.RAW_SCHEMA}")
    con.execute(f"CREATE TABLE IF NOT EXISTS {RAW} (_window TEXT NOT NULL, _partition_date TEXT NOT NULL, _extracted_at TEXT NOT NULL, _source_file TEXT NOT NULL, _loaded_at TIMESTAMP WITH TIME ZONE NOT NULL)")
    con.execute(f"CREATE TABLE IF NOT EXISTS {LOAD_LOG} (_window TEXT NOT NULL, _partition_date TEXT NOT NULL, _row_count BIGINT NOT NULL, _source_file TEXT NOT NULL, _loaded_at TIMESTAMP WITH TIME ZONE NOT NULL)")


def evolve_schema(con, parquet_file: Path) -> list[str]:
    """Add columns that exist in the file but not in the table (as VARCHAR); return the added names.

    Hints
    - The DuckDB command that lists the columns of a table or query: DESCRIBE
    - Column names may contain special characters, so wrap them in double quotes.
    """
    file_columns = con.execute(f"DESCRIBE SELECT * FROM read_parquet({_sql_str(str(parquet_file))})").fetchall()
    table_columns = con.execute(f"DESCRIBE {RAW}").fetchall()

    added_columns = []
    
    for col in file_columns:
        name = col[0]
        names = set()
        names.update(c[0] for c in table_columns)
        
        if name not in names:
            added_columns.append(name)
    
    for col in added_columns:
        con.execute(f'ALTER TABLE {RAW} ADD COLUMN "{col}" VARCHAR')

    log.info(f"Added columns to {RAW}: {', '.join(added_columns)}")

    return added_columns



def load_partition(con, window: str, day: date, root: Path = config.RAW_DIR) -> int:
    """Load one partition into the raw table; return that partition's row count after the load.

    Things to consider
    - If the file is missing, raise a clear error instead of silently skipping.
    - Which combination of SQL statements makes the load idempotent?
      If that combination fails halfway, what state is the table left in?  → transactions.
    - The column ORDER in the file and the table can differ. DuckDB has an INSERT syntax that
      matches columns by NAME rather than by position.
    - A row should be written to _load_log only when the load succeeds.
    """
    partition_file = partition_path(window, day, root)
    if not partition_file.exists():
        raise FileNotFoundError(f"Partition file not found: {partition_file}")

    ensure_tables(con)
    evolve_schema(con, partition_file)
    src_file = str(partition_file)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {RAW} WHERE _window = ? AND _partition_date = ?", (window, str(day)))
        con.execute(f"INSERT INTO {RAW} BY NAME SELECT *, ? AS _source_file, now() AS _loaded_at FROM read_parquet(?)", (src_file, src_file))
        row_count = con.execute(f"SELECT COUNT(*) FROM {RAW} WHERE _window = ? AND _partition_date = ?", (window, str(day))).fetchone()[0]
        con.execute(f"INSERT INTO {LOAD_LOG} (_window, _partition_date, _row_count, _source_file, _loaded_at) VALUES (?, ?, ?, ?, now())", (window, str(day), row_count, src_file))
        con.execute("COMMIT")
    
    except:
        con.execute("ROLLBACK")
        raise 

    return row_count



def discover_partitions(root: Path = config.RAW_DIR) -> list[tuple[str, date]]:
    """Find every partition file under root and return them as (window, date) pairs. (for --all)"""
    found = []

    for file in root.glob("*.parquet"):
        # Extract window and date from the file path
        window = file.parent.name
        date = date.fromisoformat(file.stem)
        found.append((window, date))

    return found

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load NYC 311 Parquet partitions into DuckDB")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", type=date.fromisoformat, help="single date (default: yesterday in New York)")
    g.add_argument("--start", type=date.fromisoformat, help="backfill start date (inclusive)")
    g.add_argument("--all", action="store_true", help="reload every partition under data/raw")
    p.add_argument("--end", type=date.fromisoformat, help="backfill end date (inclusive)")
    p.add_argument("--window", choices=sorted(WINDOW_FIELDS), default="updated")
    args = p.parse_args(argv)
    if bool(args.start) != bool(args.end):
        p.error("--start and --end must be used together")
    return args


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Decide the target (window, date) list and call load_partition for each.

    Make sure the connection is closed even if an exception is raised.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    if args.all:
        targets = discover_partitions()
    elif args.start and args.end:
        targets = [(args.window, d) for d in daterange(args.start, args.end)]
    else:
        targets = [(args.window, args.date or yesterday_nyc())]

    con = connect()
    try:
        total = 0
        for window, day in targets:
            log.info(f"Loading partition: window={window}, date={day}")
            count = load_partition(con, window, day)
            log.info(f"Loaded {count} rows for partition: window={window}, date={day}")
            total += count
        log.info(f"Total rows loaded: {total}")
    finally:
        con.close() 

    return log, total

if __name__ == "__main__":
    main()
