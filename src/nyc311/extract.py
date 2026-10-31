"""Extract NYC 311 service requests from the Socrata API into daily Parquet partitions.

Design principles
- One day (America/New_York local time) = one partition = one Parquet file.
  Re-running the same date overwrites the file (idempotent).
- No type conversion at the raw layer. All values are stored as strings;
  casting and cleaning are the responsibility of the dbt staging models.
- Two extraction windows are supported:
    * created : filters on `created_date`. Used for backfills
                (requests opened on that day).
    * updated : filters on the Socrata system field `:updated_at`. Used for daily runs
                (requests created OR modified on that day, so closures are captured).

Usage
    python -m nyc311.extract                                  # yesterday, updated window
    python -m nyc311.extract --date 2026-10-18 --window created
    python -m nyc311.extract --start 2026-09-01 --end 2026-09-30 --window created

Tests: tests/test_pipeline.py  →  pytest -q
"""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from nyc311 import config

log = logging.getLogger(__name__)

# window Name → SoQL Field
WINDOW_FIELDS = {
    "created": "created_date",
    "updated": ":updated_at",
}

# Metadata columns stored with every raw row (used for lineage and freshness checks)
META_COLUMNS = ["_window", "_partition_date", "_extracted_at"]


@dataclass(frozen=True)
class ExtractResult:
    partition_date: date
    window: str
    row_count: int
    path: Path


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------
def build_session(app_token: str | None = config.SOCRATA_APP_TOKEN) -> requests.Session:
    """Build a requests.Session that retries 429/5xx responses with exponential backoff.

    The Socrata app token, if given, is sent in the X-App-Token header.
    """
    session = requests.Session()
    retry = requests.adapters.Retry(
        total=6,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))
    session.headers["X-App-Token"] = app_token if app_token else ""
    
    return session



def build_params(window: str, day: date, limit: int, offset: int) -> dict[str, str]:
    """Build SoQL query parameters for one New York day, [day 00:00, day+1 00:00).

    - created_date is a New York local timestamp, so the day bounds are used as-is.
    - :updated_at is UTC, so the New York day bounds are converted to UTC. The end bound is
      built from the next calendar date (not start + 24h) so DST transitions are handled.
    - Results are ordered by :id so offset pagination is stable.
    """
    params = {}

    if window == "updated":
        start = datetime.combine(day, datetime.min.time(), tzinfo=config.SOURCE_TZ).astimezone(UTC)
        end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=config.SOURCE_TZ).astimezone(UTC)
        start_s = start.strftime("%Y-%m-%dT%H:%M:%S")
        end_s = end.strftime("%Y-%m-%dT%H:%M:%S")

    elif window == "created":
        start_s = day.isoformat() + "T00:00:00"
        end_s = (day + timedelta(days=1)).isoformat() + "T00:00:00"
        
    else:
        raise ValueError(f"Unknown window: {window}")

    where = f"{WINDOW_FIELDS[window]} >= '{start_s}' AND {WINDOW_FIELDS[window]} < '{end_s}'"

    params = {
        "$select": ":*, *",
        "$where": where,
        "$order": ":id",
        "$limit": str(limit),
        "$offset": str(offset),}

    return params


def fetch_day(
    session: requests.Session,
    day: date,
    window: str = "updated",
    page_size: int = config.PAGE_SIZE,
) -> Iterator[list[dict[str, Any]]]:
    """Yield one day of records page by page (each page is a list of dicts).

    Stops on an empty page or on a page shorter than page_size.
    HTTP errors are raised, not swallowed, so the orchestrator can see the failure.
    """
    url = f"https://{config.SOCRATA_DOMAIN}/resource/{config.DATASET_ID}.json"
    offset = 0 

    while True:
        
        params = build_params(window, day, page_size, offset)
        log.info("Fetching %s %s (offset=%d)", window, day.isoformat(), offset)
        response = session.get(url, params=params, timeout=config.REQUEST_TIMEOUT_S)
        response.raise_for_status()
        page = response.json()
        
        if not page:return
        yield page

        if len(page) < page_size:return
        offset += len(page)

        



# --------------------------------------------------------------------------
# Transform (raw = strings only)
# --------------------------------------------------------------------------
def normalize_record(rec: dict[str, Any]) -> dict[str, str | None]:
    """Convert every value of one record to a string (or None).

    - None stays None (never the string 'None').
    - Nested values (dict/list, e.g. location) become JSON strings with sorted keys.
    - System fields starting with ':' are renamed with a '_sys_' prefix (':id' -> '_sys_id').
    """
    result = {}

    for key, value in rec.items():
        if key.startswith(":"):
            col = "_sys_" + key[1:]
        else:
            col = key
        
        if value is None:
            result[col] = None
        elif isinstance(value, (dict, list)):
            result[col] = json.dumps(value, ensure_ascii=False, sort_keys=True)
        else:
            result[col] = str(value)
    
    return result


def to_rows(
    pages: Iterator[list[dict[str, Any]]], window: str, day: date, extracted_at: datetime
) -> list[dict[str, str | None]]:
    """Flatten all pages into one list of rows, normalizing each record and adding META_COLUMNS."""
    meta = {
        "_window": window,
        "_partition_date": day.isoformat(),
        "_extracted_at": extracted_at.isoformat(),
    }
    rows = []
    for page in pages:
        for rec in page:
            rows.append({**normalize_record(rec), **meta})
    return rows


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------
def partition_path(window: str, day: date, root: Path = config.RAW_DIR) -> Path:
    """Return the partition file path.

    Format: <root>/window=<window>/date=<YYYY-MM-DD>/data.parquet
    (Hive-style partitioning, readable as-is by DuckDB and BigQuery)
    """
    return root / f"window={window}" / f"date={day.isoformat()}" / "data.parquet"
    


def write_parquet(rows: list[dict[str, str | None]], path: Path) -> None:
    """Write rows to Parquet with an all-string schema.

    - Columns are the union of keys across all rows plus META_COLUMNS, so records with
      missing fields still fit and an empty day produces a schema-only file.
    - The file is written to a temp path and then atomically replaced, so a crash never
      leaves a half-written data.parquet.
    """
    col_set = set()
    
    for row in rows:
        for col in row.keys():
            col_set.add(col)

    col_set.update(META_COLUMNS)
    columns = sorted(col_set)

    schema = pa.schema([(col, pa.string()) for col in columns])
    table = pa.Table.from_pylist(rows, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".parquet.tmp")
    
    pq.write_table(table, tmp_path)
    
    tmp_path.replace(path)



def extract_day(
    day: date,
    window: str = "updated",
    session: requests.Session | None = None,
    root: Path = config.RAW_DIR,
) -> ExtractResult:
    """Run one day of extraction: fetch -> to_rows -> write_parquet -> ExtractResult.

    A new session is built if none is given. extracted_at is the current UTC time.
    """
    if session is None:
        session = build_session()

    extracted_at = datetime.now(tz=UTC)
    pages = fetch_day(session, day, window)
    rows = to_rows(pages, window, day, extracted_at)
    path = partition_path(window, day, root)
    write_parquet(rows, path)
    log.info("extracted %s %s → %s (%d rows)", window, day.isoformat(), path, len(rows))

    return ExtractResult(partition_date=day, window=window, row_count=len(rows), path=path)
    


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def yesterday_nyc() -> date:
    """Yesterday's date in New York time (even when the server runs in UTC)."""
    today_nyc = datetime.now(tz=config.SOURCE_TZ).date()
    return today_nyc - timedelta(days=1)

def daterange(start: date, end: date) -> list[date]:
    """Every date from start to end, inclusive. Empty if start is after end."""
    n_days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(n_days)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract NYC 311 service requests to Parquet")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", type=date.fromisoformat, help="single date (default: yesterday in New York)")
    g.add_argument("--start", type=date.fromisoformat, help="backfill start date (inclusive)")
    p.add_argument("--end", type=date.fromisoformat, help="backfill end date (inclusive)")
    p.add_argument("--window", choices=sorted(WINDOW_FIELDS), default="updated")
    args = p.parse_args(argv)
    if args.start and not args.end:
        p.error("--start requires --end")
    if args.end and not args.start:
        p.error("--end requires --start")
    return args


def main(argv: list[str] | None = None) -> list[ExtractResult]:
    """CLI entry point: resolve the target dates and run extract_day for each.

    --start/--end -> date range, --date -> that day, neither -> yesterday in New York.
    One session is created and reused across all dates.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    session = build_session()
    results = [extract_day(day, args.window, session) for day in (daterange(args.start, args.end) if args.start else [args.date or yesterday_nyc()])]
    sum_rows = sum(r.row_count for r in results)
    log.info("Extracted %d rows in %d partitions", sum_rows, len(results))
    
    return results

if __name__ == "__main__":
    main()