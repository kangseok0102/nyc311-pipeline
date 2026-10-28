"""Parquet 파티션 → DuckDB raw 테이블 적재.

요구사항 (이 요구사항대로 구현하세요)
- 같은 날짜를 여러 번 적재해도 결과가 같아야 한다(멱등). 재실행·백필·Airflow 재시도 대비.
- 소스에 새 컬럼이 생겨도 적재가 깨지지 않아야 한다 (raw 는 전부 VARCHAR).
- 적재 이력을 raw._load_log 에 남긴다 → 이후 dbt source freshness / 모니터링에 사용.

사용 예
    python -m nyc311.load                                     # 어제, updated 윈도우
    python -m nyc311.load --date 2026-10-18 --window created
    python -m nyc311.load --start 2026-09-01 --end 2026-09-30 --window created
    python -m nyc311.load --all                               # data/raw 아래 전부 재적재

채점 기준: tests/test_pipeline.py  →  pytest -q
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from nyc311 import config
from nyc311.extract import WINDOW_FIELDS, daterange, partition_path, yesterday_nyc  # noqa: F401

log = logging.getLogger(__name__)

RAW = f"{config.RAW_SCHEMA}.{config.RAW_TABLE}"
LOAD_LOG = f"{config.RAW_SCHEMA}.{config.LOAD_LOG_TABLE}"


def connect(db_path: Path = config.DUCKDB_PATH):
    import duckdb

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def _sql_str(s: str) -> str:
    """SQL 문자열 리터럴 (작은따옴표 이스케이프). read_parquet() 에 경로를 넣을 때 사용."""
    return "'" + s.replace("'", "''") + "'"


def ensure_tables(con) -> None:
    """raw 스키마, raw 테이블, 적재 로그 테이블을 (없으면) 만든다.

    생각해 볼 것
    - raw 테이블의 소스 컬럼 목록은 미리 알 필요가 없다. 처음엔 메타 컬럼만으로 시작해도 된다.
      메타 컬럼: _window, _partition_date, _extracted_at (파일에서 옴)
                 _source_file, _loaded_at (적재 시점에 붙임)
    - 로그 테이블엔 무엇이 있어야 freshness 를 판단할 수 있을까? (테스트가 기대하는 행 수도 확인)
    - 몇 번을 호출해도 안전해야 한다.
    """
    raise NotImplementedError


def evolve_schema(con, parquet_file: Path) -> list[str]:
    """파일에는 있는데 테이블에는 없는 컬럼을 VARCHAR 로 추가하고, 추가한 컬럼명 리스트를 돌려준다.

    힌트
    - DuckDB 에서 테이블/쿼리의 컬럼 목록을 보는 명령: DESCRIBE
    - 컬럼명에 특수문자가 있을 수 있으니 따옴표로 감싸자.
    """
    raise NotImplementedError


def load_partition(con, window: str, day: date, root: Path = config.RAW_DIR) -> int:
    """파티션 하나를 raw 테이블에 적재하고, 적재 후 그 파티션의 행 수를 돌려준다.

    생각해 볼 것
    - 파일이 없으면 조용히 넘어가지 말고 분명한 에러를 낸다.
    - '멱등'을 어떤 SQL 조합으로 만들까? 그 조합이 중간에 실패하면 테이블은 어떤 상태가 될까?
      → 트랜잭션.
    - 파일 컬럼과 테이블 컬럼의 '순서'가 다를 수 있다. 위치가 아니라 이름으로 맞춰 넣는 INSERT 문법이
      DuckDB 에 있다.
    - 적재가 성공했을 때만 _load_log 에 기록이 남아야 한다.
    """
    raise NotImplementedError


def discover_partitions(root: Path = config.RAW_DIR) -> list[tuple[str, date]]:
    """root 아래 모든 파티션 파일을 찾아 (window, date) 목록으로 돌려준다. (--all 옵션용)"""
    raise NotImplementedError


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load NYC 311 Parquet partitions into DuckDB")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", type=date.fromisoformat)
    g.add_argument("--start", type=date.fromisoformat)
    g.add_argument("--all", action="store_true", help="data/raw 아래 모든 파티션 재적재")
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--window", choices=sorted(WINDOW_FIELDS), default="updated")
    args = p.parse_args(argv)
    if bool(args.start) != bool(args.end):
        p.error("--start 와 --end 는 함께 써야 합니다")
    return args


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점. 대상 (window, date) 목록을 정하고 load_partition 을 반복 호출한다.

    연결은 예외가 나도 반드시 닫히게 하자.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    raise NotImplementedError


if __name__ == "__main__":
    main()
