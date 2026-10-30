"""NYC 311 Socrata API → 일자별 Parquet 추출.

설계 원칙 (이 원칙대로 구현하세요)
- 하루(뉴욕 현지 기준) = 파티션 1개 = Parquet 파일 1개. 같은 날짜를 다시 돌리면 덮어쓴다(멱등).
- raw 단계에서는 타입 변환을 하지 않는다. 모든 값은 문자열로 저장하고,
  타입 캐스팅/정제는 dbt staging 모델의 책임으로 둔다.
- 두 가지 추출 윈도우를 지원한다.
    * created : created_date 기준. 백필용(그날 접수된 민원).
    * updated : Socrata 시스템 필드 :updated_at 기준. 일일 운영용
                (그날 새로 생기거나 '상태가 바뀐' 민원 → 종료 처리 반영).

사용 예
    python -m nyc311.extract                                  # 어제, updated 윈도우
    python -m nyc311.extract --date 2026-10-18 --window created
    python -m nyc311.extract --start 2026-09-01 --end 2026-09-30 --window created

채점 기준: tests/test_pipeline.py  →  pytest -q
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, List
from datetime import timedelta

import requests

from nyc311 import config

log = logging.getLogger(__name__)

# window 이름 → SoQL 필드
WINDOW_FIELDS = {
    "created": "created_date",
    "updated": ":updated_at",
}

# 메타데이터 컬럼 (raw 에 함께 저장해 lineage/freshness 체크에 활용)
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
    """429/5xx 응답을 자동으로 재시도하는 requests.Session 을 만든다.

    생각해 볼 것
    - 공개 API 는 쓰로틀링(429)과 일시 장애(5xx)가 흔하다. 재시도 없이 짜면 Airflow 에서 자주 깨진다.
    - requests 자체엔 재시도가 없다. 세션에 붙일 수 있는 '어댑터'와 urllib3 의 재시도 정책을 찾아보자.
    - 재시도 간격은 고정보다 점점 늘어나는 편이 서버에 친절하다. (용어: exponential backoff)
    - 앱 토큰은 있으면 헤더로 보낸다. Socrata 문서에서 헤더 이름을 확인하자.
    """
    raise NotImplementedError


def build_params(window: str, day: date, limit: int, offset: int) -> dict[str, str]:
    """하루치 [day 00:00, day+1 00:00) 범위를 조회하는 SoQL 쿼리 파라미터를 만든다.

    생각해 볼 것
    - 필요한 키: $select, $where, $order, $limit, $offset
    - 범위는 '시작 포함, 끝 미포함'. BETWEEN 을 쓰면 왜 문제가 될까?
    - created_date 는 뉴욕 현지 시각(타임존 정보 없음), :updated_at 은 UTC.
      같은 '뉴욕의 하루'라도 두 필드에 넣을 문자열이 달라진다.
    - 서머타임 종료일은 하루가 24시간이 아니다. timedelta 를 어디에 더해야 안전할까?
    - 시스템 필드(:id, :updated_at)는 기본으로 응답에 안 온다. $select 로 함께 받는 법은?
    - 페이지를 넘길 때 결과 순서가 흔들리면 행이 누락/중복된다. 무엇으로 정렬해야 할까?
    """
    raise NotImplementedError


def fetch_day(
    session: requests.Session,
    day: date,
    window: str = "updated",
    page_size: int = config.PAGE_SIZE,
) -> Iterator[list[dict[str, Any]]]:
    """하루치 레코드를 페이지(list[dict]) 단위로 yield 하는 제너레이터.

    생각해 볼 것
    - 언제 멈춰야 하나? 빈 페이지일 때만? 마지막 페이지가 page_size 보다 짧을 때는?
    - 왜 리스트를 한 번에 return 하지 않고 yield 할까? (메모리, 진행 로그)
    - HTTP 에러는 삼키지 말고 드러내야 Airflow 가 실패를 안다.
    - 테스트의 FakeSession.get(url, params, timeout) 시그니처에 맞춰 호출하자.
    """
    raise NotImplementedError


# --------------------------------------------------------------------------
# 변환 (raw = 전부 문자열)
# --------------------------------------------------------------------------
def normalize_record(rec: dict[str, Any]) -> dict[str, str | None]:
    """레코드 1건의 값을 모두 문자열(또는 None)로 통일한다.

    규칙
    - None 은 None 그대로 (문자열 'None' 이 되면 안 된다)
    - dict/list 같은 중첩 값(location 등)은 JSON 문자열로. 키 순서가 항상 같게 하려면?
    - ':' 로 시작하는 시스템 필드는 '_sys_' 접두어로 이름을 바꾼다 (':id' → '_sys_id')
    """
    raise NotImplementedError


def to_rows(
    pages: Iterator[list[dict[str, Any]]], window: str, day: date, extracted_at: datetime
) -> list[dict[str, str | None]]:
    """모든 페이지의 레코드를 정규화하고 META_COLUMNS 값을 붙여 행 리스트로 만든다."""
    raise NotImplementedError


# --------------------------------------------------------------------------
# 저장
# --------------------------------------------------------------------------
def partition_path(window: str, day: date, root: Path = config.RAW_DIR) -> Path:
    """파티션 파일 경로를 돌려준다.

    형식: <root>/window=<window>/date=<YYYY-MM-DD>/data.parquet
    (Hive 스타일 파티션 → DuckDB/BigQuery 가 그대로 인식한다)
    """
    return root / f"window={window}" / f"date={day.isoformat()}" / "data.parquet"
    


def write_parquet(rows: list[dict[str, str | None]], path: Path) -> None:
    """행 리스트를 Parquet 로 쓴다.

    생각해 볼 것
    - 레코드마다 키 구성이 다를 수 있다. 전체 컬럼 집합은 어떻게 구할까?
    - 모든 컬럼을 string 타입으로 고정하려면 pyarrow 스키마를 명시해야 한다.
    - 데이터가 0건인 날도 '컬럼만 있는 빈 파일'을 남겨야 load 단계가 0건으로 기록할 수 있다.
    - 쓰다가 프로세스가 죽으면 반쯤 쓰인 파일이 남는다. 임시 파일에 쓰고 교체하는 방법은?
    - pyarrow import 는 함수 안에서 해도 된다 (순수 로직 테스트가 pyarrow 없이도 돌도록).
    """
    raise NotImplementedError


def extract_day(
    day: date,
    window: str = "updated",
    session: requests.Session | None = None,
    root: Path = config.RAW_DIR,
) -> ExtractResult:
    """하루치 추출 전체를 엮는다: fetch → to_rows → write_parquet → ExtractResult.

    session 이 주어지지 않으면 build_session() 으로 만든다. extracted_at 은 UTC 현재 시각.
    """
    raise NotImplementedError


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def yesterday_nyc() -> date:
    """뉴욕 시간 기준 '어제' 날짜. (서버가 UTC 여도 뉴욕 기준이어야 한다 → config.SOURCE_TZ)"""
    today_nyc = datetime.now(tz=config.SOURCE_TZ).date()
    return today_nyc - timedelta(days=1)

def daterange(start: date, end: date) -> list[date]:
    """start 부터 end 까지(양끝 포함) 하루씩 yield."""
    n_days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(n_days)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract NYC 311 service requests to Parquet")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", type=date.fromisoformat, help="단일 날짜 (기본: 뉴욕 기준 어제)")
    g.add_argument("--start", type=date.fromisoformat, help="백필 시작일 (포함)")
    p.add_argument("--end", type=date.fromisoformat, help="백필 종료일 (포함)")
    p.add_argument("--window", choices=sorted(WINDOW_FIELDS), default="updated")
    args = p.parse_args(argv)
    if args.start and not args.end:
        p.error("--start 를 쓰면 --end 도 필요합니다")
    if args.end and not args.start:
        p.error("--end 는 --start 와 함께 써야 합니다")
    return args


def main(argv: list[str] | None = None) -> list[ExtractResult]:
    """CLI 진입점. 날짜 목록을 정하고 extract_day 를 반복 호출한 뒤 결과를 로그로 남긴다.

    - --start/--end → daterange, --date → 그 날, 둘 다 없으면 yesterday_nyc()
    - 세션은 한 번만 만들어 재사용하자 (연결 재사용).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    raise NotImplementedError


if __name__ == "__main__":
    main()
