"""프로젝트 전역 설정. 환경변수(.env)로 덮어쓸 수 있다."""

from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

try:  # .env 는 선택 사항
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# --- 소스 (NYC Open Data / Socrata) ---------------------------------------
# "311 Service Requests from 2020 to Present" (2010~2019 데이터는 76ig-c548)
SOCRATA_DOMAIN = os.getenv("SOCRATA_DOMAIN", "data.cityofnewyork.us")
DATASET_ID = os.getenv("NYC311_DATASET_ID", "erm2-nwe9")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")  # 없어도 동작하지만 쓰로틀링이 심함
PAGE_SIZE = int(os.getenv("NYC311_PAGE_SIZE", "50000"))
REQUEST_TIMEOUT_S = int(os.getenv("NYC311_TIMEOUT_S", "120"))

# 311 의 created_date 등은 '뉴욕 현지 시각' floating timestamp 이므로
# "전날"의 기준도 뉴욕 시간대로 잡는다.
SOURCE_TZ = ZoneInfo("America/New_York")

# --- 저장 위치 -------------------------------------------------------------
DATA_DIR = Path(os.getenv("NYC311_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw" / "nyc311"
DUCKDB_PATH = Path(os.getenv("NYC311_DUCKDB_PATH", DATA_DIR / "warehouse" / "nyc311.duckdb"))

RAW_SCHEMA = "raw"
RAW_TABLE = "nyc311_requests"
LOAD_LOG_TABLE = "_load_log"
