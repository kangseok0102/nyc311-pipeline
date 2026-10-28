"""추출/적재 단위 테스트. 실제 API 를 호출하지 않는다 (가짜 세션 사용).

    pytest -q
"""

from __future__ import annotations

from datetime import date

import pytest

from nyc311 import extract

DAY = date(2026, 10, 18)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSession:
    """offset 에 따라 미리 정한 페이지를 돌려주는 세션."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        idx = int(params["$offset"]) // int(params["$limit"])
        return FakeResponse(self.pages[idx] if idx < len(self.pages) else [])


def _rec(key, **extra):
    return {
        ":id": f"row-{key}",
        ":updated_at": "2026-10-18T15:00:00.000Z",
        "unique_key": str(key),
        "created_date": "2026-10-18T09:30:00.000",
        "agency": "NYPD",
        "location": {"type": "Point", "coordinates": [-73.9, 40.7]},
        **extra,
    }


# ---------------------------------------------------------------- extract
def test_created_window_uses_local_day_bounds():
    p = extract.build_params("created", DAY, limit=10, offset=0)
    assert p["$where"] == (
        "created_date >= '2026-10-18T00:00:00' AND created_date < '2026-10-19T00:00:00'"
    )
    assert p["$order"] == ":id"


def test_updated_window_converts_nyc_day_to_utc():
    p = extract.build_params("updated", DAY, limit=10, offset=0)
    # 10월 18일은 EDT(UTC-4)
    assert p["$where"] == (
        ":updated_at >= '2026-10-18T04:00:00' AND :updated_at < '2026-10-19T04:00:00'"
    )


def test_updated_window_handles_dst_end():
    # 2026-11-01 은 서머타임 종료일 → 하루가 25시간
    p = extract.build_params("updated", date(2026, 11, 1), limit=10, offset=0)
    assert p["$where"] == (
        ":updated_at >= '2026-11-01T04:00:00' AND :updated_at < '2026-11-02T05:00:00'"
    )


def test_pagination_stops_on_short_page():
    session = FakeSession([[_rec(1), _rec(2)], [_rec(3)]])
    pages = list(extract.fetch_day(session, DAY, "created", page_size=2))
    assert [len(p) for p in pages] == [2, 1]
    assert [c["$offset"] for c in session.calls] == ["0", "2"]


def test_pagination_exact_multiple_makes_one_extra_call():
    session = FakeSession([[_rec(1), _rec(2)]])
    pages = list(extract.fetch_day(session, DAY, "created", page_size=2))
    assert [len(p) for p in pages] == [2]
    assert len(session.calls) == 2  # 두 번째 호출이 빈 페이지 → 종료


def test_normalize_stringifies_and_renames_system_fields():
    row = extract.normalize_record(_rec(1, bbl=1012340001, closed_date=None))
    assert row["_sys_id"] == "row-1"
    assert row["_sys_updated_at"] == "2026-10-18T15:00:00.000Z"
    assert row["bbl"] == "1012340001"
    assert row["closed_date"] is None
    assert row["location"] == '{"coordinates": [-73.9, 40.7], "type": "Point"}'
    assert all(not k.startswith(":") for k in row)


def test_to_rows_adds_metadata():
    rows = extract.to_rows(iter([[_rec(1)]]), "created", DAY, extracted_at=extract.datetime(2026, 10, 19))
    assert rows[0]["_window"] == "created"
    assert rows[0]["_partition_date"] == "2026-10-18"


def test_cli_args_validation():
    with pytest.raises(SystemExit):
        extract.parse_args(["--start", "2026-10-01"])
    a = extract.parse_args(["--start", "2026-10-01", "--end", "2026-10-03", "--window", "created"])
    assert list(extract.daterange(a.start, a.end)) == [date(2026, 10, d) for d in (1, 2, 3)]


# ------------------------------------------------- parquet + duckdb (통합)
pa = pytest.importorskip("pyarrow")
duckdb = pytest.importorskip("duckdb")

from nyc311 import load  # noqa: E402


def _extract(tmp_path, pages, day=DAY):
    return extract.extract_day(day, "created", session=FakeSession(pages), root=tmp_path / "raw")


def test_extract_then_load_is_idempotent(tmp_path):
    _extract(tmp_path, [[_rec(1), _rec(2)]])
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))

    assert load.load_partition(con, "created", DAY, root=tmp_path / "raw") == 2
    assert load.load_partition(con, "created", DAY, root=tmp_path / "raw") == 2  # 재실행
    assert con.execute(f"SELECT count(*) FROM {load.RAW}").fetchone()[0] == 2
    assert con.execute(f"SELECT count(*) FROM {load.LOAD_LOG}").fetchone()[0] == 2


def test_empty_day_writes_file_and_loads_zero(tmp_path):
    r = _extract(tmp_path, [[]])
    assert r.row_count == 0 and r.path.exists()
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    assert load.load_partition(con, "created", DAY, root=tmp_path / "raw") == 0


def test_new_source_column_is_added(tmp_path):
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    _extract(tmp_path, [[_rec(1)]], day=DAY)
    load.load_partition(con, "created", DAY, root=tmp_path / "raw")

    next_day = date(2026, 10, 19)
    _extract(tmp_path, [[_rec(2, brand_new_field="x")]], day=next_day)
    load.load_partition(con, "created", next_day, root=tmp_path / "raw")

    vals = con.execute(f"SELECT unique_key, brand_new_field FROM {load.RAW} ORDER BY 1").fetchall()
    assert vals == [("1", None), ("2", "x")]
