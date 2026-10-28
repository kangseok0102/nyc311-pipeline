.PHONY: setup test extract load daily backfill shell

DATE ?=
WINDOW ?= updated

setup:            ## 가상환경 + 의존성 설치
	python -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"

test:
	pytest -q

extract:          ## make extract DATE=2026-10-18 WINDOW=created
	python -m nyc311.extract $(if $(DATE),--date $(DATE)) --window $(WINDOW)

load:
	python -m nyc311.load $(if $(DATE),--date $(DATE)) --window $(WINDOW)

daily: extract load   ## 어제 데이터 추출 + 적재

backfill:         ## make backfill START=2026-09-01 END=2026-09-30
	python -m nyc311.extract --start $(START) --end $(END) --window created
	python -m nyc311.load    --start $(START) --end $(END) --window created

shell:            ## DuckDB CLI 로 웨어하우스 열기
	duckdb data/warehouse/nyc311.duckdb
