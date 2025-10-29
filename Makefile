SHELL := /bin/bash
PY := python

.PHONY: setup db ingest_dc transform_dc forecast_dc

setup:
	@mkdir -p data data/parquet

db:
	$(PY) utils/db.py --build

ingest_dc: setup
	$(PY) ingest/redfin.py
	$(PY) ingest/fred.py

transform_dc: db
	@echo "[transform] (placeholder) map parquet -> fact_timeseries using DuckDB SQL"

forecast_dc:
	@echo "[forecast] (placeholder) train + write forecasts; send Slack later"
