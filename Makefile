SHELL := /bin/bash
PY := python

.PHONY: setup db ingest_dc transform_dc forecast_dc

setup:
	@mkdir -p data data/parquet

db:
	$(PY) utils/db.py --build

ingest_dc: setup
	$(PY) ingest/redfin.py

transform_dc: db
	$(PY) transform/redfin_to_fact.py

forecast_dc:
	@echo "[forecast] (placeholder) train + write forecasts; Slack alerts later"
