SHELL := /bin/bash
PY := python

.PHONY: setup db ingest_dc transform_dc forecast_dc ingest_monthly transform_monthly

setup:
	@mkdir -p data data/parquet

db:
	$(PY) utils/db.py --build



ingest_dc: setup
	# Redfin (weekly, may be skipped via env if blocked)
	$(PY) ingest/redfin.py

ingest_monthly: setup
	# Zillow ZORI + FRED Unemployment (monthly)
	$(PY) ingest/zillow_zori.py
	$(PY) ingest/fred_unemployment_dc.py

ingest_bls:
	python ingest/bls_laus_dc.py



transform_dc: db
	$(PY) transform/redfin_to_fact.py

transform_monthly: db
	$(PY) transform/monthlies_to_fact.py

transform_bls:
	python transform/laus_to_fact.py



forecast_dc:
	@echo "[forecast] (placeholder) next: SARIMAX/XGBoost + backtests"

dashboard:
	streamlit run app/streamlit_app.py
