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

ingest_fred_rates:
	python ingest/fred_mortgage_rates.py

ingest_fred_yields:
	python ingest/fred_yields.py

ingest_redfin:
	python ingest/redfin_market_trends.py



transform_dc: db
	$(PY) transform/redfin_to_fact.py

transform_monthly: db
	$(PY) transform/monthlies_to_fact.py

transform_bls:
	python transform/laus_to_fact.py

transform_fred_rates:
	python transform/fred_mortgage_to_fact.py

transform_fred_yields:
	python transform/fred_yields_to_fact.py

transform_redfin:
	python transform/redfin_to_fact_v2.py

update_redfin_mirror:
	./tools/update_redfin_mirror.sh



forecast_dc:
	@echo "[forecast] (placeholder) next: SARIMAX/XGBoost + backtests"


dashboard:
	streamlit run app/streamlit_app.py


# --- Maintenance utilities ---
update_redfin_mirror:
	./tools/update_redfin_mirror.sh
