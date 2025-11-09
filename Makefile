SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip


.PHONY: venv bootstrap deps clean-venv

venv:
	python -m venv $(VENV)
	. $(VENV)/bin/activate; pip install -U pip wheel
	. $(VENV)/bin/activate; pip install -r requirements.txt

bootstrap: clean-venv venv

deps:
	$(PIP) install -r requirements.txt

clean-venv:
	rm -rf $(VENV)


.PHONY: setup db ingest_dc transform_dc forecast_dc ingest_monthly transform_monthly \
        ingest_bls ingest_fred_rates ingest_fred_yields ingest_redfin \
        transform_bls transform_fred_rates transform_fred_yields transform_redfin dashboard \
        update_redfin_mirror import_redfin_local_city import_redfin_local_county import_redfin_local_state

setup: venv
	@mkdir -p data data/parquet

db: venv
	$(PY) utils/db.py --build




ingest_dc: setup
	# Redfin (weekly, may be skipped via env if blocked)
	$(PY) ingest/redfin.py

ingest_redfin: setup
	$(PY) ingest/redfin_market_trends.py

transform_dc: db
	$(PY) transform/redfin_to_fact.py

transform_redfin: db
	$(PY) transform/redfin_to_fact_v2.py



ingest_monthly: setup
	# Zillow ZORI + FRED Unemployment (monthly)
	$(PY) ingest/zillow_zori.py
	$(PY) ingest/fred_unemployment_dc.py

transform_monthly: db
	$(PY) transform/monthlies_to_fact.py



.PHONY: bls_sync laus_gen

bls_sync:
	. .venv/bin/activate; python - <<'PY'
	from ingest.laus_expand_spec import ensure_bls_files
	ensure_bls_files()
	print("[make] BLS reference files synced.")
	PY

laus_gen: bls_sync
	. .venv/bin/activate; python ingest/laus_expand_spec.py
	. .venv/bin/activate; python ingest/laus_api_bulk.py
	. .venv/bin/activate; python transform/laus_to_fact.py



ingest_fred_rates: setup
	$(PY) ingest/fred_mortgage_rates.py

transform_fred_rates: db
	$(PY) transform/fred_mortgage_to_fact.py



ingest_fred_yields: setup
	$(PY) ingest/fred_yields.py

transform_fred_yields: db
	$(PY) transform/fred_yields_to_fact.py



forecast_dc: venv
	@echo "[forecast] (placeholder) next: SARIMAX/XGBoost + backtests"



dashboard: venv
	$(PY) -m streamlit run app/streamlit_app.py



# --- Maintenance utilities ---
update_redfin_mirror: venv
	./tools/update_redfin_mirror.sh



# --- Vendor data utilities ---
import_redfin_local_city: venv
	$(PY) tools/import_redfin_local.py --file "$(FILE)" --level city

import_redfin_local_county: venv
	$(PY) tools/import_redfin_local.py --file "$(FILE)" --level county

import_redfin_local_state: venv
	$(PY) tools/import_redfin_local.py --file "$(FILE)" --level state
