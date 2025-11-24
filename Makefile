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

# -----------------------------------------------------------------------------
# LEGACY / OLD PIPELINE TARGETS (OK to prune later if unused)
# -----------------------------------------------------------------------------


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



.PHONY: ingest_ces transform_ces bls_sync laus_gen

ingest_ces:
	$(PY) ingest/ces_expand_spec.py
	$(PY) ingest/ces_api_bulk.py

transform_ces:
	$(PY) transform/ces_to_fact.py

# one big BLS pipeline: LAUS + CES
bls_sync:
	$(PY) -c "from ingest.laus_expand_spec import ensure_bls_files; ensure_bls_files(); print('[make] BLS reference files synced.')"
	$(PY) ingest/laus_expand_spec.py
	$(PY) ingest/laus_api_bulk.py
	$(PY) transform/laus_to_fact.py
	$(PY) ingest/ces_expand_spec.py
	$(PY) ingest/ces_api_bulk.py
	$(PY) transform/ces_to_fact.py

# optional alias: keep laus_gen as a “full run” target
laus_gen: bls_sync



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




# -----------------------------------------------------------------------------
# NEW PIPELINE TARGETS (DuckDB-centric)
# -----------------------------------------------------------------------------
.PHONY: refresh-redfin refresh-ces refresh-laus refresh-census-acs refresh-census-permits \
        refresh-bea refresh-fred refresh-all make-public-db publish-public-db-only

# 🔁 Redfin: ingest + transform into fact_timeseries
refresh-redfin: venv
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/redfin_metro_to_timeseries.py
	DUCKDB_PATH=$(FULL_DB) $(PY) transform/redfin_to_fact.py
	@echo "✅ Refreshed Redfin → $(FULL_DB)"

# 🔁 CES: expand spec, bulk API fetch, transform
refresh-ces: venv
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/ces_expand_spec.py
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/ces_api_bulk.py
	DUCKDB_PATH=$(FULL_DB) $(PY) transform/ces_to_fact.py
	@echo "✅ Refreshed CES → $(FULL_DB)"

# 🔁 LAUS
refresh-laus: venv
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/laus_expand_spec.py
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/laus_api_bulk.py
	DUCKDB_PATH=$(FULL_DB) $(PY) transform/laus_to_fact.py
	@echo "✅ Refreshed LAUS → $(FULL_DB)"

# 🔁 Census ACS (adjust script names if different in your repo)
refresh-census-acs: venv
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/census_api_bulk.py
	DUCKDB_PATH=$(FULL_DB) $(PY) transform/census_to_fact.py
	@echo "✅ Refreshed Census ACS → $(FULL_DB)"

# 🔁 Census Building Permits (BPS) – adjust names if needed
refresh-census-permits: venv
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/census_building_permits.py
	DUCKDB_PATH=$(FULL_DB) $(PY) transform/census_bp_to_fact.py
	@echo "✅ Refreshed Census BPS → $(FULL_DB)"

# 🔁 BEA GDP (Quarterly) – adjust to your actual script names
refresh-bea: venv
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/bea_gdp_qtr_api.py
	@echo "✅ Refreshed BEA GDP → $(FULL_DB)"

# 🔁 FRED (macro + unemployment) – adjust to match actual scripts
refresh-fred: venv
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/fred_macro_api.py
	DUCKDB_PATH=$(FULL_DB) $(PY) ingest/fred_unemployment_api.py
	@echo "✅ Refreshed FRED → $(FULL_DB)"

# 🔁 Everything (if you want a single "full refresh" button)
refresh-all: refresh-redfin refresh-ces refresh-laus refresh-census-acs refresh-census-permits refresh-bea refresh-fred
	@echo "✅ All sources refreshed → $(FULL_DB)"

# -----------------------------------------------------------------------------
# Build public DuckDB snapshot (no git)
# -----------------------------------------------------------------------------
make-public-db: venv
	FULL_DUCKDB_PATH=$(FULL_DB) $(PY) scripts/make_public_db.py
	@echo "✅ Built data/market_public.duckdb from $(FULL_DB)"

# -----------------------------------------------------------------------------
# Publish-only:
#   - rebuild public DB
#   - enforce <100 MB
#   - git add/commit/push data/market_public.duckdb (if changed)
# -----------------------------------------------------------------------------
publish-public-db-only: venv
	@echo "🛠  Rebuilding public DB from $${DUCKDB_PATH:-data/market.duckdb}…"
	@$(PY) scripts/make_public_db.py

	@# --- size guard: fail if > 100MB ---
	@size_bytes=$$(stat -f%z data/market_public.duckdb); \
		max_bytes=$$((100 * 1024 * 1024)); \
		echo "📦 market_public.duckdb size: $$size_bytes bytes"; \
		if [ $$size_bytes -gt $$max_bytes ]; then \
			echo "❌ market_public.duckdb is too large (>100MB). Aborting."; \
			exit 1; \
		fi

	@# --- git steps ---
	@git status --short data/market_public.duckdb
	@git add data/market_public.duckdb
	@git commit -m "Update public DB snapshot" || echo "No changes to commit."
	@git push
