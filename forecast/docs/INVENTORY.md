# forecast/INVENTORY.md (Phase C)

Purpose: a living map of canonical entrypoints + responsibilities.
Rule: if it's not listed as canonical, assume it's legacy / shim / internal.
- Rule: root-level forecast/*.py is either shared utilities or shims; model logic lives under forecast/models/**.

---
## Phase C re-org objectives (non-negotiable)

- **Single source of truth for runners:** authoritative execution logic lives only under `forecast/models/**`.
- **Legacy files are shims only:** legacy modules may import-and-call canonical runners, but must not contain unique logic.
- **No god files:** any file that mixes orchestration + model fitting + artifact IO + DB writes must be split.
- **Functional grouping:** shared logic must live in one of:
  - `forecast/features/` (governance, scoring, feature IDs)
  - `forecast/design_matrix/` (X build + audit)
  - `forecast/time/` (anchors/asof)
  - `forecast/artifacts/` (read/write contracts)
  - `forecast/persistence/` (DB)
  - `forecast/contracts/` (invariants)
- **Hard contracts tested:** contracts at artifact boundaries must have a smoke test.

## Canonical runner call graphs (must stay true during refactors)

Keep these lists updated whenever code moves. If behavior changes, update here first.

### XGB Selector
- `forecast/models/xgb/backtest_selector_runner.py`
  - loads target series (anchors)
  - builds universal candidate specs (governance)
  - scores candidates + selects base series (diversity caps)
  - builds design matrix incrementally (DQ gates)
  - trains XGB selector
  - writes shortlist artifact: `runs/<batch_id>/xgb/selected_features__anchor=YYYY-MM-DD.parquet`

### SARIMAX Exog Backtest
- `forecast/models/sarimax_exog/backtest_runner.py`
  - loads anchors
  - loads XGB shortlist artifact for anchor
  - parses feature_ids -> FeatureSpecs
  - builds train + future exog (forecasted exogs)
  - fits SARIMAX(exog) and scores
  - writes run artifacts + DB rows (as applicable)

### SARIMAX Exog Live
- `forecast/models/sarimax_exog/live_runner.py`
  - loads latest target series to data_asof
  - loads latest selector artifact (or scheduled selector batch)
  - builds train + future exog (forecasted exogs)
  - fits SARIMAX(exog) and emits forecast + exog_future artifact
  - writes DB predictions (as applicable)

### SARIMAX Univariate
- `forecast/models/sarimax_univariate/backtest_runner.py`
- `forecast/models/sarimax_univariate/live_runner.py`

### XGB Forecast
- `forecast/models/xgb/backtest_forecast_runner.py`



## Canonical CLIs (use these; everything else is implementation detail)

### Core orchestration (stable entrypoints; some wrap model runners)
- forecast/cli/backtest.py
- forecast/cli/live.py
- forecast/cli/select_xgb.py
- forecast/cli/model_select.py

Note: model-level runners under forecast/models/** are the authoritative execution logic.
CLIs should remain thin and must not introduce behavior not present in runners.


### Model-specific CLIs
- forecast/cli/backtest_xgb_forecast.py   → XGB forecast backtest (writes DB predictions)
- forecast/cli/backtest_xgb_selector.py   → XGB selector backtest (artifact-only; no DB preds)
- forecast/cli/live_sarimax_exog.py       → SARIMAX(exog) live run (artifact + exog_future; writes DB)

### Legacy shims (do not extend; keep for compatibility)
- forecast/backtest_xgb_single.py         → shim → forecast/models/xgb/backtest_selector_runner.py
- forecast/backtest_sarimax_single.py     → shim → forecast/models/sarimax_univariate/backtest_runner.py

---

## Model Families

### SARIMAX Univariate
- canonical runners:
  - forecast/models/sarimax_univariate/backtest_runner.py
  - forecast/models/sarimax_univariate/live_runner.py
- core model pieces:
  - forecast/models/sarimax_univariate/fit.py
  - forecast/models/sarimax_univariate/predict.py
- legacy module (evaluate for retirement once stable):
  - forecast/sarimax_univariate.py

### SARIMAX Exog (Phase B bridge → Phase C split)
This family has two evaluation modes:
- BRIDGE (perfect-X / artifact-only upper bound)
- LIVE (exog forecast policy; deploy-faithful)

- core:
  - forecast/models/sarimax_exog/core.py            → SarimaxExogSpec, fit, forecast
- bridge runner (artifact-driven; zero feature rebuild; zero exog forecasting):
  - forecast/models/sarimax_exog/bridge_runner.py
- live runner (artifact-driven design_matrix + deterministic exog_future):
  - forecast/models/sarimax_exog/live_runner.py
- future-exog generation + artifact:
  - forecast/models/sarimax_exog/exog_future.py    → seasonal-naive exog forecasting + audit/sha
- selector → design-matrix bridge artifact builder:
  - forecast/consume_selected_features.py          → emits design_matrix parquet + audit w/ ordered feature_ids

- legacy modules (keep until fully replaced):
  - forecast/sarimax_exog.py
  - forecast/backtest_sarimax_exog_single.py

### XGB Forecast
- canonical runner:
  - forecast/models/xgb/backtest_forecast_runner.py
- model code:
  - forecast/models/xgb/forecaster.py              (was forecast/xgb_regressor.py)
- legacy module (evaluate for retirement once stable):
  - forecast/xgb_regressor.py
 
### XGB Selector (authoritative feature-identity generator for SARIMAX-exog)
- canonical runner:
  - forecast/models/xgb/backtest_selector_runner.py

- enforced governance (NON-EXPERIMENTAL DEFAULTS):
  - metric_pt_cap = 10        # max per (metric_id, property_type_id)
  - min_non_redfin = 25       # minimum non-Redfin features in top-K
  - redfin_tier_caps = ON     # tiered Redfin share caps enforced

- helpers / policy modules:
  - forecast/feature_selection.py
  - forecast/feature_policy.py
  - forecast/xgb_shortlist.py

Rule: changes to selector governance MUST be versioned and documented.

Governance rules enforced by the selector (e.g. metric concentration caps,
source diversity minimums, Redfin tier caps, selector horizon semantics) are
documented in:

- forecast/README_selector.md

If behavior appears surprising, consult README_selector.md first before
changing code.

---

## Features / Design Matrix / Governance
- forecast/feature_loader.py
- forecast/design_matrix.py
- forecast/feature_catalog.py
- forecast/feature_policy.py
- forecast/feature_selection.py
- forecast/exog_forecast.py
- forecast/consume_selected_features.py   # selector → design_matrix identity bridge (ORDERED, HASHED)

---

## Package layout (Phase C consolidation)

- forecast/models/**      → authoritative model runners + core model code
- forecast/cli/**         → thin argument parsing + dispatch
- forecast/features/**    → feature discovery, governance, selection, design-matrix builders
- forecast/core/**        → time/as-of/anchors + artifacts + persistence helpers (temporary consolidation bucket)

NOTE: Root-level modules like forecast/feature_loader.py are compatibility forwarders.
Prefer importing from forecast/features/* or forecast/core/* for new code.

---

## Artifacts (read/write contracts)
- forecast/artifacts.py
- forecast/consume_selected_features.py
- forecast/models/sarimax_exog/exog_future.py

Artifacts emitted under:
runs/<batch_id>/{xgb,sarimax_exog}/...

---

## Persistence
- forecast/db_forecast.py

---

## Time / as-of / Anchors
- forecast/asof.py
- forecast/asof_policy.py
- forecast/backtest_utils.py

---

## Contracts (new; Phase C)
- forecast/contracts/keys.py
- forecast/contracts/errors.py
- forecast/contracts/serialize.py

---

## Batch orchestration (legacy; will be replaced)
- forecast/run_backtest_batch.py
- forecast/run_sarimax_batch.py

---

## Target-specific wrappers (should be eliminated)
- deleted
---

## Repair / maintenance (evaluate necessity)
- forecast/catalog_repair.py

## Refactor plan: migrations + retirement list (tracked)

### Folder migrations (target end-state)
- `forecast/features/` → feature discovery/governance/scoring/IDs
- `forecast/design_matrix/` → design matrix build + audit/hashing
- `forecast/time/` → anchors/asof/date utils
- `forecast/artifacts/` → artifact paths + schema contracts
- `forecast/persistence/` → DuckDB writes/reads for forecasts + runs

### Retirement candidates (delete only after parity is proven)
- `forecast/sarimax_univariate.py` (once `forecast/models/sarimax_univariate/*` is authoritative everywhere)
- `forecast/sarimax_exog.py` (once `forecast/models/sarimax_exog/*` fully replaces it)
- any `forecast/backtest_*_single.py` shims (keep only if actively used by CLI)

### Refactor guardrail
If a function exists in >1 place, pick one owner module and make the others import it. Do not allow forks.

---

## Feature ID contract (do not break casually)

Many diagnostics and diversity rules infer `source_id` (and sometimes lag) from the `feature_id` string format.

Current canonical format (examples):
- `<metric_id>__<geo_id>__<property_type_id>__<source_id>_lag<k>`
  - e.g. `median_sale_price__us_nation__6__redfin_lag1`
  - e.g. `laus_labor_force_nsa__dc_county__all__laus_lag3`

Downstream code DOES parse feature_id by splitting on "__" to infer source and lag.
This is a hard dependency today.

Phase C hardening TODO (NOT YET IMPLEMENTED):
- persist structured columns: metric_id, geo_id, property_type_id, source_id, lag
- eliminate reverse-parsing of feature_id strings

---

## Data Source Refresh Status (as of latest selector work)

Refreshed / current:
- Redfin
- CES
- LAUS
- FRED macro
- FRED unemployment
- Census
- Census BPS (permits - compiled)
- Census NRC Construction completions
- BEA QGDP (SQGDP9, LineCode 1) → source_id=bea_gdp_qtr

NOT YET refreshed (planned next):
- Census BPS (permits - provisional once data is available)



Selector behavior and feature diversity are expected to change materially after these land.


### Census – New Residential Construction (via FRED)

Source: census_nrc_fred
Metrics:
- census_housing_starts_total_saar
- census_housing_completions_total_saar

Geographies:
- us_nation
- us_region_northeast
- us_region_midwest
- us_region_south
- us_region_west

Notes:
- Region-level only (Census publishes starts/completions at national + region granularity)
- Intended as macro supply-cycle features
- Paired with local BPS permits for sub-state signal
- Freshness governed by global as_of policy; lag acceptable for exogenous use
