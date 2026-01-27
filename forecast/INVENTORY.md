# forecast/INVENTORY.md (Phase C)

Purpose: a living map of canonical entrypoints + responsibilities.
Rule: if it's not listed as canonical, assume it's legacy / shim / internal.

---

## Canonical CLIs (use these; everything else is implementation detail)

### Core orchestration
- forecast/cli/backtest.py
- forecast/cli/live.py
- forecast/cli/select_xgb.py
- forecast/cli/model_select.py

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

### XGB Selector (for SARIMAX-exog feature identity)
- canonical runner:
  - forecast/models/xgb/backtest_selector_runner.py
- helpers / governance:
  - forecast/xgb_shortlist.py
  - forecast/feature_selection.py
  - forecast/feature_policy.py

---

## Features / Design Matrix / Governance
- forecast/feature_loader.py
- forecast/design_matrix.py
- forecast/feature_catalog.py
- forecast/feature_policy.py
- forecast/feature_selection.py
- forecast/exog_forecast.py

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
- forecast/sarimax_redfin.py

---

## Repair / maintenance (evaluate necessity)
- forecast/catalog_repair.py

---

## Feature ID contract (do not break casually)

Many diagnostics and diversity rules infer `source_id` (and sometimes lag) from the `feature_id` string format.

Current canonical format (examples):
- `<metric_id>__<geo_id>__<property_type_id>__<source_id>_lag<k>`
  - e.g. `median_sale_price__us_nation__6__redfin_lag1`
  - e.g. `laus_labor_force_nsa__dc_county__all__laus_lag3`

Downstream code may parse feature_id by splitting on `"__"` and then reading the last token (e.g. `"redfin_lag1"`, `"ces_lag6"`) to infer source/lag. If we ever change this naming scheme, we MUST update:
- feature-id parsing helpers (e.g., `_source_from_feature_id()` or equivalents)
- any selection constraints keyed by inferred `source` (e.g., `min_non_redfin`)

Preferred future direction (if we harden this):
- persist structured columns alongside `feature_id`: `metric_id`, `geo_id`, `property_type_id`, `source_id`, `lag`
- use those columns instead of reverse-parsing strings
