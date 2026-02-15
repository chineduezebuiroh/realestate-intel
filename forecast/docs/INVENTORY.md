# forecast/INVENTORY.md (Phase C)

Purpose: a living map of canonical entrypoints + responsibilities.  
Rule: if it's not listed as canonical, assume it's legacy / shim / internal.

Rule: root-level forecast/*.py is either shared utilities or shims; authoritative
model logic lives under forecast/models/**.

---
## Phase C re-org objectives (NON-NEGOTIABLE)

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

---
## Phase C Execution Invariants (ENFORCED)

The following rules are hard stops. Violations must raise errors, not warnings.

- All backtests and live runs require:
  - explicit `BATCH_ID`
  - explicit `data_asof`
- `data_asof` must be ≤ anchor date (or train_end for live)
- All anchors and train_end timestamps must be month-end
- Artifact paths must be unique per:
  - model_name
  - run_kind
  - anchor or train_end
  - batch_id
- Runners must emit BOTH:
  - on-disk artifacts
  - DuckDB rows (`forecast_runs`)
- Anchor sets must be identical across model families for comparative evals
- Any violation must raise immediately and abort the run

---
## Canonical runner call graphs (MUST STAY TRUE)

If behavior changes, update this section first.

### XGB Selector (artifact-only)
- `forecast/models/xgb/backtest_selector_runner.py`
  - loads target series
  - selects anchors (via forecast/core/anchors.py)
  - builds universal candidate specs (governance)
  - scores candidates + selects base series (diversity caps)
  - builds design matrix incrementally (DQ gates)
  - trains XGB selector
  - enforces final-K composition (min_non_redfin, tier caps)
  - writes:
    - `runs/<batch_id>/xgb/selected_features__anchor=YYYY-MM-DD.parquet`
    - `runs/<batch_id>/xgb/selector_summary__anchor=YYYY-MM-DD.json`
    - `runs/<batch_id>/xgb/candidate_scores__anchor=YYYY-MM-DD.parquet`

### XGB Selector Evaluation (batch, multi-anchor)
- `forecast/cli/eval_xgb_selector.py`
  - enumerates targets × anchors
  - runs selector per anchor
  - emits manifest:
    - `manifest__xgb_selector.jsonl`
  - records per-run success/failure + artifact paths

### SARIMAX Exog Backtest
- `forecast/models/sarimax_exog/backtest_runner.py`
  - loads anchors
  - loads selector shortlist artifact for anchor
  - parses feature_ids → FeatureSpecs
  - builds train + future exog
  - fits SARIMAX(exog)
  - writes run artifacts + DB rows

### SARIMAX Exog Live
- `forecast/models/sarimax_exog/live_runner.py`
  - loads latest target series to data_asof
  - loads latest selector artifact (or scheduled selector batch)
  - builds train + future exog
  - fits SARIMAX(exog)
  - writes forecast artifacts + DB predictions

### SARIMAX Univariate
- `forecast/models/sarimax_univariate/backtest_runner.py`
- `forecast/models/sarimax_univariate/live_runner.py`

### XGB Forecast
- `forecast/models/xgb/backtest_forecast_runner.py`

---
## Canonical CLIs (stable entrypoints)

CLIs are thin; all behavior must live in runners.

### Selector
- `forecast/cli/backtest_xgb_selector.py`
- `forecast/cli/eval_xgb_selector.py`

### Forecast backtests / live
- `forecast/cli/backtest.py`
- `forecast/cli/live.py`
- `forecast/cli/backtest_xgb_forecast.py`
- `forecast/cli/live_sarimax_exog.py`

### Legacy / renamed (QUARANTINED)
- `forecast/cli/eval_forecast_runs.py`  
  → renamed from `eval.py`; legacy forecast scoring only

---
## Model Families (LOCKED FOR BAKEOFF)

**These three models are the permanent bakeoff set:**
1. SARIMAX Univariate
2. XGB Forecast
3. SARIMAX Exog

No additional models enter evaluation without an explicit Phase-C decision.

### SARIMAX Univariate
- runners:
  - `forecast/models/sarimax_univariate/backtest_runner.py`
  - `forecast/models/sarimax_univariate/live_runner.py`
- core:
  - `forecast/models/sarimax_univariate/fit.py`
  - `forecast/models/sarimax_univariate/predict.py`
- legacy:
  - `forecast/sarimax_univariate.py` (retire after parity)

### SARIMAX Exog
- core:
  - `forecast/models/sarimax_exog/core.py`
- runners:
  - `forecast/models/sarimax_exog/bridge_runner.py`
  - `forecast/models/sarimax_exog/backtest_runner.py`
  - `forecast/models/sarimax_exog/live_runner.py`
- exog future:
  - `forecast/models/sarimax_exog/exog_future.py`
- selector → X bridge:
  - `forecast/consume_selected_features.py`
- legacy:
  - `forecast/sarimax_exog.py`

### XGB Forecast
- runner:
  - `forecast/models/xgb/backtest_forecast_runner.py`
- model:
  - `forecast/models/xgb/forecaster.py`
- legacy:
  - `forecast/xgb_regressor.py`

---
## XGB Selector Governance (NON-EXPERIMENTAL DEFAULTS)

- `metric_pt_cap = 10`
- `min_non_redfin = 25`
- Redfin tier caps enforced
- Selector horizon = short (1–3 months)

Governance rules are documented in:
- `forecast/README_selector.md`

---
## Time / Anchors / As-Of (AUTHORITATIVE)

- `forecast/core/anchors.py`
  - `AnchorPolicy`
  - `choose_anchors(...)`
  - `month_end_index(...)`
- Legacy shims (do not extend):
  - `forecast/backtest_utils.py`

---
## Features / Design Matrix / Governance
- `forecast/feature_loader.py`
- `forecast/feature_policy.py`
- `forecast/feature_selection.py`
- `forecast/consume_selected_features.py`

---
## Artifacts
- `forecast/artifacts.py`
- selector outputs under:
  - `runs/<batch_id>/xgb/`

---
## Persistence
- `forecast/db_forecast.py`

---
## Contracts
- `forecast/contracts/keys.py`
- `forecast/contracts/errors.py`
- `forecast/contracts/serialize.py`

---
## Feature ID Contract (DO NOT BREAK)

Canonical format:
`<metric_id>__<geo_id>__<property_type_id>__<source_id>_lag<k>`

Downstream code parses feature_id strings directly.
Structured columns are a **Phase-C hardening TODO**, not yet implemented.

---
## Data Source Status (as of latest selector runs)

Refreshed:
- Redfin
- CES
- LAUS
- FRED macro
- Census NRC (starts, completions)
- BEA QGDP

Pending:
- Census BPS permits (provisional refresh)

Selector behavior is expected to shift once these land.
