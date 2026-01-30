# XGB Selector (Feature Identity + Shortlist Governance)

The XGB selector is responsible for **feature identity selection**, not forecasting.

It produces an **ordered, immutable shortlist of exogenous features**
used downstream by SARIMAX-exog (bridge and live).

---

## Two Horizons (Intentional and Non-Interchangeable)

This repo uses **two different horizons on purpose**.

### 1) Selector horizon (XGB backtest, purpose = feature selection)

- Goal: rank and govern candidate exogenous features (and their lags).
- Horizon is intentionally short (typically **1–3 months**).
- Reason:
  - Feature importance is more stable at short horizons.
  - Long-horizon uncertainty contaminates feature identity.
- Selector horizon is NOT a modeling choice; it is a governance choice.

Selector runs are governed by `FeaturePolicy`, including:
- `xgb_selector_horizon_months`
- `xgb_selector_latest_anchor_offset_months`
- `xgb_selector_anchor_step_months`
- `xgb_selector_max_anchors`

#### Backtestability rule (hard requirement)
- The freshest anchor must still have future `y` available for scoring.
- Therefore:  
  `latest_anchor_offset_months >= selector_horizon`
- Default behavior:
  - `latest_anchor_offset_months = selector_horizon`

---

### 2) Production forecast horizon (live models)

- Goal: generate real predictions (e.g. 6, 12, 18 months).
- Used by:
  - SARIMAX-univariate
  - SARIMAX-exog (bridge + live)
  - Production XGB forecast runs
- This horizon must NEVER be conflated with the selector horizon.

---

## Why the Split Exists

- Short-horizon signals are more reliable for feature ranking.
- Long-horizon accuracy depends more on model dynamics than feature identity.
- We standardize selector behavior so SARIMAX-exog always consumes a **stable, comparable feature set**.

---

## Selector Governance (Phase C Baseline Rules)

The selector enforces **non-experimental governance rules**.
These are defaults, not tuning knobs.

Current baseline:

- `metric_pt_cap = 10`  
  Max base series per `(metric_id, property_type_id)`.

- `min_non_redfin = 25`  
  Minimum number of non-Redfin features in final top-K.

- `redfin_tier_caps = ON`  
  Enforced tier share caps for Redfin metrics.

These rules exist to prevent:
- metric echoing across geographies
- single-source dominance
- fragile shortlists

Any change to these rules requires:
- explicit versioning
- selector re-runs
- updated documentation

---

## Selector Artifacts (Selector-of-Record)

Selector runs write artifacts to:
- runs/<batch_id>/xgb/

Key artifact:
- `selected_features__anchor=YYYY-MM-DD.parquet`

This artifact is:
- ordered
- hashed
- immutable
- the **sole source of feature identity** for SARIMAX-exog

Downstream models MUST NOT:
- rebuild features
- reorder columns
- substitute alternative shortlists

---

## Freshness Policy

SARIMAX-exog validates that the selector artifact is **fresh relative to `data_asof`**.

If the shortlist anchor is too stale:
- the run fails fast
- the selector must be re-run closer to `data_asof`

This is intentional.
Using stale feature identity invalidates backtests and live comparisons.
