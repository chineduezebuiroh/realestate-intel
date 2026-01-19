# XGB “selector” runs (shortlist generator)

This repo uses **two different horizons** on purpose:

1) **Selector horizon (XGB backtest, purpose=selector)**
- Goal: rank candidate exogenous features (and their lags) for downstream models (esp. SARIMAX-exog).
- Horizon is intentionally short (typically **1–3 months**) because feature importance is more stable and less contaminated by long-horizon uncertainty.
- Selector runs are governed by `FeaturePolicy`:
  - `xgb_selector_horizon_months`
  - `xgb_selector_latest_anchor_offset_months`
  - `xgb_selector_anchor_step_months`
  - `xgb_selector_max_anchors`

Important backtestability rule:
- The “freshest” anchor must still have future y available to score.
- Therefore `latest_anchor_offset_months >= horizon`.
- Default behavior: `latest_anchor_offset_months = horizon` (freshest possible anchor that is still scoreable).

Selector outputs:
- Writes XGB artifacts under: `runs/<batch_id>/xgb/`
- These artifacts are used as the selector-of-record for SARIMAX-exog.

2) **Production forecast horizon (live models)**
- Goal: produce real predictions (e.g. 6, 12, 18 months).
- This horizon is used by SARIMAX-univariate, SARIMAX-exog, and any production XGB forecast runs.
- Production horizon should NOT be conflated with the selector horizon.

## Why the split exists
- Long-horizon “best features” often differ from short-horizon “best features”.
- We standardize the selector horizon so SARIMAX-exog always consumes a consistent shortlist signal.

## Freshness policy
SARIMAX-exog checks shortlist freshness vs live train_end and will fail if the shortlist anchor is too stale.
If you hit “shortlist is stale”, re-run the selector batch closer to `data_asof`.
