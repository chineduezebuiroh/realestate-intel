# forecast/ Inventory (Phase C)

## Canonical Entrypoints (only these should be used)
- forecast/cli/backtest.py
- forecast/cli/live.py
- forecast/cli/select_xgb.py
- forecast/cli/model_select.py

## Model Families
### SARIMAX Univariate
- canonical runner: forecast/models/sarimax_univariate/backtest_runner.py
- legacy shim: forecast/backtest_sarimax_single.py
- model code: forecast/sarimax_univariate.py

### SARIMAX Exog
- backtest runner: forecast/backtest_sarimax_exog_single.py
- runner/model: forecast/sarimax_exog.py
- bridge artifacts: forecast/consume_selected_features.py + forecast/artifacts.py

### XGB Forecast
- model code: forecast/xgb_regressor.py
- (runner may be embedded elsewhere; locate and list)

### XGB Selector
- forecast/backtest_xgb_single.py
- shortlist helpers: forecast/xgb_shortlist.py

## Features
- forecast/feature_loader.py
- forecast/design_matrix.py
- forecast/feature_catalog.py
- forecast/feature_policy.py
- forecast/feature_selection.py
- forecast/exog_forecast.py

## Artifacts
- forecast/artifacts.py
- forecast/consume_selected_features.py

## Persistence
- forecast/db_forecast.py

## Time / as-of
- forecast/asof.py
- forecast/asof_policy.py
- forecast/backtest_utils.py

## Batch orchestration (legacy; will be replaced)
- forecast/run_backtest_batch.py
- forecast/run_sarimax_batch.py

## Target-specific wrappers (should be eliminated)
- forecast/sarimax_redfin.py

## Repair / maintenance (evaluate necessity)
- forecast/catalog_repair.py

## Contracts (new; Phase C)
- forecast/contracts/keys.py
- forecast/contracts/errors.py
- forecast/contracts/serialize.py
