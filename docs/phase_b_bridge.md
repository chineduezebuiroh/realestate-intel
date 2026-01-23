# Phase B Bridge: XGB Selector → Design Matrix Artifact → SARIMAX(exog)

This document defines the deterministic bridge workflow connecting XGB feature selection to SARIMAX(exog) forecasting via immutable artifacts.

## Overview
1. XGB selector produces ordered selected features
2. Consumer builds a deterministic design matrix artifact (+ audit JSON)
3. SARIMAX bridge runner consumes the artifact without rebuilding

This bridge validates plumbing and contracts. Model quality is explicitly out of scope.

## Step 1 — Run XGB selector
```bash
DUCKDB_PATH=data/market.duckdb python -m forecast.backtest_xgb_single \
  --purpose selector \
  --metric_id median_sale_price \
  --geo_id dc_city \
  --property_type_id 6 \
  --horizon 12 \
  --batch_id 20260122T_XGB_SELECTOR_MEDIAN_SFH_DQ_V02 \
  --data_asof 2025-11-30
```
Expected output:
- selected_features__anchor=2025-10-31.parquet

## Step 2 — Build design matrix artifact
```bash
DUCKDB_PATH=data/market.duckdb python -m forecast.consume_selected_features \
  --batch_id 20260122T_XGB_SELECTOR_MEDIAN_SFH_DQ_V02 \
  --anchor_date 2025-10-31 \
  --metric_id median_sale_price \
  --geo_id dc_city \
  --property_type_id 6 \
  --top_k 100 \
  --overwrite
```
Outputs:
- design_matrix__anchor=2025-10-31__asof=2025-11-30.parquet
- design_matrix__anchor=2025-10-31__asof=2025-11-30.json

## Step 3 — SARIMAX bridge run
```bash
DUCKDB_PATH=data/market.duckdb python -m forecast.sarimax_exog \
  --metric_id median_sale_price \
  --geo_id dc_city \
  --property_type_id 6 \
  --horizon 12 \
  --run_kind bridge \
  --label "Bridge artifact run" \
  --batch_id 20260122T_SARIMAX_EXOG_BRIDGE_V04 \
  --design_matrix_path runs/20260122T_XGB_SELECTOR_MEDIAN_SFH_DQ_V02/sarimax_exog/design_matrix__anchor=2025-10-31__asof=2025-11-30.parquet \
  --design_matrix_audit_path runs/20260122T_XGB_SELECTOR_MEDIAN_SFH_DQ_V02/sarimax_exog/design_matrix__anchor=2025-10-31__asof=2025-11-30.json
```

## Invariants
- feature_ids order preserved end-to-end
- design_matrix_sha256 matches audit
- data_asof_effective propagated to DB
- non-converged fits are allowed but flagged

## Intent
This bridge is a contract proof, not a production forecast path.
