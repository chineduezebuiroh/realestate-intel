# Native Supply Feature Policy Production Decision

**Decision identity:** `supply_native_feature_policy_2026_08_17`
**Human decision:** `supply_native_feature_policy_approved`
**Feature-policy calibration:** closed
**Supply metric-weight calibration:** pending (S0–S7)

Production now uses Active Inventory `MA12/I4` (40/15/45), Permit Activity `MA12/A2` (75/10/15), and Permit Intensity `MA12/N4` (40/15/45). All three Level and Long definitions remain MA12 level and MA12 relative to lag 12. Active Inventory and Permit Intensity Short remain MA12 relative to lag 3. Permit Activity Short remains its governed MA12-relative-to-lag-6 definition; this promotion changes its Level and Long weights only.

The bounded evidence trail is Phase 1 Feature Anatomy; Phase 1 repairs for native versus aligned dates, canonical source identity, raw calendar-month alignment, aligned Metric → Dimension contributions, cancellation semantics, and raw-feature SVGs; Phase 2 Feature-Weight Calibration; Phase 2 correlation hardening; and final MA9/MA12 checks for I4 and N4. ADR-010 records the human rationale and final MA conclusions.

BPS-FINAL-80 is preserved as superseded history. The 2026-08-06 Supply metric-weight promotion is not superseded: 60/20/20 remains production. There is no metric-weight, axis-weight, normalization, Demand, Price, Affordability, Labor, or Capital Markets change. Consequently, native Supply **feature** calibration is closed but the Supply dimension is not fully frozen. The S0–S7 metric-weight decision is still pending, and Capital Markets remains unchanged and deferred until that work is complete.

## Immutable production candidate

Generate the new run without `--smoothing-experiment-id` and do not commit it:

```bash
PYTHONPATH=. python -u scripts/run_regime_pipeline.py \
  --run-id supply_feature_policy_production_20260817 \
  --experiment-id supply_feature_policy_production \
  --artifact-root artifacts/regime/runs \
  --serving-db data/market_serving.duckdb \
  --metadata-json '{"promotion_contract":"supply_native_feature_policy_2026_08_17","active_inventory":"MA12/I4","permit_activity":"MA12/A2","permit_intensity":"MA12/N4","supply_metric_weights":{"active_inventory":0.60,"permit_activity":0.20,"permit_intensity":0.20},"metric_weight_calibration":"pending (S0-S7)","human_decision":"supply_native_feature_policy_approved","automated_winner":false}'
```
