# Corrected 10Y-2Y Spread P6 Feature-Policy Promotion

**Contract:** `capital_markets_spread_10y_2y_p6_2026_08_18`  
**Human decision:** `spread_10y_2y_p6_approved`  
**Automated winner:** false

## Context

The original P7 selection was calibrated on inverted source chronology. The physical provider series is `fred_spread_2y_10y = treasury_2y - treasury_10y`; the repaired canonical contract remains `spread_10y_2y = treasury_10y - treasury_2y = -fred_spread_2y_10y`. After the defect was confirmed and repaired, targeted P0-P9 revalidation ran on corrected chronology.

## Decision and rationale

Promote P6 for `spread_10y_2y`: 60% Level, 5% Short, and 35% Long. Construction remains MA9 Level, MA9 arithmetic difference versus lag 3 for Short, and MA9 arithmetic difference versus lag 12 for Long, with positive normalization.

P6 supplied the strongest overall stability/responsiveness tradeoff: materially fewer reversals and whipsaws than Short-heavier policies, stronger persistence, strong recent material-move direction agreement, materially better corrected raw-cycle preservation than P7/P9/P8, preserved response magnitude, and a Level-led rather than Long-dominated contribution structure. The boundary test showed that 5% Short was earned rather than arbitrary.

The principal tradeoff is weaker turning-point preservation than P4. The governed turning-point sample was sparse, and that limited detector evidence did not override the broader stability and responsiveness evidence.

## Rejected finalists

- **P7** is `revalidation_failed`: it was excessively Long-dominated, showed materially weaker corrected raw-cycle correlation and material-move direction agreement, closely resembled the Long feature, and gained stability partly by over-damping. Its original promotion record remains historical evidence.
- **P9 and P8** remained overly Long-biased relative to their gained stability.
- **P4** was credible and better at the sparse turning-point sample, but P6 had the stronger overall stability/responsiveness balance.

## Consequences

Corrected-polarity feature revalidation is closed and all six Capital Markets native feature policies are valid. The other five policies, metric weights, normalization, and Demand/Supply axis weights do not change. Family metric-weight calibration remains `pending_rerun`; no family-weight production policy has been selected and no family calibration was run here.

## Fresh immutable production baseline

Authoritative artifacts are unavailable in the hosted environment, so no run is fabricated or committed. Materialize it locally:

```bash
PYTHONPATH=. python -u scripts/run_regime_pipeline.py \
  --run-id capital_markets_feature_policy_corrected_production_20260818 \
  --experiment-id capital_markets_feature_policy_corrected_production \
  --artifact-root artifacts/regime/runs \
  --serving-db data/market_serving.duckdb \
  --metadata-json '{"promotion_contract":"capital_markets_spread_10y_2y_p6_2026_08_18","mortgage_30y_feature_policy":"P4","mortgage_15y_feature_policy":"P2","treasury_10y_feature_policy":"P1","fedfunds_feature_policy":"P5","spread_10y_2y_feature_policy":"P6","spread_10y_fedfunds_feature_policy":"P9","corrected_spread_polarity":true,"capital_markets_metric_weights":{"mortgage_30y":0.15,"mortgage_15y":0.15,"treasury_10y":0.15,"fedfunds":0.10,"spread_10y_2y":0.225,"spread_10y_fedfunds":0.225},"family_metric_weight_calibration":"pending_rerun","human_decision":"spread_10y_2y_p6_approved","automated_winner":false}'
```

Validate it with:

```bash
PYTHONPATH=. python scripts/validate_capital_markets_corrected_feature_policy_run.py --artifact-root artifacts/regime/runs --serving-db data/market_serving.duckdb
```
