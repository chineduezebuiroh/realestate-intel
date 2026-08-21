# Capital Markets Native Feature-Policy Production Decision

**Contract:** `capital_markets_native_feature_policy_2026_08_18`  
**Human decision:** `capital_markets_native_feature_policy_approved`  
**Automated winner:** false  
**Feature calibration:** closed  
**Family metric-weight calibration:** pending

The prior incumbent was 60/20/20 Level/Short/Long across all six metrics. Production now uses:

| Metric | Policy | Level | Short | Long | Construction and direction |
|---|---|---:|---:|---:|---|
| mortgage_30y | P4 | .55 | .10 | .35 | MA12 level; MA12 proportional lag-3/lag-12 change; negative |
| mortgage_15y | P2 | .60 | .10 | .30 | MA12 level; MA12 proportional lag-3/lag-12 change; negative |
| treasury_10y | P1 | .60 | .15 | .25 | MA12 level; MA12 proportional lag-3/lag-12 change; negative |
| fedfunds | P5 | .50 | .10 | .40 | MA3 level; MA3 proportional lag-3/lag-12 change; negative |
| spread_10y_2y | P7 | .35 | .10 | .55 | MA9 level; MA9 arithmetic lag-3/lag-12 difference; positive |
| spread_10y_fedfunds | P9 | .40 | .10 | .50 | MA9 level; MA9 arithmetic lag-3/lag-12 difference; positive |

The evidence trail is Phase 1 anatomy; national-native/aligned-evaluation geography repair; cross-axis dtype repair; Phase 2 P0–P7 calibration; Phase 2.5 incumbent-reference correction, turning/delay validation, performance hardening, and responsiveness repair; P8/P9 boundary extension; and human selection. ADR-012 contains the rationale.

Metric weights remain mortgage_30y .15, mortgage_15y .15, treasury_10y .15, fedfunds .10, spread_10y_2y .225, and spread_10y_fedfunds .225. The conceptual 45% long-rate / 10% Fed Funds / 45% spread allocation is unchanged and pending separate calibration. Capital Markets remains 10% of Demand and 15% of Supply. Normalization and Supply S8 are unchanged; so are Price, Affordability, and Labor.

## Immutable production run

Authoritative local data is required. Do not commit the generated run:

```bash
PYTHONPATH=. python -u scripts/run_regime_pipeline.py \
  --run-id capital_markets_feature_policy_production_20260818 \
  --experiment-id capital_markets_feature_policy_production \
  --artifact-root artifacts/regime/runs \
  --serving-db data/market_serving.duckdb \
  --metadata-json '{"promotion_contract":"capital_markets_native_feature_policy_2026_08_18","mortgage_30y_feature_policy":"P4","mortgage_15y_feature_policy":"P2","treasury_10y_feature_policy":"P1","fedfunds_feature_policy":"P5","spread_10y_2y_feature_policy":"P7","spread_10y_fedfunds_feature_policy":"P9","capital_markets_metric_weights":{"mortgage_30y":0.15,"mortgage_15y":0.15,"treasury_10y":0.15,"fedfunds":0.10,"spread_10y_2y":0.225,"spread_10y_fedfunds":0.225},"family_metric_weight_calibration":"pending","human_decision":"capital_markets_native_feature_policy_approved","automated_winner":false}'
```

Validate it against the immediate pre-promotion Supply S8 baseline:

```bash
PYTHONPATH=. python scripts/validate_capital_markets_feature_policy_run.py
```

Governance is closed for native features and promoted, but Capital Markets is not fully frozen until family metric-weight calibration is complete. No candidate family-weight grid has been promoted.
