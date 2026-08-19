# Capital Markets F4 family-weight promotion and final freeze

On 2026-08-18, human review approved F4 (35% Long-Term Rates / 10% Fed Funds / 55% Spreads) from the corrected F0-F9 rerun rooted at `capital_markets_feature_policy_corrected_production_20260818`. F4 is a supported selection within the practical plateau, not a mathematical optimum, and was not an automated winner. ADR-014 records rationale and rejected alternatives; the promotion and freeze JSON contracts are authoritative machine-readable governance.

All six native policies (P4/P2/P1/P5/P6/P9), the canonical 10Y-2Y sign inversion, normalization, Demand/Supply weights (0.10/0.15), and Supply S8 remain frozen. Equal intra-family weighting is retained and further calibration is not required. Capital Markets is fully closed.

## Production materialization

Authoritative artifacts are absent from hosted Codex and no run is fabricated. After merge, with the corrected baseline and authoritative serving data locally available, materialize the immutable run from the promoted registry:

```bash
python scripts/run_regime_pipeline.py --run-id capital_markets_f4_production_20260818 --experiment-id capital_markets_f4_production --metadata-json '{"promotion_contract":"capital_markets_family_weight_f4_2026_08_18","capital_markets_family_policy":"F4","capital_markets_family_weights":{"long_term_rates":0.35,"fedfunds":0.10,"spreads":0.55},"capital_markets_metric_weights":{"mortgage_30y":0.11666666666666667,"mortgage_15y":0.11666666666666667,"treasury_10y":0.11666666666666667,"fedfunds":0.10,"spread_10y_2y":0.275,"spread_10y_fedfunds":0.275},"capital_markets_feature_policies":{"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_2y":"P6","spread_10y_fedfunds":"P9"},"corrected_spread_polarity":true,"normalization_changed":false,"family_metric_weight_calibration":"closed","capital_markets_calibration":"closed","capital_markets_fully_frozen":true,"human_decision":"capital_markets_f4_family_weight_approved","automated_winner":false}'
python scripts/validate_capital_markets_f4_production_run.py
```

## Non-blocking engineering backlog

The corrected family-weight diagnostic took approximately 12m39s, including approximately ten minutes in stability-statistics computation. Future diagnostic-performance hardening should target cached/vectorized stability-statistics calculation. This is non-blocking and does not prevent the freeze.
