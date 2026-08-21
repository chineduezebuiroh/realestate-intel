# Capital Markets `spread_10y_2y` Polarity Repair

## Decision

Canonical source resolution sign-inverts the retained physical provider series:

- physical identity: `fred_spread_2y_10y = treasury_2y - treasury_10y`;
- canonical identity: `spread_10y_2y = treasury_10y - treasury_2y`;
- canonicalization: `spread_10y_2y = -fred_spread_2y_10y`.

The operation occurs in `resolve_canonical_metrics`, before feature engineering,
normalization, metric scoring, dimensions, axes, or regime construction. There
is no downstream polarity compensation. The provider key remains unchanged.

## Defect and proof

The production source artifact mapped the physical `2Y - 10Y` series directly
to canonical `spread_10y_2y`, despite the canonical governed formula being
`10Y - 2Y`. Authoritative local comparison established exact evidence:

- canonical versus physical: correlation `+1.0`;
- canonical versus reconstructed `2Y - 10Y`: correlation `+1.0`;
- canonical versus governed `10Y - 2Y`: correlation `-1.0`;
- maximum absolute residual versus physical: `0.0`;
- maximum absolute residual versus reconstructed reverse: `0.0`.

This caused mirror-image spread-family behavior, internal cancellation, an
invalid `spread_10y_2y` feature-calibration conclusion, and invalid F0-F9
family-weight decision evidence.

## Governance

The immutable run `capital_markets_feature_policy_production_20260818` and all
older runs remain unchanged as historical evidence. Runs produced before this
repair may contain inverted `spread_10y_2y` chronology and must not be silently
reinterpreted.

The historical promotion contract
`config/capital_markets_native_feature_policy_2026_08_18.json` is likewise
unchanged. Subsequent defect and revalidation state is recorded separately in
`config/capital_markets_spread_polarity_repair_2026_08_18.json` so discovery of
the defect does not rewrite the approval facts that occurred.

P7 remains configured as a temporary baseline but is
`revalidation_required`. P4 mortgage 30Y, P2 mortgage 15Y, P1 Treasury 10Y, P5
Fed Funds, and P9 10Y/Fed-Funds spread remain provisionally valid and unchanged.
The F0-F9 family-weight evidence is `invalidated_by_spread_polarity_defect`; its
artifacts are retained, no candidate is promoted, and rerun remains pending.
Metric weights, Demand/Supply axis weights, normalization polarity, and all
non-Capital-Markets policies are unchanged.

## Corrected candidate

Hosted data and prior immutable artifacts are not available, so no substitute
run or parity result is fabricated. Materialize locally with authoritative data:

```bash
python scripts/run_regime_pipeline.py \
  --run-id capital_markets_spread_polarity_repair_20260818 \
  --experiment-id capital_markets_spread_polarity_repair \
  --serving-db data/market_serving.duckdb \
  --metadata-json '{"repair_contract":"capital_markets_spread_10y_2y_polarity_repair_2026_08_18","canonical_formula":"treasury_10y - treasury_2y","physical_source_identity":"fred_spread_2y_10y = treasury_2y - treasury_10y","repair_method":"sign_inverted_provider_spread","human_decision":"spread_10y_2y_polarity_defect_confirmed","automated_winner":false,"spread_10y_2y_feature_policy":"P7_revalidation_required","other_capital_markets_feature_policies":{"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_fedfunds":"P9"},"family_metric_weight_calibration":"invalidated_pending_rerun","production_policy_changed":"source_canonicalization_only"}'
```

Then validate it fail closed:

```bash
python scripts/validate_capital_markets_spread_polarity_repair_run.py \
  --artifact-root artifacts/regime/runs \
  --serving-db data/market_serving.duckdb
```

The validator reports raw correlation/sign agreement and metric-score
correlation/sign agreement descriptively; those statistics are not acceptance
thresholds.

## Next steps

1. Materialize the corrected-source production candidate.
2. Rerun only `spread_10y_2y` feature anatomy/calibration.
3. Promote a corrected spread feature policy after human review.
4. Materialize a fresh Capital Markets feature-policy baseline.
5. Rerun F0-F9 family-weight calibration locally.
