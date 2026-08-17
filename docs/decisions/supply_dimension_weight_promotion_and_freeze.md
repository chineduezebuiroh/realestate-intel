# Supply Dimension Weight Promotion and Freeze

## Current production decision — S8 (2026-08-17)

The human-approved current policy is **S8**: Active Inventory 0.65, Permit
Activity 0.30, and Permit Intensity 0.05. The machine contract is
`config/supply_metric_weight_s8_2026_08_17.json`, the frozen architecture is
`config/supply_dimension_frozen_s8_2026_08_17.json`, and ADR-011 records the
decision. This closes the S0-S9 bounded metric-weight campaign and completes
Supply calibration. It was not an automated winner.

Native policies remain Active Inventory MA12/I4 (40/15/45), Permit Activity
MA12/A2 (75/10/15 with governed lag-6 Short), and Permit Intensity MA12/N4
(40/15/45). S8 changes only the three governed Supply metric weights. Feature
construction, normalization, membership, missingness renormalization,
Supply-to-axis weights, Demand, Labor, Price, Affordability, and Capital Markets
are unchanged.

S4 remains the historical Permit-responsive stability reference, S2 the
historical incremental-Intensity compromise, and S9 the rejected upper-bound
stress. S9 weakened Permit responsiveness and made Inventory excessively
dominant. The practical boundary is closed at 65%; a 75% test was neither run
nor warranted.

Generate the immutable production run with:

```bash
PYTHONPATH=. python -u scripts/run_regime_pipeline.py \
  --run-id supply_s8_production_20260817 \
  --experiment-id supply_s8_production \
  --artifact-root artifacts/regime/runs \
  --serving-db data/market_serving.duckdb \
  --metadata-json '{"promotion_contract":"supply_metric_weight_s8_2026_08_17","active_inventory":"MA12/I4","permit_activity":"MA12/A2","permit_intensity":"MA12/N4","supply_metric_weights":{"active_inventory":0.65,"permit_activity":0.30,"permit_intensity":0.05},"human_decision":"supply_s8_metric_weight_approved","automated_winner":false,"supply_calibration":"closed","capital_markets":"unchanged"}'
PYTHONPATH=. python scripts/validate_supply_s8_production_run.py
```

Generated runs remain immutable, ignored, and uncommitted.

## Historical production decision — S0 (superseded)

### Decision

`Supply` is frozen under `supply_dimension_frozen_v1` following the
human-approved promotion `supply_metric_weight_promotion_2026_08_06` on
2026-08-06. The authoritative registry rows are `redfin_inventory`,
`bps_total_units`, and `derived_permit_intensity` in
`config/metric_dimension_registry.csv`. Their prior weights were
`0.3334 / 0.3333 / 0.3333`; their production weights are now
`0.60 / 0.20 / 0.20`.

The evidence contract is `supply_metric_weight_diagnostic_v1`, evaluated against
the authoritative source run
`macro_regime_v1_settled_ma12_features_20260805`. Its historical
`recommendation_state = none` and `promotion_state = none` remain unchanged.
Challenger A was selected afterward by explicit human decision.

Permit activity and permit intensity are highly correlated, and equal weighting
caused the combined permit family to dominate Supply. Challenger A materially
improved stability and reduced permit-family dominance while preserving
responsiveness within the governed diagnostic. The human selected it as the
minimum effective correction over the stronger Challenger B.

The sibling dimensions in the same registry are Price, Transaction Activity,
Liquidity, Demand, Capital Markets, and Affordability. None of their weights
changed. No Supply membership, settled MA12 feature policy, source precedence,
alignment, geography, normalization, axis, coordinate, geometry, regime,
transition, cancellation, or missing-value policy changed. The existing scorer
drops unavailable observations and divides by the available configured-weight
sum; it is not a Supply-specific implementation and it does not fill values.
Capital Markets is the next diagnostic workstream.

The machine-readable promotion and freeze records are respectively
`config/supply_metric_weight_promotion_2026_08_06.json` and
`config/supply_dimension_frozen_v1.json`.

## Post-merge immutable production rerun

The hosted environment does not contain the governed local serving database and
source-artifact set, so the authoritative run is deliberately deferred. From the
merged revision, run exactly (with no `--smoothing-experiment-id`):

```bash
PYTHONPATH=. python -u \
  scripts/run_regime_pipeline.py \
  --run-id macro_regime_v1_frozen_supply_20260806 \
  --experiment-id supply_metric_weight_promotion_2026_08_06 \
  --artifact-root artifacts/regime/runs \
  --serving-db data/market_serving.duckdb \
  --validation-geo district_of_columbia_dc__county \
  --validation-geo essex_county_nj__county \
  --validation-geo montgomery_county_md__county \
  --validation-geo prince_george_s_county_md__county \
  --validation-geo fairfax_county_va__county \
  --validation-geo san_francisco_county_ca__county \
  --validation-geo los_angeles_county_ca__county \
  --metadata-json '{
    "run_purpose": "authoritative production baseline after frozen Supply promotion",
    "promotion_contract": "supply_metric_weight_promotion_2026_08_06",
    "supply_freeze_contract": "supply_dimension_frozen_v1",
    "supply_metrics": ["active_inventory", "permit_activity", "permit_intensity"],
    "supply_metric_weights": {"active_inventory": 0.60, "permit_activity": 0.20, "permit_intensity": 0.20},
    "settled_feature_weights": {"level": 0.50, "short": 0.25, "long": 0.25},
    "ma12_structural_contract": {
      "level": "MA12(raw)",
      "short": "MA12(raw) / lag3(MA12(raw)) - 1",
      "long": "MA12(raw) / lag12(MA12(raw)) - 1"
    },
    "capital_markets_status": "unchanged_pending_decomposition",
    "affordability_status": "unchanged_pending_capital_markets_decision"
  }'
```

The run ID is new and the artifact store rejects an incumbent directory by
default; no prior immutable run may be overwritten.

## Deterministic post-rerun validation

First run the focused registry/freeze test, then validate artifact integrity and
compare the new run to the settled-MA12 parent:

```bash
PYTHONPATH=. python -u scripts/smoke_tests/90_99/99_supply_metric_weight_promotion.py
PYTHONPATH=. python -u scripts/validate_frozen_supply_run.py \
  --artifact-root artifacts/regime/runs \
  --run-id macro_regime_v1_frozen_supply_20260806 \
  --prior-run-id macro_regime_v1_settled_ma12_features_20260805
```

The validator checks manifest completion and identity, hashes and policy
metadata, governed-county Supply output, reconstruction through the existing
production dimension scorer (including sparse-date effective weights), settled
feature rows, unchanged non-Supply dimensions, and the presence of recomputed
Supply-axis/coordinate/geometry/regime descendants. It also rejects an absent
prior run or identical run identity. Runtime reports and production artifacts
remain local and must not be committed.
