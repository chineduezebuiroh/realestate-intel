# Affordability Feature-Weight Diagnostic (Phase 4B)

Phase 4A's derive-first contract is frozen. Phase 4B compares only incumbent
`AFF-FW-A` (50/20/30) with balanced challenger `AFF-FW-B` (50/25/25) for
`price_to_income` and `payment_burden`. It reuses the promoted raw-derived,
MA12, lag-3, and lag-12 chronology, normalizes that chronology once, and varies
only contribution weights. Production registries are not changed.

Scores use production availability renormalization and clipping. Contribution
reconstruction fails closed above `1e-12`; missing features remain unavailable
rather than being represented as zero. Turning points retain Phase 4A's
geography-safe three-month persistence semantics.

No winner, rank, or composite is produced. Recommendation and promotion remain
`none`, and the human decision remains `pending`. Demand-axis and regime context
are explicit empty outputs when they are not present in the canonical source
artifact.

Authoritative run:

```bash
PYTHONPATH=. python -u scripts/build_affordability_feature_weight_diagnostic.py \
  --source-metrics artifacts/regime/runs/macro_regime_v1_frozen_supply_20260806/source_metrics.parquet \
  --source-run-id macro_regime_v1_frozen_supply_20260806 \
  --output-dir artifacts/regime/comparisons/affordability_feature_weight_phase4b
```
