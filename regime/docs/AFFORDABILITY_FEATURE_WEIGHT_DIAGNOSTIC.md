# Affordability Feature-Weight Diagnostic (Phase 4B)

**Phase 4B — COMPLETE**

**Selected: `AFF-FW-A`**

**Production feature weights retained: 50/20/30**

**Affordability calibration: COMPLETE**

Phase 4A's derive-first contract is frozen. Phase 4B compared only incumbent
`AFF-FW-A` (50/20/30) with challenger `AFF-FW-B` (50/25/25) for
`price_to_income` and `payment_burden`. It reused the promoted raw-derived,
full-window MA12, lag-3, and lag-12 chronology, normalized that chronology once,
and varied only contribution weights. Production configuration already matched
the selected policy and therefore required no weight edit.

The human decision retains A and does not promote B. Moving five percentage
points from long-term to short-term weight in B produced higher median movement,
higher P90/P99 tails, more sign flips, generally more turns, higher rolling
volatility, and higher cancellation without a meaningful responsiveness benefit.
Similarity to the incumbent was not a selection criterion, and no automated
rank, composite, or winner was produced.

Before settlement, the shared Phase 4A turning-point helper was generalized so
its complete grid accepts an explicit policy and metric universe. The repaired
dimension evidence now contains exactly one `affordability` row per policy, and
the decision matrix contains non-null all-history and latest-36-month dimension
turn counts.

The final production architecture derives each canonical affordability metric
from raw median sale price and canonical forward-filled household income; payment
burden additionally consumes raw canonical monthly `mortgage_30y`. No new income
smoothing or pre-derivation mortgage smoothing occurs. Each derived metric then
uses full-window MA12 structural level, lag-3 MA ratio, lag-12 MA ratio, and
50/20/30 feature weights. The Capital Markets mortgage structural feature state
does not cross the Affordability derivation boundary. Canonical economic formulas
remain unchanged.

Authoritative run:

```bash
rm -rf artifacts/regime/comparisons/affordability_feature_weight_phase4b
PYTHONPATH=. python -u scripts/build_affordability_feature_weight_diagnostic.py \
  --source-metrics artifacts/regime/runs/macro_regime_v1_frozen_supply_20260806/source_metrics.parquet \
  --source-run-id macro_regime_v1_frozen_supply_20260806 \
  --output-dir artifacts/regime/comparisons/affordability_feature_weight_phase4b
```
