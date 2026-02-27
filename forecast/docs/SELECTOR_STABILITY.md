# Selector Stability Ranking

*(Phase C — Post Bridge Diagnostics)*

This document defines how we rank and freeze exogenous feature identities for SARIMAX-exog using artifacts emitted by `xgb_selector`.

It governs **feature identity stability**, not final model dimensionality.

If behavior contradicts this document, the code is wrong.

---

## 1. Why Stability Ranking Exists

`xgb_selector` produces a per-anchor selection of features.

A single anchor is not enough to freeze a defensible canonical exog set because:
- Feature usefulness can vary by anchor (regime sensitivity)
- The selector is constrained by governance caps and quotas
- Short-horizon lift can be noisy
- Feature identity must be defensible across time

Stability ranking aggregates per-anchor evidence into a **per-feature cross-anchor stability signal**.

It answers:
> “Is this feature consistently useful enough to freeze as part of canonical identity?”

It does not answer:
> “Will SARIMAX behave well with this full set?”

That is handled separately (see Policy B).

---

## 2. Terminology

**Anchor**
A training cutoff month-end where we evaluate selection/scoring.

**Lift vs baseline**
Predictive improvement of a cheap model over a baseline predictor, computed per feature and per anchor. Lift can be negative.

**Selected**
Whether the feature was selected by `xgb_selector` at a given anchor.

**base_feature_id**
Lag-stripped feature identity:
`metric__geo__pt__source`

Lags are model expansions, not identities.

---

## 3. Data Sources (Required Columns)

From each:

`candidate_scores__anchor=YYYY-MM-DD.parquet`

Required columns:
- `feature_id`
- `lift_vs_baseline`
- `selected`
- `best_lead` (diagnostic)
- `n_eff` (diagnostic)

Before aggregation:
- Lag suffixes must be stripped
- `_lag0` is illegal (system invariant)

---

## 4. Aggregation: Per Feature Per Anchor

If multiple rows exist for the same base feature within an anchor:
- `selected(anchor)` = max(selected)
- `lift_any(anchor)` = max(lift_vs_baseline)
- `lift_selected(anchor)` = max(lift_vs_baseline where selected == 1), else NaN

This ensures a base feature contributes at most one lift value per anchor.

---

## 5. Two Stability Rankings

Stability is computed in two independent ways.

---

### A) Promotion-Aligned Stability (Selected-Only)

Computed only on anchors where the feature was selected.

Statistics:
- `selected_anchors`
- `median_lift_selected`
- `std_lift_selected`
- `min_lift_selected`
- `win_rate_selected`
  - `win_selected = lift_selected > win_eps`

Eligibility gate (default):
- `selected_anchors >= 2`

Promotion score:
- promotion_score = median_lift_selected * win_rate_selected / (1 + std_lift_selected)

Ineligible features receive -inf score (deterministic bottom placement).

Interpretation:

Promotion ranking reflects what the selector actually trusted repeatedly.

---

### B) Intrinsic Stability (All-Anchors with Downside Gates)

Computed across all anchors, independent of selection.

Diagnostics:
- `median_lift_any`
- `std_lift_any`
- `p10_lift_any`
- `neg_rate_any`

Eligibility gates (default):
- `p10_lift_any >= -0.05`
- `neg_rate_any <= 0.25`

Intrinsic score:

Must reward positive central tendency and penalize volatility.
Downside gates prevent “two-cluster trap” features.

Interpretation:

Intrinsic ranking detects features with broad signal even if selector quotas suppressed them.

---

## 6. Merged Output and Origin

Merged per-metric table includes:
- `promotion_rank`
- `intrinsic_rank`
- `eligible_promotion`
- `eligible_intrinsic`
- `promotion_score`
- `intrinsic_score`
- `origin:`

**origin**        **meaning**
both              eligible in both rankings
promotion         eligible promotion only
intrinsic         eligible intrinsic only
neither           eligible in neither

Artifact:

`stability_merged__metric=<metric>.csv`

---

## 7. Canonical Exog Freeze Policy (v09.0)

Production freeze priority:
1. `origin == both`
2. then `origin == promotion`
3. `origin == intrinsic` reserved for research

This yields a deterministic canonical identity set.

Important:
- Canonical v09.0 removed `_lag0`
- Leads restricted to `{1,3,6,12}`
- Identity freeze is separate from model K

Canonical freeze defines **who may enter the model**, not how many.

---

## 8. Critical Distinction: Stability ≠ SARIMAX Adequacy

Bridge diagnostics revealed:
- High K (≥15) leads to:
  - Rank deficiency
  - Condition numbers ≈ 1e16
  - Near-singular exog matrices
  - Degraded MAE

Therefore:

Selector stability does NOT guarantee:
- Good conditioning
- Identifiable parameters
- Good SARIMAX performance

That is governed by:
- Policy B (obs-to-parameter adequacy rule)
- Conditioning diagnostics
- Exog rank checks

Selector governs identity.
SARIMAX governs statistical validity.

---

## 9. Lessons from Bridge Diagnostics

Empirical findings (median_ppsf, dc_city):
- K=5 outperformed larger K
- K=15–30 degraded
- Rank deficiency observed (rank 29 of 30)
- Condition numbers ~1e16

Conclusion:

Raw stability ranking allows too many correlated signals.

Selector must evolve.

---

## 10. Phase C Evolution (D): Stability Under Collinearity

Current selector ranks independently.

Planned redesign:

Selector will optimize for:
> Stable marginal lift under diversity + redundancy control

Future additions:
- Greedy forward selection within top pool
- Incremental lift threshold (ε)
- Family caps
- Optional conditioning-aware redundancy pruning

Goal:
- Prevent highly correlated clusters entering canonical identity
- Improve downstream conditioning before SARIMAX fit

This is a selector-level improvement.
It does NOT replace Policy B.

---

## 11. Relationship to Policy B (Model Adequacy)

Policy B enforces:
- Obs-to-parameter minimum ratios
- Hard fail on insufficient data
- Deterministic gating

Selector stability chooses candidates.
Policy B determines if model fit is allowed.

Both layers are required.

---

## 12. Implementation

Script:

`forecast/selection/stability_rank.py`

Artifacts:
- `stability_merged__metric=<metric>.csv`
- `canonical_exog_set__metric=<metric>.csv`

Versioning:

Stability logic changes require:
- Explicit version bump
- Rerun of stability artifacts
- Documentation update
