# Selector stability ranking

This document defines how we rank and freeze exogenous feature identities for SARIMAX-exog using artifacts emitted by `xgb_selector`.

## Why stability ranking exists

`xgb_selector` produces a per-anchor selection of features. A single anchor is not enough to freeze a defensible canonical exog set because:
- feature usefulness can vary by anchor (regime sensitivity)
- the selector is constrained by governance caps/quotas
- we want a principled stability signal before promoting to “production identity”

Stability ranking aggregates per-anchor evidence into a per-feature score.

## Terminology

- **Anchor**: a training cutoff month-end where we evaluate selection/scoring.
- **Lift vs baseline**: predictive improvement of a cheap model over a baseline predictor, computed per feature and per anchor. Lift can be negative.
- **Selected**: whether the feature was selected by `xgb_selector` at a given anchor.
- **base_feature_id**: lag-stripped feature identity (e.g., `metric__geo__pt__source`).

## Data sources (required columns)

From each `candidate_scores__anchor=...parquet`:
- `feature_id` (base id; lags must be stripped before aggregation)
- `lift_vs_baseline` (float; negative allowed)
- `selected` (bool/int)
- `best_lead`, `n_eff` (diagnostic only)

## Aggregation: per feature per anchor

If multiple rows exist for the same base feature within an anchor:
- `selected(anchor)` = max(selected)
- `lift_any(anchor)` = max(lift_vs_baseline) across available rows
- `lift_selected(anchor)` = max(lift_vs_baseline) across rows where selected==1, else NaN

## Two rankings

### A) Promotion-aligned stability (selected-only)

Computed only on anchors where the feature was selected.

Selected-only stats:
- `selected_anchors`
- `median_lift_selected`, `std_lift_selected`, `min_lift_selected`
- `win_rate_selected` where `win_selected = lift_selected > win_eps`

Default eligibility gates (promotion):
- `selected_anchors >= 2`

Promotion score:

promotion_score = median_lift_selected * win_rate_selected / (1 + std_lift_selected)


Ineligible features get score = `-inf` (pushed to bottom deterministically).

### B) Intrinsic stability (all-anchors with downside gates)

Computed across all anchors (independent of selection), with downside-risk diagnostics.

All-anchor diagnostics:
- `median_lift_any`, `std_lift_any`
- `p10_lift_any` (10th percentile)
- `neg_rate_any` (fraction of anchors with lift_any < 0)

Default eligibility gates (intrinsic):
- `p10_lift_any >= -0.05`
- `neg_rate_any <= 0.25`

Intrinsic score (default form; may evolve):
- must reward positive central tendency and penalize volatility; downside gates prevent “two-cluster” traps.

## Merged output and origin

The merged per-metric table contains:
- `promotion_rank`, `intrinsic_rank`
- `eligible_promotion`, `eligible_intrinsic`
- `promotion_score`, `intrinsic_score`
- `origin`:
  - `both` = eligible in both rankings
  - `promotion` = eligible promotion only
  - `intrinsic` = eligible intrinsic only
  - `neither` = eligible in neither

## Canonical exog freeze policy

Default production freeze priority:
1) `origin == both` (sorted by promotion_rank)
2) then `origin == promotion` (sorted by promotion_rank)
3) keep `origin == intrinsic` as research overflow unless explicitly promoted later

This yields a deterministic, auditable canonical identity set used by:
- SARIMAX-exog bridge/backtest/live runs

## Implementation

Script:
- `forecast/selection/stability_rank.py`

Artifacts:
- `artifacts/<phase>/selector_stability/<version>/stability_merged__metric=<metric>.csv`
- optionally `canonical_exog_set__metric=<metric>.csv`
