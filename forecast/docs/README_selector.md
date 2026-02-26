# XGB Selector (Feature Identity + Shortlist Governance)

*(Phase C — Post Phase-B Freeze)*

The XGB selector is responsible for feature identity selection, not forecasting.

It produces an **ordered, immutable shortlist of exogenous features** that is consumed downstream by:
- SARIMAX-exog (bridge and live)
- no other model

This document defines the **selector-of-record contract**.
If code behavior contradicts this document, the code is wrong.

Last Updated: After canonical exog v09.0 freeze + bridge diagnostic sweep.

---

## 1. Two Horizons (Intentional and Non-Interchangeable)

This system intentionally uses two different horizons.

### 1.1 Selector Horizon (Feature Identity)

Used only by the XGB selector.

Typical values: **1–3 months**

Purpose:
- Rank and govern candidate exogenous features
- Stabilize feature identity
- Minimize long-horizon noise contamination

This is **not a modeling choice**.
This is a **governance choice**.

Short horizons are required because:
- Feature importance is more stable
- Long-horizon uncertainty contaminates identity
- Selector is defining *what signals exist*, not forecasting performance

---

### 1.2 Production Forecast Horizon

Used by:
- SARIMAX-univariate
- SARIMAX-exog (bridge + live)
- XGB forecast models

Typical values:
- 6, 12, 18 months

These horizons must never be conflated.

Selector horizon governs identity.
Production horizon governs forecasting.

---

## 2. Anchor Selection Semantics (Hard Rules)

Selector anchors are chosen from target y only, subject to:
- `horizon`
- `min_train_len`
- `step_months`
- `max_anchors`
- `latest_anchor_offset_months`
- `data_asof` (hard clamp)

---

### 2.1 Backtestability Rule (Non-Negotiable)

The freshest selector anchor must still have future `y` available for scoring.

Therefore:

`latest_anchor_offset_months >= selector_horizon`

Default behavior:

`latest_anchor_offset_months = selector_horizon`

---

### 2.2 data_asof Clamp (Critical)

Selector anchor selection is performed as if the run occurred at `data_asof`.
- Anchors after `data_asof` are illegal
- Feature identity must reflect only information available at that time

This ensures:
- Fair backtests
- Comparable evaluation batches
- Deterministic reruns
- No as-of leakage

---

### 2.3 Single-Anchor Selector Contract

By default, selector backtests MUST produce exactly one anchor.

Rules:
- `max_anchors = 1` is enforced
- Multi-anchor selector runs are allowed only when:
  - anchors are explicitly provided
  - the run is an evaluation batch

This is intentional.

Selector output must be a single, deterministic feature identity.
Multi-anchor selection contaminates governance.

---

## 3. Feature ID Contract (Frozen)

Canonical format:

`<metric_id>__<geo_id>__<property_type_id>__<source_id>_lag<k>`

Allowed lags:

`1, 3, 6, 12`

Rules:
- `_lag0` is prohibited system-wide.
- Selector must not emit lag0.
- Canonical exog sets must not contain lag0.
- Downstream parsing depends on this format.

Changing this requires explicit versioning and full reruns.

---

## 4. Selector Governance (Phase C Baseline)

These rules are defaults, not tuning knobs.

**Baseline Rules**
- `metric_pt_cap = 10`
Maximum base series per `(metric_id, property_type_id)`

- `min_non_redfin = 25`
Minimum number of non-Redfin features in final top-K

- `redfin_tier_caps = ON`

These rules prevent:
- metric echoing across geographies
- single-source dominance
- fragile shortlists

Any change requires:
- explicit version bump
- selector reruns
- documentation updates

---

## 5. Selector Stability + Canonical Exog Freeze

Selector candidate artifacts are aggregated into stability rankings:
- Promotion-aligned stability
- Intrinsic stability

Merged into:

`stability_merged__metric=<metric>.csv`

Canonical exog set (v09.0):
- No lag0
- Leads restricted to `{1,3,6,12}`
- Built via deterministic rule:
  - origin == both
  - then origin == promotion
  - intrinsic reserved for research

Canonical exog sets freeze feature identity, not final model dimensionality.

---

## 6. Bridge Interaction (Important Clarification)

Bridge runner:
- Uses canonical feature identity
- Does NOT rebuild features
- Evaluates short-horizon marginal signal

Bridge exists to answer:

“Does this feature identity add signal at all?”

Bridge does NOT:
- Validate long-horizon deployability
- Guarantee production performance
- Override selector governance

Selector defines identity.
Bridge tests signal ceiling.

---

## 7. Phase C Direction — Selector Evolution (D)

Empirical finding from bridge diagnostics:
- High K (≥15) produces:
  - Severe collinearity
  - Rank deficiency
  - Condition numbers ≈ 1e16
  - Performance degradation

Therefore, selector must evolve from:
> Independent ranking

to:
> Marginal predictive lift under diversity + stability governance

Planned evolution:
1. Stage 1:
  - Cheap predictive lift (default)
2. Stage 2:
  - Greedy forward selection within top candidate pool
  - Add feature only if incremental lift ≥ ε
3. Diversity constraints:
  - Family caps
  - Metric caps
  - Optional geo caps
4. Conditioning awareness:
  - Evaluate on final complete-case mask
  - Emit redundancy diagnostics

Selector will optimize for stable marginal contribution, not raw lift.

This does NOT replace SARIMAX adequacy gating (Policy B).
Both layers must enforce stability.

---

## 8. Freshness Policy (Fail Fast by Design)

SARIMAX-exog validates selector freshness relative to `data_asof`.

If the selector anchor is too stale:
- the run fails immediately
- selector must be re-run

Using stale feature identity invalidates:
- backtests
- live comparisons
- production confidence

Freshness enforcement is intentional.

---

## 9. What Selector Does NOT Do

Selector does NOT:
- Forecast target
- Enforce parameter adequacy (model responsibility)
- Override SARIMAX gating rules
- Forecast future exogs
- Mutate canonical exog sets during bridge or live

Selector defines identity.
Models enforce statistical discipline.
