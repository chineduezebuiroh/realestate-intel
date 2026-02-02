# XGB Selector (Feature Identity + Shortlist Governance)

The XGB selector is responsible for **feature identity selection**, not forecasting.

It produces an **ordered, immutable shortlist of exogenous features**
that is consumed downstream by:
- SARIMAX-exog (bridge and live)
- no other model

This document defines the **selector-of-record contract**.
If code behavior contradicts this document, the code is wrong.

---

## 1. Two Horizons (Intentional and Non-Interchangeable)

This system intentionally uses **two different horizons**.

### 1.1 Selector Horizon (Feature Identity)

- Used only by the XGB selector
- Typical values: **1–3 months**
- Purpose:
  - Rank and govern candidate exogenous features
  - Stabilize feature identity
- This is **not a modeling choice**
- This is a **governance choice**

Short horizons are required because:
- Feature importance is more stable
- Long-horizon uncertainty contaminates feature identity

---

### 1.2 Production Forecast Horizon

Used by:
- SARIMAX-univariate
- SARIMAX-exog (bridge + live)
- XGB forecast models

Typical values:
- 6, 12, 18 months

**These horizons must never be conflated.**

---

## 2. Anchor Selection Semantics (Hard Rules)

Selector anchors are chosen from **target y only**, subject to:

- `horizon`
- `min_train_len`
- `step_months`
- `max_anchors`
- `latest_anchor_offset_months`
- `data_asof` (hard clamp)

### 2.1 Backtestability Rule (Non-Negotiable)

The freshest selector anchor must still have future `y` available for scoring.

Therefore:

- latest_anchor_offset_months >= selector_horizon


Default behavior:
- `latest_anchor_offset_months = selector_horizon`

---

### 2.2 data_asof Clamp (Critical)

Selector anchor selection is performed **as if the run occurred at `data_asof`**.

- Anchors after `data_asof` are illegal
- Feature identity must reflect only information available at that time

This ensures:
- Fair backtests
- Comparable eval batches
- Deterministic reruns

---

### 2.3 Single-Anchor Selector Contract

**By default, selector backtests MUST produce exactly one anchor.**

Rules:
- `max_anchors = 1` is enforced
- Multi-anchor selector runs are allowed **only** when:
  - anchors are explicitly provided
  - the run is an evaluation batch

This is intentional:
- Selector output must be a single, deterministic feature identity
- Multi-anchor selection contaminates governance

---

## 3. Selector Governance (Phase C Baseline)

The selector enforces **non-experimental governance rules**.

These are defaults, not tuning knobs.

### Baseline Rules

- `metric_pt_cap = 10`  
  Maximum base series per `(metric_id, property_type_id)`

- `min_non_redfin = 25`  
  Minimum number of non-Redfin features in final top-K

- `redfin_tier_caps = ON`  
  Tiered share caps enforced for Redfin metrics

These rules prevent:
- metric echoing across geographies
- single-source dominance
- fragile shortlists

Any change requires:
- explicit versioning
- selector reruns
- documentation updates

---

## 4. Selector Artifacts (Selector-of-Record)

Selector runs write artifacts to:

- runs/<batch_id>/xgb/


Primary artifact:
- `selected_features__anchor=YYYY-MM-DD.parquet`

This artifact is:
- ordered
- checksummed (sha256)
- immutable
- the **sole source of feature identity** for SARIMAX-exog

Downstream models MUST NOT:
- rebuild features
- reorder columns
- substitute alternative shortlists

---

## 5. Selector Evaluation (Batch-Oriented)

Selector evaluation is performed via:
- `forecast/cli/eval_xgb_selector`

Evaluation characteristics:
- supports multi-anchor batches
- emits a `.jsonl` manifest
- records per-anchor artifacts + hashes
- designed for auditability, not ad-hoc inspection

Selector evaluation exists to:
- validate governance behavior
- compare stability across anchors
- support promotion decisions

---

## 6. Freshness Policy (Fail Fast by Design)

SARIMAX-exog validates selector freshness relative to `data_asof`.

If the selector anchor is too stale:
- the run fails immediately
- the selector must be re-run closer to `data_asof`

This is intentional.

Using stale feature identity invalidates:
- backtests
- live comparisons
- production confidence
