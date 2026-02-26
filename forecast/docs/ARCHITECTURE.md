# Forecast Architecture Rules (Phase C)

This document defines **hard architectural constraints**.
If code behavior contradicts this document, the code is wrong.

---

## 1. Entrypoints (Phase C Rule)

Authoritative execution logic lives **only** in model runners under:

- `forecast/models/**`

CLI modules under `forecast/cli/**` are **thin wrappers only**.
They may:
- parse arguments
- validate inputs
- dispatch to runners

They MUST NOT:
- introduce new logic
- change defaults
- override governance
- mutate artifacts
- bypass invariants

Legacy runners remain temporarily for compatibility but must not be extended.

---

## 2. Determinism (Non-Negotiable)

No Phase C change may alter:
- artifact naming
- feature_id ordering
- anchor selection semantics
- as-of resolution behavior
- audit hashes

Determinism is enforced at:
- selector output (ordered feature_ids + sha256)
- `consume_selected_features` (identity + order + hash)
- design_matrix artifacts (sha256 + audit)
- exog_future artifacts (policy-hashed)

If two runs differ in output with identical inputs, the system is broken.

---

## 3. Anchors and As-Of (Canonical)

**Authoritative anchor logic lives in:**
- `forecast/core/anchors.py`

Key principles:
- Anchors are selected from **target y only**
- Anchors respect `data_asof` constraints
- Selector defaults to **exactly one anchor**
- Evaluation runners may use **multiple anchors**

Legacy modules (e.g. `forecast/backtest_utils.py`) are shims only.

---

## 4. SARIMAX-Exog: Bridge vs Live (Canonical Mental Model)

This section defines **non-negotiable semantics**.
Violations invalidate comparisons.

---

### 4.1 Terms (Used Precisely)

**Feature**
- A semantic signal: `(metric_id × geo_id × property_type_id × source)`
- Lags are *model expansions*, not features
- Feature identity is frozen by the selector

**Artifact**
- Immutable, checksummed output with audit metadata
- Examples:
  - `selected_features__anchor=...parquet`
  - `design_matrix__anchor=...__asof=...parquet`
  - `exog_future__anchor=...__asof=...__h=...parquet`

**Design Matrix Artifact**
- Contains:
  - target `y`
  - ordered exogenous columns (`feature_ids`)
- Represents everything known up to `data_asof_effective`

---

### 4.2 Selector → Model Flow (Reference Diagram)

```text
XGB selector
   │
   ▼
selected_features (artifact, ordered, hashed)
   │
   ▼
consume_selected_features
   │
   ▼
design_matrix (artifact: y + X, ordered, hashed)
   │
   ├──► SARIMAX-exog BRIDGE
   │       (uses future rows already present)
   │
   └──► SARIMAX-exog LIVE
           (forecasts exog deterministically)
```

- Everything downstream of design_matrix is feature-identity frozen.

---

### 4.3 Bridge Runner (Upper Bound Control)

**Module**
- forecast/models/sarimax_exog/bridge_runner.py

**Definition**
- “If future exogenous values were perfectly known, how well could SARIMAX-exog perform?”

**Rules**
- ❌ No feature rebuild
- ❌ No exog forecasting
- ✅ Uses only future rows already present in the design matrix
- ✅ Fails if horizon exceeds available rows

**Purpose**
- Establishes a theoretical ceiling
- Acts as a gate before investing in live exog forecasting

**Interpretation**
- Optimistic
- Non-deployable
- Scientifically required

---

### 4.4 Live Runner (Deploy-Faithful)

**Module**
- forecast/models/sarimax_exog/live_runner.py

**Definition**
- “Given only information available at data_asof, how would this model behave in production?”

**Rules**
- ❌ No feature rebuild
- ❌ No access to future true exogs
- ✅ Deterministically forecasts future exogs
- ✅ Emits exog_future + prediction artifacts with audit

**Purpose**
- Measures real deployable performance
- Mirrors production behavior exactly

**Interpretation**
- Conservative
- Deployable
- Truth-bearing

---

### 4.5 Why Both Exist (Non-Optional Logic)

- Bridge answers: “Is this even worth pursuing?”
- Live answers: “What will actually happen?”

**Decision rules:**
- Bridge ≤ univariate → stop (exogs useless)
- Bridge > univariate AND Live ≤ univariate → signal exists but exog forecasting destroys value
- Live > univariate → exogs are production-worthy

Bridge is a *gate*.  
Live is a *truth test*.
---

## 5. Backtests vs Live (Critical Distinction)

- Backtests answer: “What would have happened if run then?”
- Live answers: “What happens if run today?”

**Therefore:**
- Backtests must respect information constraints at anchor time
- Bridge is allowed only as an upper bound
- Live backtests must mirror production exactly

---

## 6. Selector Governance (Phase C Baseline)

  
                ┌──────────────────────────┐
                │   XGB SELECTOR (past)    │
                │  chooses FEATURE SET     │
                └───────────┬──────────────┘
                            │
                            ▼
           ┌────────────────────────────────────┐
           │ DESIGN MATRIX ARTIFACT             │
           │ (y + X up to as_of; MAY include    |
           │ limited future X strictly for      |
           │  BRIDGE evaluation only)           │
           └───────────┬──────────────┬─────────┘
                       │              │
                       │              │
        (Bridge path)  │              │  (Live path)
                       │              │
                       ▼              ▼
        ┌─────────────────────┐   ┌─────────────────────┐
        │ BRIDGE RUNNER       │   │ EXOG FUTURE BUILDER │
        │ uses only rows that │   │ forecasts X forward │
        │ already exist       │   │ deterministically   │
        └──────────┬──────────┘   └──────────┬──────────┘
                   │                         │
                   ▼                         ▼
        ┌─────────────────────┐   ┌─────────────────────┐
        │ SARIMAX-EXOG        │   │ SARIMAX-EXOG        │
        │ (perfect-X eval)    │   │ (live-faithful)     │
        └─────────────────────┘   └─────────────────────┘

---

**These rules are defaults, not tunables:**
- metric_pt_cap = 10
- min_non_redfin = 25
- redfin_tier_caps = ON

**Changes require:**
- explicit versioning
- audit regeneration
- documentation updates

**Details live in:**
- forecast/README_selector.md

---

## 7. Non-Negotiable Invariants

- Feature identity comes only from selector artifacts
- Column order follows audited feature_ids
- All artifacts are immutable and checksummed
- No runner may regenerate features or reorder columns

Violating any invariant invalidates all comparisons.
















































# Forecast Architecture Rules (Phase C — Post Phase-B Freeze)

This document defines hard architectural constraints.
If code behavior contradicts this document, the code is wrong.

Last Updated: After canonical exog v09.0 freeze + bridge K-sweep diagnostics.
