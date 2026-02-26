# Forecast Architecture Rules (Phase C — Post Phase-B Freeze)

This document defines hard architectural constraints.
If code behavior contradicts this document, the code is wrong.

Last Updated: After canonical exog v09.0 freeze + bridge K-sweep diagnostics.

---

## 1. Entrypoints (Phase C Rule)
Authoritative execution logic lives only in model runners under:
- `forecast/models/**`

CLI modules under `forecast/cli/**` are thin wrappers only.

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
- evaluation artifacts (score_table + eval_frame sha256)

If two runs differ in output with identical inputs, the system is broken.

---

## 3. Anchors and As-Of (Canonical)

Authoritative anchor logic lives in:
- `forecast/core/anchors.py`

Key principles:
- Anchors are selected from target y only
- Anchors respect `data_asof` constraints
- `data_asof` must be explicitly passed (no fallback)
- Selector defaults to exactly one anchor
- Evaluation runners may use multiple anchors
- Evaluation MUST enforce:
  - `target_date <= data_asof`

Legacy modules (e.g. `forecast/backtest_utils.py`) are shims only.

As-of leakage is a critical violation and invalidates evaluation results.

---

## 4. SARIMAX-Exog: Bridge vs Live (Canonical Mental Model)

This section defines non-negotiable semantics.
Violations invalidate comparisons.

---

### 4.1 Terms (Used Precisely)

Feature
- A semantic signal:
`(metric_id × geo_id × property_type_id × source)`
- Lags are *model expansions*, not features
- Feature identity is frozen by the selector

Artifact
- Immutable, checksummed output with audit metadata
- Examples:
  - `selected_features__anchor=...parquet`
  - `design_matrix__anchor=...__asof=...parquet`
  - `exog_future__anchor=...__asof=...__h=...parquet`

Design Matrix Artifact
- Contains:
  - target `y`
  - ordered exogenous columns (`feature_ids`)
- Represents everything known up to `data_asof_effective`
- No runner may mutate feature identity downstream

---

### 4.2 Selector → Model Flow (Reference Diagram)

```
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

Everything downstream of design_matrix is feature-identity frozen.

---

### 4.3 Bridge Runner (Upper Bound Control)

Module:
- `forecast/models/sarimax_exog/bridge_runner.py`

Definition:
- “If future exogenous values were perfectly known, how well could SARIMAX-exog perform?”

Rules:
- ❌ No feature rebuild
- ❌ No exog forecasting
- ✅ Uses only future rows already present in design matrix
- ✅ Fails if horizon exceeds available rows
- ✅ Emits fit diagnostics:
  - `exog_rank`
  - `exog_cond`
  - `exog_smin`
  - `n_exogs_effective`
  - `n_obs_train`

Purpose:
- Establish theoretical ceiling
- Gate exog usefulness before investing in live forecasting

Interpretation:
- Optimistic
- Non-deployable
- Scientifically required

---

### 4.4 Live Runner (Deploy-Faithful)

Module:
- `forecast/models/sarimax_exog/live_runner.py`

Definition:
- “Given only information available at data_asof, how would this model behave in production?”

Rules:
- ❌ No feature rebuild
- ❌ No access to future true exogs
- ✅ Deterministically forecasts future exogs
- ✅ Emits exog_future + prediction artifacts
- ✅ Writes forecast_predictions rows
- ✅ Enforces adequacy gates (see Policy B)

Purpose:
- Measures real deployable performance
- Mirrors production exactly

Interpretation:
- Conservative
- Deployable
- Truth-bearing

---

### 4.5 Why Both Exist (Non-Optional Logic)

- Bridge answers: “Is this even worth pursuing?”
- Live answers: “What will actually happen?”

Decision rules:
- Bridge ≤ univariate → stop (exogs useless)
- Bridge > univariate AND Live ≤ univariate → signal exists but exog forecasting destroys value
- Live > univariate → exogs are production-worthy

Bridge is a gate.
Live is a truth test.

---

## 5. Backtests vs Live (Critical Distinction)

- Backtests answer: “What would have happened if run then?”
- Live answers: “What happens if run today?”

Therefore:
- Backtests must respect information constraints at anchor time
- Evaluation must enforce data_asof limits
- Bridge is allowed only as an upper bound diagnostic
- Live backtests must mirror production exactly

---

## 6. Selector Governance (Phase C Baseline)

```
                ┌──────────────────────────┐
                │   XGB SELECTOR (past)    │
                │  chooses FEATURE SET     │
                └───────────┬──────────────┘
                            │
                            ▼
           ┌────────────────────────────────────┐
           │ DESIGN MATRIX ARTIFACT             │
           │ (y + X up to as_of; MAY include    │
           │ limited future X strictly for      │
           │ BRIDGE evaluation only)            │
           └───────────┬──────────────┬─────────┘
                       │              │
         (Bridge path) │              │ (Live path)
                       ▼              ▼
        ┌─────────────────────┐   ┌─────────────────────┐
        │ BRIDGE RUNNER       │   │ EXOG FUTURE BUILDER │
        │ perfect-X eval      │   │ deterministic X     │
        └──────────┬──────────┘   └──────────┬──────────┘
                   ▼                         ▼
        ┌─────────────────────┐   ┌─────────────────────┐
        │ SARIMAX-EXOG        │   │ SARIMAX-EXOG        │
        │ (upper bound)       │   │ (deploy-faithful)   │
        └─────────────────────┘   └─────────────────────┘
```

Defaults (non-experimental):
- `metric_pt_cap = 10`
- `min_non_redfin = 25`
- `redfin_tier_caps = ON`
- Allowed lags: `(1, 3, 6, 12)`
- `_lag0` prohibited
- Canonical stability version: `v09.0`

Changes require:
- explicit version bump
- audit regeneration
- documentation updates

Details live in:
- `forecast/README_selector.md`

---

## 7. Policy B — Parameter Adequacy (New Hard Constraint)

Empirical finding:
- With 87 observations and 30 exogs:
  - rank deficiency
  - condition numbers ~1e16
  - unstable performance

Therefore:

All SARIMAX-exog runners must enforce:

Reject run if:
- `n_obs / (n_exogs + arima_param_count) < 5`
- OR `exog_rank < n_exogs`
- OR `exog_cond > 1e12`

Failure must:
- write failure artifact
- not write forecast_predictions rows
- not silently continue

Adequacy is an architectural constraint, not a tuning preference.

---

## 8. Selector Evolution Direction (Phase C — D)

Selector must evolve from:
> Independent ranking

to:
> Marginal predictive lift under diversity + conditioning governance.

Planned direction:
1. Stage 1:
  - Cheap predictive lift (default)
2. Stage 2:
  - Greedy forward selection on top-K candidate pool
  - Add feature only if incremental lift ≥ ε
3. Diversity constraints:
  - Family caps
  - Metric caps
  - Optional geography caps
4. Collinearity awareness:
  - Compute on final complete-case mask
  - Emit redundancy diagnostics

This does NOT replace Policy B.
Selector and model must both enforce stability.

---

## 9. Non-Negotiable Invariants

- Feature identity comes only from selector artifacts
- Column order follows audited feature_ids
- All artifacts are immutable and checksummed
- No runner may regenerate features or reorder columns
- No evaluation may leak post-asof actuals
- No model may silently fit under rank deficiency

Violating any invariant invalidates all comparisons.
