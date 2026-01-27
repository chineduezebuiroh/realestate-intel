# Forecast Architecture Rules (Phase C)

## Entrypoints (Phase C rule)

Authoritative execution logic lives in model runners under:
- forecast/models/**

CLI modules under forecast/cli/** are thin wrappers only.
They may parse arguments and dispatch, but MUST NOT:
- change behavior
- introduce logic not present in runners
- alter defaults or governance

Legacy runners remain temporarily for compatibility but must not be extended.


## Determinism
No Phase C change may alter:
- artifact naming
- feature_id ordering
- as-of resolution behavior
- hashes in audit sidecars

Determinism is enforced at:
- selector output (ordered feature_ids)
- consume_selected_features (order + hash)
- design_matrix artifacts (sha256 + audit)
- exog_future artifacts (policy-hashed)


---

## SARIMAX-Exog: Bridge vs Live (Canonical Mental Model)

This section defines the *non-negotiable* semantics of SARIMAX-exog evaluation and deployment.
If this section is violated, results are not comparable.

---

### 1. Terms (used precisely)

**Feature**
- A semantic signal (metric × geo × property_type × source)
- Lags are NOT features; they are model-level expansions applied after identity is frozen.
- Features are *identified*, not rebuilt, in Phase C.


**Artifact**
- A serialized, immutable output with a checksum and audit sidecar.
- Examples:
  - `selected_features__anchor=...parquet`
  - `design_matrix__anchor=...__asof=...parquet`
  - `exog_future__anchor=...__asof=...__h=...parquet`

**Design Matrix Artifact**
- Contains:
  - `y` (target)
  - ordered exogenous columns (`feature_ids`)
- Represents *everything known up to `data_asof_effective`*.

---

### 2. The Anchor Diagram (this is the reference)

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
   │       (uses future rows already present in artifact)
   │
   └──► SARIMAX-exog LIVE
           (generates exog_future deterministically, then forecasts)

```
---


Everything downstream of `design_matrix` is **feature-identity frozen**.

---

### 3. Bridge Runner (Artifact-Driven Upper Bound)

**Module**
- `forecast/models/sarimax_exog/bridge_runner.py`

**Definition**
> “If I had perfect knowledge of future exogenous values, how well *could* SARIMAX-exog perform?”

**Rules**
- ❌ No feature rebuild
- ❌ No exog forecasting
- ✅ Uses only future exog rows that already exist in the design matrix artifact
- ✅ Fails fast if horizon exceeds available future rows

**Purpose**
- Establishes a *best-case ceiling* for SARIMAX-exog.
- If bridge performance does not beat univariate SARIMAX, exogs are not helping *even in theory*.

**Interpretation**
- Optimistic
- Non-deployable
- Scientifically useful as a control

---

### 4. Live Runner (Deploy-Faithful Evaluation)

**Module**
- `forecast/models/sarimax_exog/live_runner.py`

**Definition**
> “Given only information available at `data_asof`, how would this model behave in production?”

**Rules**
- ❌ No feature rebuild
- ❌ No use of future true exogs
- ✅ Deterministically forecasts future exogs (`exog_future` artifact)
- ✅ Writes both `exog_future` and predictions with full audit trail

**Purpose**
- Measures *realistic* performance.
- Mirrors production behavior exactly.

**Interpretation**
- Conservative
- Deployable
- The only valid estimate of live performance

---

### 5. Why Both Exist (Non-Optional Logic)

- **Bridge answers**: “Is this even worth pursuing?”
- **Live answers**: “What will actually happen in production?”

Rules:
- If BRIDGE ≤ univariate SARIMAX → stop (exogs are useless).
- If BRIDGE > univariate but LIVE ≤ univariate → exogs have signal but forecasting them destroys value.
- Only if LIVE > univariate is SARIMAX-exog production-worthy.

Bridge is a *gate*.  
Live is a *truth test*.

---

### 6. Backtests vs Live (Critical Distinction)

- Backtests answer: *“What would have happened if run at that time?”*
- Live answers: *“What will happen if run today?”*

Therefore:
- SARIMAX-exog backtests must respect **information constraints at anchor time**.
- Using bridge in backtests is allowed **only** to establish an upper bound, never as a deployment proxy.

---

### 7. SARIMAX-exog Evaluation Diagram: Bridge vs Live
  
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


## Selector Governance (Baseline, Non-Experimental)

The XGB selector enforces baseline feature-diversity rules.
These are considered Phase C defaults, not tunables:

- metric_pt_cap = 10
  (max base series per metric_id × property_type_id)

- min_non_redfin = 25
  (minimum non-Redfin features in final top-K)

- redfin_tier_caps = ON
  (tiered Redfin share enforcement)

Changes to these rules require:
- explicit versioning
- selector audit reruns
- documentation updates


### 8. Non-Negotiable Invariants

- Feature identity comes only from selector artifacts.
- Column order is enforced by `feature_ids` in audits.
- All artifacts are immutable and checksum-verified (sha256).
- No runner may silently regenerate features or reorder columns.

Violating any invariant invalidates comparisons across models.
