# forecast/INVENTORY.md (Phase C — Post Phase-B Freeze)

Purpose: authoritative map of canonical entrypoints + responsibilities.

Rule: if it's not listed as canonical, assume it's legacy / shim / internal.

Rule: root-level forecast/*.py is either shared utilities or shims; authoritative
model logic lives under forecast/models/**.

Last Updated: After canonical exog v09.0 freeze + bridge diagnostic sweep.

---

#### Phase C re-org objectives (NON-NEGOTIABLE)

- **Single source of truth for runners:** authoritative execution logic lives only under `forecast/models/**`.
- **Legacy files are shims only:** legacy modules may import-and-call canonical runners, but must not contain unique logic.
- **No god files:** any file that mixes orchestration + model fitting + artifact IO + DB writes must be split.
- **Functional grouping:** shared logic must live in one of:
  - `forecast/features/`
  - `forecast/design_matrix/`
  - `forecast/time/`
  - `forecast/artifacts/`
  - `forecast/persistence/`
  - `forecast/contracts/`
- Hard contracts tested: contracts at artifact boundaries must have smoke tests.


## 🔒 Phase B Freeze State (CRITICAL)
---
The following are frozen and must remain true unless a version bump explicitly states otherwise:

**1. Canonical Exogenous Identity**
- Stability version: `v09.0`
- No `_lag0` allowed.
- Allowed leads: `{1, 3, 6, 12}`
- Canonical exog set built via:
  - `forecast/selection/stability_rank.py`
  - `forecast/selection/build_canonical_exog_set.py`

**2. As-Of Leakage Eliminated**
- Evaluation joins must enforce:
  - `target_date <= data_asof`
- No evaluation may use future actuals beyond run’s `data_asof`.
- This rule is enforced in `forecast/eval/core.py`.

**3. Bridge Backtest = Short-Horizon Diagnostic**
- `bridge_runner.py` evaluates:
  - Fit at anchor
  - Predict next H (currently 3)
- Purpose:
  - Test exog marginal signal
  - NOT full production horizon

**4. Empirical Finding from K Sweep**

Using 9 anchors:

- K=5 is the only stable regime.
- K ≥ 15 produces:
  - Severe collinearity
  - Condition numbers ≈ 1e16
  - Rank deficiency
  - Performance degradation

Conclusion:
- Exog must be dimension-controlled.
- Selector must optimize for marginal contribution + diversity.
- SARIMAX must enforce parameter adequacy gates.

---
## 🛑 New Enforcement (Phase C Hardening)

**Policy B — Obs-to-Parameter Adequacy (MANDATORY)**
To be implemented in sarimax_exog runners:

Reject run if:
- `n_obs / (n_exogs + arima_param_count) < 5`
- OR `condition_number > 1e12`
- OR `exog_rank < n_exogs`

Runs failing these checks:
- Must record failure artifact.
- Must not write forecast_predictions rows.

---
## Canonical runner call graphs (MUST STAY TRUE)
(unchanged, but now with bridge context)

**XGB Selector (artifact-only)**

`forecast/models/xgb/backtest_selector_runner.py`

Responsibilities:
- load target series
- choose anchors
- build universal candidate specs
- Stage 1 scoring (cheap_lift default)
- diversity governance caps
- incremental design matrix build
- XGB fit
- enforce K + min_non_redfin
- emit:
  - `selected_features__anchor=...`
  - `selector_summary__anchor=...`
  - `candidate_scores__anchor=...`

---
**SARIMAX Exog — Bridge Diagnostic**

`forecast/models/sarimax_exog/bridge_runner.py`

Purpose:
- Test canonical exog marginal signal
- Fit short horizon only

Must:
- Emit fit diagnostics:
  - condition_number
  - rank
  - smallest singular value
- Enforce Policy B (Phase C)

---
**SARIMAX Exog — Backtest / Live**

Same structure, but:
- May use longer horizon
- Must share adequacy enforcement

---
## XGB Selector Governance (CURRENT DEFAULTS)

- `metric_pt_cap = 10`
- `min_non_redfin = 25`
- Redfin tier caps enforced
- Lead months allowed: `(1, 3, 6, 12)`
- `_lag0` prohibited
- Selector horizon: short (1–3 months)

---
## 🔄 Selector Re-Architecture (Phase C Direction — D)

The selector will evolve from:
> Independent feature ranking

to:
> Marginal predictive lift under diversity + conditioning governance.

Planned changes:
1. Stage 1:
   - Cheap predictive lift (already default)
2. Stage 2:
  - Greedy forward selection:
    - Add feature only if incremental lift > ε
3. Diversity constraints:
  - Family caps
  - Metric caps
  - Geography caps (future)
4. Collinearity awareness:
  - Evaluate on final complete-case mask
  - Record redundancy diagnostics

---
## Data Source Status

Refreshed:
- Redfin
- CES
- LAUS
- FRED macro
- Census NRC
- BEA QGDP

Pending:
- Census BPS permits refresh

---
## Roadmap Items (ACTIVE — DO NOT DROP)

- Implement Policy B in SARIMAX runners
- Marginal-lift selector refactor
- Candidate ordering bias mitigation
- Centralize index normalization
- Refactor forecast folder into functional groupings
- Dynamic AGG_POLICY
- Future exog policy (Contract C evaluation)
- Dual-horizon selector concept (deferred)
- Structured feature_id columns (Phase C hardening)
- Strict batch_id/data_asof (no fallback)

---
## Feature ID Contract (LOCKED)

Canonical format:
`<metric_id>__<geo_id>__<property_type_id>__<source_id>_lag<k>`

Allowed k:
`1, 3, 6, 12`

No lag0 permitted anywhere in system.

Downstream parsing depends on this string contract.
