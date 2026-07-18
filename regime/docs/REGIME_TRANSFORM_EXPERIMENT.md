# Regime Transform Experiment History

## Purpose

The purpose of this document is to record the evolution of feature transform policies used by the Regime Engine.

This document captures:
- hypotheses
- experiments
- diagnostics
- promotion decisions
- rejected approaches

It is intended to explain *why* production policies exist, not how they are implemented.

---

# Guiding Principles

1. The incumbent policy is **not** considered ground truth.
2. Candidate transforms are evaluated against economic objectives rather than similarity to existing outputs.
3. Promotion requires passing a deterministic Production Readiness Challenge.
4. Rejected experiments remain documented to prevent rediscovery.

---

# Inventory Experiments

## Original Production Policy

Raw monthly observations with traditional momentum features.

Issues observed:

- excessive seasonality
- unstable short-term movement
- noisy regime transitions

---

## Candidates Evaluated

- MA3 Momentum
- MA3 Deviation
- MA6 Structural
- MA9 Structural (research)
- MA12 Structural

---

## Major Findings

### MA3 Momentum

Pros

- responsive

Cons

- retained excessive seasonality

Decision

Research only.

---

### MA3 Deviation

Pros

- improved stability
- preserved shocks

Decision

Promoted as inventory finalist.

---

### MA6 Structural

Introduced a coherent structural definition:

level = MA6

short = MA6 / lag3(MA6) - 1

long = MA6 / lag12(MA6) - 1

Pros

- substantially reduced seasonality

Cons

- did not outperform MA3 deviation for inventory

Decision

Research only.

---

### MA12 Structural

Pros

- excellent seasonality suppression

Cons

- over-smoothed
- delayed structural response

Decision

Rejected.

---

# Labor Experiments

Candidate

LAUS MA6 Structural

Approved production contract:

level = MA6

short = MA6 / lag3(MA6) - 1

long = MA6 / lag12(MA6) - 1

Production implementation status

COMPLETE

Immutable production run ID

macro_regime_v1_bps120_laus_ma6

Experiment ID

production_laus_ma6

Immutable acceptance smoke test

scripts/smoke_tests/50_59/50_laus_ma6_immutable_acceptance.py

Acceptance artifact

artifacts/regime/comparisons/laus_ma6_immutable_acceptance/acceptance_summary.json

Readiness decision

PROMOTE_MA6

Readiness score

1.0

Failed hard gates

0

Final status

decision-approved, production-integrated, immutable-run verified, and acceptance-validated

Diagnostics performed

- chronology review
- volatility audit
- contribution audit
- cancellation audit
- production readiness challenge
- immutable acceptance validation

Historical evidence retained

- The frozen incumbent remains `macro_regime_v1_bps120_sources`.
- The MA3 control and MA6 challenger remain part of the labor experiment record.
- The chronology findings remain intact, including the observed 10-month maximum DC Demand-axis lag.
- The 10-month maximum DC Demand-axis lag is accepted within the forecast-regime architecture because the labor transform is used as a structural regime signal rather than a tactical nowcast trigger, and the downstream regime framework values reduced seasonal churn over immediate monthly sensitivity.

Immutable acceptance test verifies

- both immutable runs exist and pass artifact/hash verification;
- persisted LAUS production features match the approved MA6 formulas;
- non-LAUS raw features remain exactly unchanged;
- non-LAUS normalized features remain exactly unchanged;
- non-LAUS metric scores remain exactly unchanged;
- non-Demand dimensions remain exactly unchanged;
- non-Demand axes remain exactly unchanged;
- intended Demand, coordinate, and regime changes are reported rather than treated as failures.

Major findings

- substantial seasonality reduction
- materially lower downstream volatility
- acceptable turning-point responsiveness
- acceptable operational cost

Production closeout

- implementation is complete;
- immutable production run creation is complete;
- immutable acceptance validation is complete;
- Acceptance smoke test executed successfully against immutable production run macro_regime_v1_bps120_laus_ma6 on 2026-07-16;
- the LAUS MA6 transform is frozen as the production policy;
- future changes require a new Production Readiness Challenge.

Decision

PROMOTE_MA6

---

# Price Family Experiments

Implemented

- linked source substitution
- derived metric recomputation
- lineage preservation

Current status

Production Readiness Challenge pending.

---

# Production Readiness Framework

Every challenger must pass a Production Readiness Challenge.

Evaluation categories

- Structural Fidelity
- Economic Responsiveness
- Regime Stability
- Interpretability
- Operational Cost

Similarity to the incumbent is **not** a promotion criterion.

---

# Frozen Production Policies

Inventory

MA3 Deviation

Labor

MA6 Structural

Price Family

Pending

Combined Policy

Pending

---

# Price / Affordability PRC — Phase 1

Candidate

Linked Price Family MA12 Structural

Preferred candidate ID

`price_family_ma12_structural_linked`

Legacy implementation ID retained for compatibility

`price_family_ma12_momentum_lag3`

Implementation status

PHASE 1 COMPLETE — structural contract diagnostics only

Phase 1 scope completed

- validated the linked structural feature contract for `median_sale_price`, `median_ppsf`, `price_to_income`, and `payment_burden`;
- validated full-window MA12 behavior with no partial-window level values;
- validated same-state lag3 and lag12 feature formulas;
- validated linked derived recomputation from substituted MA12 `median_sale_price`;
- validated preservation of `median_household_income` and `mortgage_30y` inputs;
- validated derived lineage, exact component membership, component ages, duplicate-key protection, finite outputs, deterministic reruns, and row-level reproducibility examples;
- validated preferred-vs-legacy candidate identifier parity for comparable outputs;
- validated perturbation isolation for unrelated geographies and unrelated metrics;
- validated lazy public exports for the experiments package after the lazy-import change;
- wrote deterministic Phase 1 diagnostic artifacts.

Phase 1 smoke tests

- `scripts/smoke_tests/50_59/51_price_family_ma12_feature_contract.py`
- `scripts/smoke_tests/50_59/52_price_family_linked_derived_recalculation.py`

Phase 1 artifact locations

- `artifacts/regime/comparisons/price_family_ma12_structural_linked/phase1_feature_contract/`
- `artifacts/regime/comparisons/price_family_ma12_structural_linked/phase1_linked_recalculation/`

Formulas validated

For `median_sale_price` and `median_ppsf`:

```text
level = full-window trailing MA12(raw value)
short = level / lag3(level) - 1
long  = level / lag12(level) - 1
```

For `price_to_income`:

```text
level = substituted MA12 median_sale_price / preserved median_household_income
short = level / lag3(level) - 1
long  = level / lag12(level) - 1
```

For `payment_burden`:

```text
level = canonical mortgage payment from substituted MA12 median_sale_price,
        preserved median_household_income, and preserved mortgage_30y
short = level / lag3(level) - 1
long  = level / lag12(level) - 1
```

Remaining PRC phases

- isolation audit;
- economic responsiveness diagnostics;
- stability and seasonality diagnostics;
- chronology review;
- visual review packet;
- PRC scoring;
- immutable challenger run creation;
- immutable acceptance validation;
- registry promotion decision.

Production status

No production registry promotion occurred. No immutable challenger run was created. The candidate remains experimental until the remaining PRC phases are completed and accepted.
