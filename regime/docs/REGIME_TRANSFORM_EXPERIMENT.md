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
