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

Production policy:

level = MA6

short = MA6 / lag3(MA6) - 1

long = MA6 / lag12(MA6) - 1

Diagnostics performed

- chronology review
- volatility audit
- contribution audit
- cancellation audit
- production readiness challenge

Major findings

- substantial seasonality reduction
- materially lower downstream volatility
- acceptable turning-point responsiveness
- acceptable operational cost

Decision

PROMOTED TO PRODUCTION

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
