# ADR-001 — Feature Transform Governance

## Status

Accepted

---

## Context

The Regime Engine relies on engineered features that summarize latent economic conditions.

Multiple transform strategies may exist for a given metric.

Historically, transform selection relied on exploratory comparison.

As the project matured, a repeatable governance process became necessary.

---

## Decision

Production transform policies shall be promoted only through a Production Readiness Challenge.

Promotion decisions shall not be based on similarity to the incumbent.

Each challenger is evaluated against explicit economic objectives.

---

## Required Evaluation Categories

- Structural Fidelity
- Economic Responsiveness
- Regime Stability
- Interpretability
- Operational Cost

---

## Promotion Requirements

A challenger must satisfy all hard gates and meet the minimum overall readiness score.

If promoted:

- production registry updated
- ADR updated
- roadmap updated
- experiment frozen

---

## Consequences

Benefits

- deterministic governance
- reproducible promotion decisions
- documented rationale
- consistent methodology across all metrics

Trade-offs

- more experimentation before promotion
- additional diagnostic infrastructure

These trade-offs are accepted.
