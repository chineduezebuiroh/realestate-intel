# C3.09 — Dimension Weighting Framework v1

## Purpose

The Dimension Weighting Framework establishes how weights are defined, governed, tested, and modified throughout the Regime Engine.

The objective is to ensure that weighting decisions remain transparent, interpretable, reproducible, and resistant to overfitting.

---

# Design Philosophy

A key architectural principle of the Regime Engine is:

> Measure independently first. Combine later.

Weights exist at multiple levels of the scoring hierarchy and must be governed consistently.

Weighting decisions should prioritize:

* Economic logic
* Interpretability
* Stability
* Reproducibility

before optimization.

---

# Weight Hierarchy

The Regime Engine contains four levels of weighting.

## Level 1 — Feature Weights

Examples:

* Level
* Short-Term Change
* Long-Term Change

Feature weights are defined within individual dimension documents.

---

## Level 2 — Subcomponent Weights

Examples:

* Affordability:

  * Payment Burden
  * Price-to-Income

* Supply:

  * Inventory
  * Permit Activity
  * Construction Intensity

Subcomponent weights are defined within dimension documents.

---

## Level 3 — Dimension Weights

Examples:

Demand Axis:

* Demand Dimension
* Price Dimension
* Affordability Dimension

Supply Axis:

* Supply Dimension
* Capital Markets Dimension

Dimension weights are defined within axis construction documents.

---

## Level 4 — Future Composite Frameworks

Examples:

* Market Balance Diagnostics
* Market Comparison Engine
* Future Composite Scores

These frameworks may introduce additional weighting structures.

---

# Weighting Principles

## Principle 1 — Economic Logic First

Weights should initially be determined through economic reasoning rather than optimization.

---

## Principle 2 — Simplicity Preferred

Prefer simple weights whenever possible.

Examples:

* 50 / 50
* 75 / 25
* 80 / 20

Avoid unnecessary precision unless supported by evidence.

---

## Principle 3 — Interpretability

Every weight should have a documented rationale.

Future users should understand why a weight exists.

---

## Principle 4 — Stability

Weights should remain reasonably stable across:

* Markets
* Geographies
* Economic cycles
* Time periods

---

## Principle 5 — Avoid Overfitting

Weights should not be adjusted solely to improve historical results.

The objective is robust future performance rather than perfect historical fit.

---

# Weight Evolution Framework

## Version 1

Expert-defined weights.

Based on economic logic and architectural design.

---

## Version 2

Backtest-informed adjustments.

Historical performance may be used to refine weights.

---

## Version 3

Potential optimization.

Only considered if meaningful performance improvements can be demonstrated.

---

# Governance Rules

1. Every weight must have documented rationale.

2. Every weight change requires version control.

3. Weight changes must be backtested.

4. Historical outputs must remain reproducible.

5. Simplicity is preferred unless evidence supports complexity.

---

# Metadata Requirements

All score outputs should include:

* score_version
* weight_version
* calculation_timestamp

This ensures complete reproducibility.

---

# Governance Notes

1. Weights are assumptions, not facts.

2. Weights may evolve over time.

3. Transparency is more important than optimization.

4. Reproducibility is mandatory.

5. Economic logic remains the primary driver of weighting decisions.
