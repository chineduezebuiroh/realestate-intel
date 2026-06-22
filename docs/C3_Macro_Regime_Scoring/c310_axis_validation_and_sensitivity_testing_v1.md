# C3.10 — Axis Validation & Sensitivity Testing v1

## Purpose

The Axis Validation & Sensitivity Testing framework establishes how the Regime Engine will be evaluated, stress tested, and refined.

The objective is to ensure that:

* Axis outputs are economically reasonable
* Regime assignments are stable
* Weighting assumptions are defensible
* Model behavior remains interpretable

This framework governs validation rather than calculation.

---

# Design Philosophy

A key architectural principle of the Regime Engine is:

> A model should not be trusted simply because it produces plausible outputs.

All weighting decisions and regime assignments should be tested against both economic intuition and historical behavior.

Validation is intended to improve confidence, not maximize historical fit.

---

# Validation Hierarchy

The Regime Engine should be evaluated at four levels.

## Level 1 — Dimension Validation

Questions:

* Do individual dimensions behave as expected?
* Do dimension scores rise and fall logically through economic cycles?
* Do dimension outputs align with source data?

Examples:

* Demand should weaken during major recessions.
* Affordability should deteriorate during rapid price appreciation.
* Capital Markets should tighten during restrictive rate environments.

---

## Level 2 — Axis Validation

Questions:

* Do axis scores behave logically?
* Do component contributions make sense?
* Do axis weights produce interpretable results?

Examples:

* Strong demand and strong prices should generally produce higher Demand Axis scores.
* Rising inventory and permit activity should generally increase Supply Axis scores.

---

## Level 3 — Coordinate Validation

Questions:

* Do resulting coordinates align with economic intuition?
* Do coordinates move through the cycle logically over time?
* Are regime transitions reasonable?

Examples:

* Markets should generally move through adjacent phases.
* Large regime jumps should be uncommon and investigated.
* Regime paths should appear economically plausible.

---

## Level 4 — Regime Validation

Questions:

* Do assigned regimes match observed market conditions?
* Do major turning points align with historical events?
* Are forecasted regimes directionally reasonable?

Examples:

* Housing booms should generally appear within Expansion phases.
* Housing downturns should generally appear within Recession phases.
* Recovery periods should generally precede Expansion periods.

---

# Sensitivity Testing

Sensitivity testing evaluates how outputs respond to changes in assumptions.

---

## Weight Sensitivity

Test:

* Demand Axis weights
* Supply Axis weights
* Dimension weights
* Subcomponent weights

Questions:

* Does a small weight change create large output changes?
* Are outputs stable?

---

## Missing Data Sensitivity

Test:

* Feature removal
* Metric removal
* Dimension removal

Questions:

* Does the model remain stable?
* Does confidence appropriately decline?

---

## Geography Sensitivity

Test:

* National
* CBSA
* County

Questions:

* Do outputs remain interpretable across geographic scales?

---

## Time Horizon Sensitivity

Test:

* Different historical periods
* Different economic cycles
* Different recession environments

Questions:

* Are outputs robust across time?

---

# Validation Principles

## Principle 1

Economic logic is the primary validation standard.

---

## Principle 2

Interpretability is preferred over optimization.

---

## Principle 3

Stable outputs are preferred over highly reactive outputs.

---

## Principle 4

Backtesting informs decisions but does not dictate them.

---

## Principle 5

Avoid overfitting historical outcomes.

The goal is robust future performance.

---

# Governance Rules

1. Validation results should be documented.

2. Weight changes should be tested before implementation.

3. Significant methodology changes require re-validation.

4. Validation findings should be reproducible.

5. Version history must be maintained.

---

# Future Expansion

Potential future additions:

* Formal backtesting reports
* Historical regime studies
* Forecast regime validation
* Cross-market validation studies
* Statistical robustness testing

---

# Governance Notes

1. Validation is required before major methodology changes.

2. Sensitivity testing is intended to identify fragility.

3. Stability is a feature, not a flaw.

4. A model that explains its behavior is preferred to a model that merely predicts well.

5. Validation should improve confidence without sacrificing interpretability.

## Data History Constraint

Some sources, especially Redfin, have limited history relative to full real estate cycles.

As a result, validation should not rely solely on traditional long-horizon backtesting.

The v1 validation framework should combine:

- historical reasonableness checks
- known-event comparisons
- cross-market comparisons
- sensitivity testing
- limited backtesting where source history permits

Backtesting should be treated as one validation input rather than the sole standard of proof.
