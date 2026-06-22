# C3.08 — Supply Axis Construction v1

## Purpose

The Supply Axis measures overall housing supply pressure within a housing market.

It combines multiple dimensions into a single score representing the degree to which supply conditions contribute to housing market imbalance.

The objective is to answer:

> How much supply pressure exists relative to this market's historical experience?

---

# Design Philosophy

The Supply Axis is intended to measure housing supply pressure rather than housing demand or housing outcomes.

A key architectural decision was made during Regime Engine design:

Supply is influenced by both observed supply conditions and financing conditions.

Supply Dimension provides the primary signal.

Capital Markets provides a secondary signal.

Other dimensions are excluded.

---

# Axis Construction Framework

The Supply Axis consists of:

1. Supply Dimension
2. Capital Markets Dimension

---

# Component Roles

## Supply Dimension

Role:

Primary supply signal.

Measures:

* Active inventory
* Permit activity
* Construction intensity

Answers:

> How much housing supply pressure exists?

---

## Capital Markets Dimension

Role:

Supply formation signal.

Measures:

- Cost of capital
- Financing conditions
- Yield curve conditions

Answers:

> How supportive are financing conditions for future housing creation?

Capital Markets influence supply indirectly through development feasibility and construction activity.

More supportive financing conditions increase housing production capacity.

More restrictive financing conditions reduce housing production capacity.

Capital Markets contribute directly to Supply Axis construction.

---

# Initial Weighting Framework

| Component                 | Weight |
| ------------------------- | ------ |
| Supply Dimension          | 85%    |
| Capital Markets Dimension | 15%    |

Initial weights are intentionally conservative.

Observed supply conditions remain the dominant signal.

Capital Markets provide contextual adjustment.

Weights may be revised after backtesting.

---

# Calculation Framework

Step 1:

Compute normalized dimension scores.

Inputs:

* Supply Score
* Capital Markets Score

Step 2:

Apply weighted combination.

Supply Axis Score =
(0.85 × Supply Score)
+
(0.15 × Capital Markets Score)

The resulting Supply Axis Score represents the X-coordinate used within the Regime Geometry framework.

---

# Interpretation

+1.00

Extremely high supply pressure.

0.00

Neutral supply pressure.

-1.00

Extremely low supply pressure.

---

# Excluded Dimensions

The following dimensions are intentionally excluded:

## Demand

Demand influences the Demand Axis.

## Affordability

Affordability influences housing participation rather than supply pressure.

## Transaction Activity

Transaction Activity measures market participation rather than supply creation.

## Price

Price measures market outcomes rather than supply pressure.

---

# Missing Data Handling

If a component score is unavailable:

* Remove component
* Re-normalize remaining weights

Reduce confidence score accordingly.

---

# Output Contract

The Supply Axis produces:

* Supply Axis Score
* Component Contributions
* Coverage Ratio
* Confidence Score
* Score Version
* As-Of Date

---

# Governance Notes

1. Supply Axis measures housing supply pressure.

2. Supply Dimension remains the dominant signal.

3. Capital Markets measure financing support for future housing creation.

4. More supportive Capital Markets increase Supply Axis strength.

5. More restrictive Capital Markets reduce Supply Axis strength.

6. Supply Axis Score becomes the X-coordinate within the Regime Geometry framework.

7. Measure independently first. Combine later.
