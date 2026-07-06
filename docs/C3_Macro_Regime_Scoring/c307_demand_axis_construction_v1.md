# C3.07 — Demand Axis Construction v1

## Purpose

The Demand Axis measures overall housing demand strength within a housing market.

It combines multiple dimensions into a single score representing the degree to which demand conditions support housing market expansion.

The objective is to answer:

> How strong is housing demand relative to this market's historical experience?

---

# Design Philosophy

The Demand Axis is intended to measure effective housing demand rather than housing outcomes alone.

A key architectural decision was made during Regime Engine design:

Demand is influenced by multiple dimensions.

Demand Dimension provides the primary signal.

Price and Affordability provide secondary signals.

Transaction Activity is excluded from axis construction and evaluated separately.

---

# Axis Construction Framework

The Demand Axis consists of:

1. Demand Dimension
2. Price Dimension
3. Affordability Dimension
4. Capital Markets Dimension

Each component contributes independently.

---

# Component Roles

## Demand Dimension

Role:

Primary demand driver.

Measures:

- Employment growth
- Population growth
- GDP growth
- Labor force growth
- Unemployment conditions

Answers:

> Why do people want housing?

---

## Price Dimension

Role:

Secondary confirmation signal.

Measures:

- Price appreciation
- PPSF appreciation

Answers:

> How strongly are demand conditions expressing themselves through prices?

Price is treated as an outcome rather than a direct driver.

However, strong price appreciation often reflects underlying demand strength.

---

## Affordability Dimension

Role:

Demand participation signal.

Measures:

- Housing payment burden
- Price-to-income relationships

Answers:

> Can households realistically participate in the market?

Higher affordability supports demand.

Lower affordability constrains demand.

Affordability contributes directly to Demand Axis construction.

---

## Capital Markets Dimension

Role:

Demand participation signal.

Measures:

- lending availablity
- tbd...

Answers:

> Can households realistically participate in the market?

Higher access to lending and debt markets supports demand.

Lower access to lending and debt markets constrains demand.

Lending / debt-market access contributes directly to Demand Axis construction.

---

# Initial Weighting Framework

| Component                 | Weight |
| ------------------------- | ------ |
| Demand Dimension          | 65%    |
| Price Dimension           | 15%    |
| Affordability Dimension   | 10%    |
| Capital Markets Dimension | 10%    |

Initial weights are intentionally conservative.

Demand remains the dominant signal.

Price and Affordability provide contextual adjustments.

Weights may be revised after backtesting.

---

# Calculation Framework

Step 1:

Compute normalized dimension scores.

Inputs:

- Demand Score
- Price Score
- Affordability Score
- Capital Markets Score

Step 2:

Apply weighted combination.

Demand Axis Score =
(0.65 × Demand Score)
+
(0.15 × Price Score)
+
(0.10 × Affordability Score)
+
(0.10 × Capital Markets Score)

The resulting Demand Axis Score represents the Y-coordinate used within the Regime Geometry framework.

---

# Interpretation

Higher scores indicate stronger demand conditions.

Lower scores indicate weaker demand conditions.

Examples:

+1.00

Extremely strong demand conditions.

0.00

Neutral demand conditions.

-1.00

Extremely weak demand conditions.

---

# Excluded Dimensions

The following dimensions are intentionally excluded:

## Supply

Supply influences the Supply Axis.

## Transaction Activity

Transaction Activity measures participation rather than demand.

It is evaluated separately.

## Capital Markets

Capital Markets influence both Demand and Supply indirectly.

Its role is determined during later Axis Calibration.

Capital Markets are excluded from v1 Demand Axis construction.

---

# Missing Data Handling

If a component score is unavailable:

- Remove component
- Re-normalize remaining weights

Example:

Demand unavailable:

Price = 60%

Affordability = 40%

Confidence score should be reduced accordingly.

---

# Output Contract

The Demand Axis produces:

- Demand Axis Score
- Component Contributions
- Coverage Ratio
- Confidence Score
- Score Version
- As-Of Date

---

# Governance Notes

1. Demand Axis measures effective housing demand.

2. Demand Axis combines demand drivers and demand participation constraints.

3. Demand remains the dominant signal.

4. Price acts as a confirmation signal.

5. Affordability measures participation capacity and contributes directly to Demand Axis construction.

6. Transaction Activity is intentionally excluded.

7. Capital Markets influence is evaluated separately during Axis Construction calibration.

8. Measure independently first. Combine later.
