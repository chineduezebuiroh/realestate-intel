# C3.05 — Price Dimension v1

## Purpose

The Price Dimension measures the degree of pricing pressure present within a market.

It is intended to capture how strongly demand and supply conditions are expressing themselves through home prices.

The objective is to answer:

> How strongly are market conditions translating into price appreciation or depreciation relative to this market's own historical experience?

---

# Core Question

The Price Dimension answers:

> How much pricing pressure exists within this market?

This dimension does not attempt to measure:

* Demand directly
* Supply directly
* Affordability directly

Those concepts are captured by separate dimensions.

The Price Dimension measures market outcomes rather than market drivers.

---

# Design Philosophy

A key architectural decision was made during Regime Engine design:

Demand and Price are treated as separate concepts.

Demand measures underlying economic and demographic drivers.

Price measures how those drivers are expressing themselves through housing values.

Examples:

Strong demand may produce only modest price growth if supply is abundant.

Strong demand may produce significant price growth if supply is constrained.

Separating Demand from Price allows the Regime Engine to distinguish between market drivers and market outcomes.

---

# Dimension Structure

The Price Dimension consists of two subcomponents:

1. Sale Price Pressure
2. Price-Per-Square-Foot Pressure

---

# Subcomponent 1 — Sale Price Pressure

## Purpose

Measures changes in overall housing prices.

## Primary Source

* Redfin

## Candidate Metric

* Median Sale Price

## Candidate Features

* Median Sale Price Level
* Median Sale Price Short-Term Change
* Median Sale Price Long-Term Change

## Interpretation

Higher values indicate stronger pricing pressure.

Lower values indicate weaker pricing pressure.

---

# Subcomponent 2 — Price-Per-Square-Foot Pressure

## Purpose

Measures pricing pressure normalized for property size.

## Primary Source

* Redfin

## Candidate Metric

* Median Price Per Square Foot

## Candidate Features

* Median PPSF Level
* Median PPSF Short-Term Change
* Median PPSF Long-Term Change

## Interpretation

Higher values indicate stronger pricing pressure.

Lower values indicate weaker pricing pressure.

---

# Normalization

All features are normalized according to:

B2.2 — Normalization Framework

Output range:

-1.00 = Extremely Weak Pricing Pressure

0.00 = Neutral Pricing Pressure

+1.00 = Extremely Strong Pricing Pressure

All normalization is performed relative to the market's own historical experience.

No cross-market normalization is performed.

---

# Subcomponent Weighting

Initial Version

| Subcomponent      | Weight |
| ----------------- | ------ |
| Median Sale Price | 50%    |
| Median PPSF       | 50%    |

Weights may be revised after backtesting.

---

# Feature Weighting Within Subcomponents

| Feature Type      | Weight |
| ----------------- | ------ |
| Level             | 20%    |
| Short-Term Change | 40%    |
| Long-Term Change  | 40%    |

Rationale:

Price is primarily a momentum-oriented dimension.

The direction and speed of price movement are generally more informative than the absolute level of prices.

---

# Geography Coverage

Expected Coverage

## CBSA

* Median Sale Price
* Median PPSF

## County

* Median Sale Price
* Median PPSF

Coverage depends on Redfin availability.

---

# Missing Data Handling

If a feature is unavailable:

* Remove feature
* Re-normalize remaining weights

If a subcomponent is unavailable:

* Remove subcomponent
* Re-normalize remaining weights

Reduce confidence score accordingly.

No imputation in v1.

---

# Output Contract

The Price Dimension produces:

* Price Dimension Score
* Coverage Ratio
* Confidence Score
* Subcomponent Scores
* As-Of Date
* Score Version

---

# Interpretation

Higher Price Dimension Scores indicate stronger pricing pressure.

Lower Price Dimension Scores indicate weaker pricing pressure.

The Price Dimension does not determine regime classification independently.

It acts as a supporting dimension during Demand Axis construction.

---

# Governance Notes

1. Price Dimension measures market outcomes rather than market drivers.

2. Price Dimension is not a substitute for Demand.

3. Demand can strengthen before prices respond.

4. Prices can continue rising after Demand begins weakening.

5. Price Dimension helps capture the degree to which market conditions are expressing themselves through appreciation or depreciation.

6. Final influence on regime placement is determined during Axis Construction.

7. Price Dimension is expected to contribute to the Demand Axis but does not define the Demand Axis independently.
