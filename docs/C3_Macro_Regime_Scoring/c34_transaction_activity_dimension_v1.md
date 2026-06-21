# C3.4 — Transaction Activity Dimension v1

## Purpose

The Transaction Activity Dimension measures the level of housing market activity occurring within a market.

It is intended to capture participation and transaction throughput rather than market balance.

The objective is to answer:

> How actively are market participants transacting relative to this market's own historical experience?

---

# Core Question

The Transaction Activity Dimension answers:

> How much market activity is occurring?

This dimension does not attempt to determine:

* Buyer leverage
* Seller leverage
* Market equilibrium
* Supply-demand balance

Those concepts are handled separately within the Market Balance Diagnostic Layer.

---

# Design Philosophy

A key architectural decision was made during Regime Engine design:

Transaction Activity is treated as an independent dimension.

Market Balance metrics are not included in this dimension.

Specifically:

Included:

* Transaction Volume
* Pending Activity

Excluded:

* Days on Market (DOM)
* Sale-to-List Ratio
* Months of Supply

Rationale:

DOM, Sale-to-List Ratio, and Months of Supply are viewed as observed consequences of supply-demand imbalance rather than independent forces driving market cycles.

These metrics will be evaluated separately within the Market Balance Diagnostic Layer.

---

# Dimension Structure

The Transaction Activity Dimension consists of two subcomponents:

1. Closed Transaction Activity
2. Pending Transaction Activity

---

# Subcomponent 1 — Closed Transaction Activity

## Purpose

Measures completed transaction volume.

## Primary Source

* Redfin

## Candidate Metrics

* Homes Sold

## Candidate Features

* Homes Sold Level
* Homes Sold Short-Term Change
* Homes Sold Long-Term Change

## Interpretation

Higher values indicate greater transaction activity.

---

# Subcomponent 2 — Pending Transaction Activity

## Purpose

Measures near-term transaction pipeline activity.

## Primary Source

* Redfin

## Candidate Metrics

* Pending Sales

## Candidate Features

* Pending Sales Level
* Pending Sales Short-Term Change
* Pending Sales Long-Term Change

## Interpretation

Higher values indicate stronger transaction activity.

Pending activity may provide earlier signals than completed sales.

---

# Normalization

All features are normalized according to:

B2.2 — Normalization Framework

Output range:

-1.00 = Extremely Low Activity

0.00 = Neutral Activity

+1.00 = Extremely High Activity

All normalization is performed relative to the market's own historical experience.

No cross-market normalization is performed.

---

# Subcomponent Weighting

Initial Version

| Subcomponent                 | Weight |
| ---------------------------- | ------ |
| Closed Transaction Activity  | 50%    |
| Pending Transaction Activity | 50%    |

Weights may be revised after backtesting.

---

# Feature Weighting Within Subcomponents

| Feature Type      | Weight |
| ----------------- | ------ |
| Level             | 25%    |
| Short-Term Change | 35%    |
| Long-Term Change  | 40%    |

Rationale:

Transaction activity is both a current condition and a momentum signal.

Recent acceleration or deceleration is important.

---

# Geography Coverage

Expected Coverage

## CBSA

* Homes Sold
* Pending Sales

## County

* Homes Sold
* Pending Sales

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

The Transaction Activity Dimension produces:

* Transaction Activity Score
* Coverage Ratio
* Confidence Score
* Subcomponent Scores
* As-Of Date
* Score Version

---

# Interpretation

Higher Transaction Activity Scores indicate greater market participation and throughput.

Lower Transaction Activity Scores indicate reduced market participation and throughput.

This dimension does not determine market balance.

It measures activity only.

---

# Governance Notes

1. High activity does not imply strong demand.

2. Low activity does not imply weak demand.

3. High activity can occur in both Expansion and Hypersupply.

4. Low activity can occur in both Recovery and Recession.

5. Market balance is evaluated separately.

6. Transaction Activity is treated as a supporting dimension within the Macro Regime Engine.
