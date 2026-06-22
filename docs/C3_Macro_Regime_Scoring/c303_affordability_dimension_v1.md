# C3.03 — Affordability Dimension v1

## Purpose

The Affordability Dimension measures the degree of affordability pressure present within a market.

It is a supporting dimension used by the Macro Regime Engine.

The objective is to quantify how difficult it is becoming for households to participate in the housing market.

---

# Core Question

The Affordability Dimension answers:

> How severe is affordability pressure relative to this market's own historical experience?

---

# Affordability Dimension Structure

The Affordability Dimension consists of three subcomponents:

1. Price-to-Income
2. Payment Burden

---

# Subcomponent 1 — Price-to-Income

## Purpose

Measures home prices relative to household income.

## Primary Sources

* Redfin
* Census ACS

## Candidate Metric

Median Sale Price ÷ Median Household Income

## Candidate Features

* Price-to-Income Level
* Price-to-Income Short-Term Change
* Price-to-Income Long-Term Change

## Interpretation

Higher values indicate worsening affordability.

Higher scores contribute positively to Affordability Pressure.

---

# Subcomponent 2 — Payment Burden

## Purpose

Measures ownership affordability.

## Primary Sources

* Redfin
* Mortgage Rates
* Income Data

## Candidate Metric

Estimated Monthly Housing Payment ÷ Monthly Household Income

Housing Payment may include:

* Principal
* Interest
* Taxes
* Insurance

Initial implementation may use principal and interest only.

## Candidate Features

* Payment Burden Level
* Payment Burden Short-Term Change
* Payment Burden Long-Term Change

## Interpretation

Higher values indicate worsening affordability.

Higher scores contribute positively to Affordability Pressure.

---

# Normalization

All features are normalized using the standard framework defined in:

B2.2 — Normalization Framework

Output range:

-1.00 = Extremely Low Affordability Pressure

0.00 = Neutral

+1.00 = Extremely High Affordability Pressure

All normalization is performed relative to the market's own history.

No cross-market normalization is performed.

---

# Subcomponent Weighting

Initial Version:

| Subcomponent    | Weight |
| --------------- | ------ |
| Price-to-Income | 50%    |
| Payment Burden  | 50%    |

Rationale:

Ownership affordability is expected to drive the majority of cycle dynamics in the initial version.

Weights may be revised after backtesting.

---

# Feature Weighting Within Subcomponents

| Feature Type      | Weight |
| ----------------- | ------ |
| Level             | 50%    |
| Short-Term Change | 20%    |
| Long-Term Change  | 30%    |

Rationale:

Affordability is primarily a condition variable.

Current affordability matters more than short-term movement.

---

# Geography Coverage

Expected Coverage:

## CBSA

* Price-to-Income
* Payment Burden

## County

* Price-to-Income
* Payment Burden

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

The Affordability Dimension produces:

* Affordability Dimension Score
* Coverage Ratio
* Confidence Score
* Subcomponent Scores
* As-Of Date
* Score Version

---

# Interpretation

Higher Affordability Dimension Scores indicate greater affordability pressure.

Lower Affordability Dimension Scores indicate more affordable market conditions.

The Affordability Dimension does not determine regime classification independently.

It influences axis construction during the Regime Engine process.

---

# Governance Notes

1. Affordability Pressure is not equivalent to home price appreciation.

2. Affordability Pressure is not equivalent to demand.

3. Strong demand can coexist with high affordability pressure.

4. Weak demand can coexist with low affordability pressure.

5. Affordability acts as a structural constraint on future demand strength.

6. Final influence on regime placement is determined during Dimension-to-Axis Mapping.

7. Price-to-Income = Structural Affordability | Payment Burden = Financing Affordability

Future Version Consideration:
Rental affordability may be incorporated if a dedicated rental-market framework is introduced.
