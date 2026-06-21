# C3.6 — Capital Markets Dimension v1

## Purpose

The Capital Markets Dimension measures the degree to which financing conditions support or constrain housing market activity.

It is intended to capture the cost and availability of capital rather than the affordability consequences experienced by households.

The objective is to answer:

> How supportive or restrictive are current financing conditions relative to historical experience?

---

# Core Question

The Capital Markets Dimension answers:

> How easy or difficult is it for capital to flow into housing markets?

This dimension does not attempt to measure:

* Housing affordability
* Demand directly
* Supply directly
* Transaction activity
* Price appreciation

Those concepts are captured by separate dimensions.

The Capital Markets Dimension measures financing conditions rather than housing market outcomes.

---

# Design Philosophy

A key architectural decision was made during Regime Engine design:

Capital Markets and Affordability are treated as separate concepts.

Capital Markets measures:

* Cost of capital
* Availability of capital
* Financial conditions

Affordability measures:

* Household purchasing power
* Housing payment burden
* Household housing accessibility

Capital Markets acts primarily as a market driver.

Affordability acts primarily as a market outcome.

Example:

Mortgage rates may rise immediately.

Capital Markets conditions deteriorate immediately.

Affordability deterioration may occur later as households respond to those higher financing costs.

Separating these dimensions allows the Regime Engine to distinguish between causes and consequences.

---

# Dimension Structure

The Capital Markets Dimension consists of three subcomponents:

1. Cost of Capital
2. Yield Curve Conditions
3. Credit Conditions

---

# Subcomponent 1 — Cost of Capital

## Purpose

Measures the direct cost of borrowing.

## Primary Sources

* FRED

## Candidate Metrics

* 30-Year Mortgage Rate
* 15-Year Mortgage Rate
* Effective Federal Funds Rate
* 10-Year Treasury Yield

## Candidate Features

* Cost of Capital Level
* Cost of Capital Short-Term Change
* Cost of Capital Long-Term Change

## Interpretation

Higher borrowing costs indicate more restrictive financing conditions.

Higher scores contribute positively to Capital Markets Restrictiveness.

---

# Subcomponent 2 — Yield Curve Conditions

## Purpose

Measures the broader macro-financial environment.

## Primary Sources

* FRED

## Candidate Metrics

* 2Y–10Y Spread
* 10Y–Fed Funds Spread
* Additional Yield Curve Measures

## Candidate Features

* Yield Curve Level
* Yield Curve Short-Term Change
* Yield Curve Long-Term Change

## Interpretation

Yield curve behavior provides information regarding future economic conditions and financing environments.

More restrictive conditions contribute positively to Capital Markets Restrictiveness.

---

# Subcomponent 3 — Credit Conditions

## Purpose

Measures the availability of capital.

## Initial Version

Placeholder only.

No implementation in v1.

Future candidates may include:

* Lending standards
* Credit spreads
* Bank lending conditions
* Mortgage credit availability indices

---

# Normalization

All features are normalized according to:

B2.2 — Normalization Framework

Output range:

-1.00 = Extremely Supportive Financing Conditions

0.00 = Neutral Financing Conditions

+1.00 = Extremely Restrictive Financing Conditions

All normalization is performed relative to historical experience.

No cross-market normalization is performed.

---

# Subcomponent Weighting

Initial Version

| Subcomponent           | Weight |
| ---------------------- | ------ |
| Cost of Capital        | 70%    |
| Yield Curve Conditions | 30%    |
| Credit Conditions      | 0%     |

Rationale:

The current dataset is dominated by interest-rate-related measures.

Credit Conditions remain reserved for future expansion.

---

# Feature Weighting Within Subcomponents

| Feature Type      | Weight |
| ----------------- | ------ |
| Level             | 40%    |
| Short-Term Change | 30%    |
| Long-Term Change  | 30%    |

Rationale:

Financing conditions are largely condition-based.

Current financing environments matter more than short-term fluctuations.

---

# Geography Coverage

Expected Coverage

## National

* Full coverage

## CBSA

* Derived from national capital market conditions

## County

* Derived from national capital market conditions

Capital Markets are currently treated as national conditions that influence all markets simultaneously.

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

The Capital Markets Dimension produces:

* Capital Markets Score
* Coverage Ratio
* Confidence Score
* Subcomponent Scores
* As-Of Date
* Score Version

---

# Interpretation

Higher Capital Markets Scores indicate more restrictive financing conditions.

Lower Capital Markets Scores indicate more supportive financing conditions.

The Capital Markets Dimension does not determine regime classification independently.

It acts as a supporting dimension during Axis Construction.

---

# Governance Notes

1. Capital Markets measures financing conditions rather than housing outcomes.

2. Capital Markets is not a substitute for Affordability.

3. Capital Markets acts primarily as a driver of future market conditions.

4. Affordability often reflects the downstream consequences of Capital Markets conditions.

5. Capital Markets may influence both Demand and Supply Axis construction.

6. Capital Markets remains a standalone dimension until Axis Construction determines how its influence is distributed.

7. Measure independently first. Combine later.
