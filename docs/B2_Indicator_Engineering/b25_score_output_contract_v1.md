# B2.5 Score Output Contract v1

## Purpose

This document defines the standardized outputs produced by the Indicator Engineering Layer.

The goal is to ensure all downstream systems consume a consistent score structure regardless of:

* indicator source
* geography
* property type
* update frequency
* feature availability

This document serves as the interface between:

```text
Phase B — Indicator Engineering

and

Phase C — Regime Engine
```

---

# Design Principles

## Principle 1 — Scores Must Be Comparable

All dimension scores must be normalized onto a common scale.

Target range:

```text
-1.00 to +1.00
```

where:

```text
+1.00 = extremely supportive

 0.00 = neutral

-1.00 = extremely unsupportive
```

relative to the metric's own historical behavior.

---

## Principle 2 — Geography Independence

Scores must be interpretable regardless of geography.

Examples:

* National
* Region
* State
* CBSA
* County

A score of:

```text
Demand = +0.80
```

should carry the same interpretation across all geographies.

---

## Principle 3 — Property Type Independence

Macro regime scoring uses:

```text
property_type = all
```

Local market scoring may use:

```text
sfh
condo
townhome
multifamily
```

The output schema remains identical.

---

## Principle 4 — Missing Data Tolerance

Scores must remain computable when indicators are unavailable.

Example:

County may lack CES payroll data.

The engine should:

* recalculate weights
* preserve score validity
* record reduced confidence

rather than fail.

---

# Dimension Outputs

The Indicator Engineering Layer produces six primary dimensions.

## Demand Score

Captures:

* employment growth
* payroll growth
* labor force growth
* population growth
* migration
* GDP growth

Interpretation:

```text
High = strengthening demand

Low = weakening demand
```

---

## Supply Score

Captures:

* inventory growth
* permit activity
* housing starts
* construction intensity

Interpretation:

```text
High = increasing supply pressure

Low = constrained supply
```

Note:

High supply is not inherently bullish or bearish.

Interpretation depends on demand conditions.

---

## Affordability Score

Captures:

* payment burden
* price-to-income
* rent burden

Interpretation:

```text
High = improving affordability

Low = worsening affordability
```

---

## Liquidity Score

Captures:

* days on market
* transaction volume
* sale-to-list ratio

Interpretation:

```text
High = liquid market

Low = illiquid market
```

---

## Price Score

Captures:

* price growth
* price acceleration
* PPSF growth

Interpretation:

```text
High = strong pricing power

Low = weak pricing power
```

---

## Capital Markets Score

Captures:

* mortgage rates
* Treasury rates
* spreads
* credit conditions

Interpretation:

```text
High = supportive financing environment

Low = restrictive financing environment
```

---

# Dimension Output Schema

Each dimension produces:

```text
dimension_name

dimension_score

feature_count

expected_feature_count

coverage_ratio

confidence_score
```

---

## Example

Demand Score:

```json
{
  "dimension": "demand",
  "score": 0.72,
  "feature_count": 5,
  "expected_feature_count": 6,
  "coverage_ratio": 0.83,
  "confidence_score": 0.91
}
```

---

# Coverage Ratio

Definition:

```text
available_features
/
expected_features
```

Range:

```text
0.00 to 1.00
```

Examples:

```text
6 / 6 = 1.00

5 / 6 = 0.83

4 / 6 = 0.67
```

Purpose:

Tracks completeness of the score.

---

# Confidence Score

Definition:

Measure of score reliability.

Influenced by:

* feature coverage
* history length
* data freshness
* publication lag
* source quality

Range:

```text
0.00 to 1.00
```

Interpretation:

```text
0.90+ = high confidence

0.70–0.90 = moderate confidence

<0.70 = caution
```

---

# Composite Score Package

Every geography produces:

```text
Demand Score
Supply Score
Affordability Score
Liquidity Score
Price Score
Capital Markets Score
```

plus:

```text
overall_coverage_ratio

overall_confidence_score
```

---

# Explicit Non-Goals

This layer does NOT:

* classify regimes
* determine cycle phase
* calculate wheel position
* forecast transitions
* rank markets

Those responsibilities belong to later phases.

---

# Downstream Consumers

Phase C:

```text
Regime Engine
```

Consumes:

```text
Demand
Supply
Affordability
Liquidity
Price
Capital Markets
```

and transforms them into:

```text
Demand Axis
Supply Axis
```

for cycle-wheel positioning.

---

Phase D:

```text
Market Comparison Engine
```

Consumes:

```text
Dimension Scores
Axis Scores
Regime Outputs
```

for cross-market comparison and visualization.

---

# Future Enhancements

Potential additions:

* score stability metrics
* feature attribution
* leading indicator weights
* forecasted dimension scores
* score explainability outputs

These are intentionally deferred from v1.
