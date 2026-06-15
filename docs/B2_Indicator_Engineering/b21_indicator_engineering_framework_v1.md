# Indicator Engineering Framework v1

## Purpose

This document defines the standardized feature engineering framework used to transform raw indicators into comparable signals for scoring, ranking, and regime classification.

The objective is to create a consistent methodology that can be applied across all indicators regardless of source, geography, or reporting frequency.

This framework serves as the bridge between:

* B1. Indicator Taxonomy
* Phase C. Regime Engine

---

# Core Philosophy

Raw indicators are not directly comparable.

Every indicator must first be transformed into a common feature set before entering the scoring framework.

Examples:

* Employment
* Labor Force
* Unemployment
* Population
* GDP
* Inventory
* Mortgage Rates
* Median Sale Price
* Days on Market

All indicators will generate the same family of engineered features.

---

# Standard Feature Set

Each indicator generates three primary features.

## 1. Level

Current value of the indicator.

Examples:

* Employment = 1,250,000
* Unemployment Rate = 4.2%
* Inventory = 3,500 homes
* GDP = $425 billion
* Median Sale Price = $650,000

Purpose:

Measure current conditions.

---

## 2. Short-Term Change

Measures recent momentum.

Examples:

* Month-over-Month Change
* Quarter-over-Quarter Change
* Annual Change

Selection depends on source frequency.

Purpose:

Measure recent acceleration or deceleration.

---

## 3. Long-Term Change

Measures broader trend direction.

Examples:

* Year-over-Year Change
* Multi-Year Growth Rate

Selection depends on source frequency.

Purpose:

Measure structural trend strength.

---

# Canonical Outputs

Every indicator generates six standardized outputs.

## Output Set

1. Level
2. Level Percentile
3. Short-Term Change
4. Short-Term Change Percentile
5. Long-Term Change
6. Long-Term Change Percentile

This structure applies to every indicator regardless of source.

Example:

Employment

* Level
* Level Percentile
* MoM Change
* MoM Percentile
* YoY Change
* YoY Percentile

GDP

* Level
* Level Percentile
* QoQ Change
* QoQ Percentile
* YoY Change
* YoY Percentile

Population

* Level
* Level Percentile
* Annual Change
* Annual Change Percentile
* Multi-Year Change
* Multi-Year Change Percentile

The default multi-year comparison window will be determined during implementation based on historical data availability and indicator characteristics.
---

# Frequency Mapping

## Monthly Indicators

Examples:

* Employment
* Unemployment
* Inventory
* DOM
* Sale-to-List Ratio
* Mortgage Rates

Features:

* Level
* Month-over-Month Change
* Year-over-Year Change

---

## Quarterly Indicators

Examples:

* BEA Quarterly GDP

Features:

* Level
* Quarter-over-Quarter Change
* Year-over-Year Change

---

## Annual Indicators

Examples:

* Population
* Annual GDP
* Median Household Income

Features:

* Level
* Annual Change
* Multi-Year Change

The default multi-year comparison window will be determined during implementation based on historical data availability and indicator characteristics.
Future versions may allow configurable multi-year windows.

---

# Historical Percentile Methodology

Every engineered feature will be converted into a historical percentile rank.

Examples:

* Employment Level Percentile
* Employment YoY Percentile
* Inventory MoM Percentile
* GDP QoQ Percentile

Percentiles will be calculated relative to the indicator's own historical distribution.

Cross-market normalization is intentionally deferred to the Normalization Framework.

---

# Indicator Classes

## Class A — Cyclical Indicators

Examples:

* Unemployment
* Inventory
* DOM
* Mortgage Rates
* Sale-to-List Ratio

Characteristics:

* Frequently mean-reverting
* Respond strongly to economic cycles
* Level is highly informative

For cyclical indicators:

* Level receives meaningful weight
* Short-Term Change receives meaningful weight
* Long-Term Change receives meaningful weight

---

## Class B — Structural Growth Indicators

Examples:

* Population
* GDP
* Income
* Median Sale Price
* Median PPSF

Characteristics:

* Tend to trend upward over long horizons
* Growth rates are generally more informative than absolute levels

For structural indicators:

* Short-Term Change receives meaningful weight
* Long-Term Change receives meaningful weight
* Level remains available but may receive lower weight in future scoring frameworks

Weighting decisions are deferred to the Regime Scoring Framework.

---

# Native Frequency Principle

Engineered features shall be calculated using the indicator's native reporting frequency.

Examples:

* Monthly indicators use MoM and YoY calculations.
* Quarterly indicators use QoQ and YoY calculations.
* Annual indicators use Annual Change and 5-Year Change calculations.

The platform shall not manufacture synthetic monthly growth metrics from quarterly or annual source data.

---

# Monthly Score Alignment

Future scoring frameworks may require indicators to be aligned to a common monthly evaluation cadence.

In such cases:

* Lower-frequency indicators may be carried forward using the latest available observation.
* This alignment mechanism exists solely for score construction.
* Canonical engineered features shall continue to be calculated using native reporting frequencies.

---

# Future Extensions

Future versions may introduce:

* Acceleration Metrics
* Volatility Metrics
* Peer Relative Rankings
* Z-Scores
* Regime-Specific Feature Families

These are intentionally excluded from v1 to maintain transparency and interpretability.

---

# Output Contract

Every indicator entering the scoring framework must provide:

1. Level
2. Short-Term Change
3. Long-Term Change
4. Historical Percentile for each feature

This creates a standardized feature structure that can be consumed consistently by:

* Macro Regime Engine
* Local Opportunity Engine
* Forecasting Layer
* Composite Score Framework
* Market Cycle Classification Framework
