# C3.1 — Demand Dimension v1

## Purpose

This document defines the Demand Dimension used by the Macro Regime Engine.

The Demand Dimension measures the strength of underlying economic and demographic demand conditions within a geography.

It is one of the core inputs used to construct the Demand Axis of the regime cycle wheel.

---

# Core Question

The Demand Dimension answers:

> Is demand strengthening, weakening, or neutral relative to this market's own history?

---

# Demand Dimension Inputs

The Demand Dimension is constructed from five candidate subcomponents:

1. Employment
2. Labor Force
3. Unemployment
4. Population
5. GDP

---

# Subcomponent Definitions

## Employment

Measures job growth and labor market expansion.

Primary Sources:

* BLS CES
* BLS LAUS

Candidate Features:

* employment_level
* employment_short_term_change
* employment_long_term_change

Interpretation:

Higher employment strength is supportive of demand.

---

## Labor Force

Measures workforce depth and participation.

Primary Source:

* BLS LAUS

Candidate Features:

* labor_force_level
* labor_force_short_term_change
* labor_force_long_term_change

Interpretation:

Higher labor force strength is supportive of demand.

---

## Unemployment

Measures labor market weakness.

Primary Sources:

* BLS LAUS
* FRED Unemployment

Candidate Features:

* unemployment_rate_level
* unemployment_rate_short_term_change
* unemployment_rate_long_term_change

Interpretation:

Higher unemployment is negative for demand.

Unemployment features must be direction-adjusted so that:

```text
Lower unemployment = stronger demand signal
Higher unemployment = weaker demand signal
```

---

## Population

Measures long-term demographic demand.

Primary Source:

* Census ACS

Candidate Features:

* population_level
* population_short_term_change
* population_long_term_change

Interpretation:

Higher population strength is supportive of demand.

---

## GDP

Measures economic output and market productivity.

Primary Source:

* BEA

Candidate Features:

* gdp_level
* gdp_short_term_change
* gdp_long_term_change

Interpretation:

Higher GDP strength is supportive of demand.

---

# Normalization

Each feature enters the Demand Dimension as a direction-adjusted historical percentile.

All feature scores use the standard Phase B normalized score convention:

```text
-1.00 = extremely weak
 0.00 = neutral
+1.00 = extremely strong
```

---

# Default Weighting

The v1 Demand Dimension uses equal-weighted subcomponents where available.

Default weights:

| Subcomponent | Weight |
| ------------ | -----: |
| Employment   |    20% |
| Labor Force  |    20% |
| Unemployment |    20% |
| Population   |    20% |
| GDP          |    20% |

If a subcomponent is unavailable, remaining weights are renormalized.

---

# Feature Aggregation Within Subcomponents

Each subcomponent is calculated from available feature scores.

Default within-subcomponent weights:

| Feature Type      | Weight |
| ----------------- | -----: |
| Level             |    25% |
| Short-Term Change |    35% |
| Long-Term Change  |    40% |

Rationale:

* Level provides current condition context.
* Short-term change captures current momentum.
* Long-term change captures structural demand direction.

For structural growth indicators such as Population and GDP, future versions may reduce the Level weight if testing shows level percentiles mostly reflect trend age rather than regime position.

---

# Missing Data Handling

If a feature is missing inside a subcomponent:

* exclude the missing feature
* renormalize remaining feature weights
* preserve feature coverage metadata

If an entire subcomponent is missing:

* exclude the subcomponent
* renormalize remaining subcomponent weights
* reduce confidence score

No missing demand feature should be imputed in v1.

---

# Geography Coverage

The Demand Dimension may be calculated at both:

* CBSA / metro level
* County level

Different geographies may have different data coverage.

Examples:

* CES may be available at state and metro levels.
* LAUS may be available at county level.
* GDP may be available at state and county levels.
* Population may be available at county level.

The Demand Dimension should remain computable as long as coverage metadata is retained.

---

# Output Contract

The Demand Dimension produces:

```text
asof_date
geo_name
geo_level
property_type
dimension_family = demand
dimension_score
subcomponent_count_available
subcomponent_count_expected
coverage_ratio
confidence_score
score_version
```

---

# Interpretation

|   Demand Score | Interpretation             |
| -------------: | -------------------------- |
| +0.60 to +1.00 | Strong demand              |
| +0.20 to +0.60 | Moderately positive demand |
| -0.20 to +0.20 | Neutral demand             |
| -0.60 to -0.20 | Moderately weak demand     |
| -1.00 to -0.60 | Weak demand                |

---

# Governance Notes

1. Demand Dimension does not classify regimes by itself.

2. Demand Dimension feeds the Demand Axis.

3. Affordability may later modify the Demand Axis, but it is not part of the core Demand Dimension.

4. Liquidity and Price may confirm or challenge demand interpretation, but they are not core Demand Dimension inputs in v1.

5. Weighting may be revised after backtesting.
