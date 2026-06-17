# B2.4 Dimension Score Construction v1

## Purpose

This document defines how normalized engineered features roll up into dimension-level scores.

Dimension scores are intermediate outputs between engineered indicators and the Regime Engines.

This document does not define final regime classification. Regime states, cycle-wheel coordinates, and transition rules are handled in Phase C.

---

# Dimension Score Families

The platform currently defines six primary dimension families:

1. Demand
2. Supply
3. Affordability
4. Liquidity
5. Price
6. Capital Markets

Each dimension score summarizes the condition of one market force.

---

# Input Contract

Inputs to dimension scoring come from B2.3 Feature Catalog.

Each input feature should provide:

* metric_id
* geography
* property_type
* feature_name
* raw_value
* historical_percentile
* direction_adjusted_percentile
* feature_tier
* dimension_family

Only eligible Tier 1 regime features enter dimension score construction.

Tier 2 structural profile features and Tier 3 experimental features are excluded from v1 dimension scoring.

---

# Score Scale

All dimension scores use a 0–100 scale.

Interpretation:

| Score Range | Interpretation |
| ----------: | -------------- |
|        0–20 | Very weak      |
|       20–40 | Weak           |
|       40–60 | Neutral        |
|       60–80 | Strong         |
|      80–100 | Very strong    |

The score should be interpreted as strength relative to the geography's own historical context.

---

# Default Aggregation Method

The v1 default aggregation method is a simple weighted average of eligible direction-adjusted feature percentiles.

Formula:

```text
dimension_score =
    sum(feature_score * feature_weight)
    /
    sum(feature_weight)
```

Where:

```text
feature_score = direction_adjusted_percentile
```

and:

```text
feature_weight = v1 configured feature weight
```

If no custom weights are configured, all eligible Tier 1 features within the dimension receive equal weight.

---

# Feature-Level Scoring

Every feature entering a dimension score must be direction-adjusted before aggregation.

Example:

Unemployment rate:

```text
Raw percentile = 90
Direction = lower is better
Direction-adjusted percentile = 10
```

Employment growth:

```text
Raw percentile = 90
Direction = higher is better
Direction-adjusted percentile = 90
```

This ensures that higher dimension scores always indicate more favorable conditions.

---

# Dimension Definitions

## Demand Score

Purpose:

Measures underlying economic and demographic demand strength.

Candidate Tier 1 inputs:

* employment_score
* labor_force_score
* unemployment_score
* population_score
* gdp_score

Interpretation:

| Score | Meaning                                |
| ----: | -------------------------------------- |
|  High | Demand backdrop is historically strong |
|   Low | Demand backdrop is historically weak   |

---

## Supply Score

Purpose:

Measures supply pressure and new housing availability.

Candidate Tier 1 inputs:

* active_inventory_score
* permits_score
* starts_score
* construction_activity_score

Important:

Supply directionality is context-sensitive.

For v1, Supply Score should be interpreted as supply pressure, not necessarily market attractiveness.

High Supply Score means:

```text
supply pressure is elevated relative to history
```

Low Supply Score means:

```text
supply pressure is constrained relative to history
```

Phase C will determine whether high supply pressure is favorable or unfavorable depending on the regime framework.

---

## Affordability Score

Purpose:

Measures affordability conditions for households and buyers.

Candidate Tier 1 inputs:

* price_to_income_score
* payment_burden_score
* rent_burden_score

Interpretation:

| Score | Meaning                                |
| ----: | -------------------------------------- |
|  High | Affordability conditions are favorable |
|   Low | Affordability conditions are strained  |

---

## Liquidity Score

Purpose:

Measures transaction velocity and market-clearing strength.

Candidate Tier 1 inputs:

* dom_score
* sale_to_list_score
* transaction_volume_score
* months_supply_score

Interpretation:

| Score | Meaning                                   |
| ----: | ----------------------------------------- |
|  High | Market is liquid and clearing efficiently |
|   Low | Market is illiquid or slowing             |

---

## Price Score

Purpose:

Measures price strength and valuation momentum.

Candidate Tier 1 inputs:

* median_sale_price_score
* median_ppsf_score

Interpretation:

| Score | Meaning                                         |
| ----: | ----------------------------------------------- |
|  High | Price environment is strong relative to history |
|   Low | Price environment is weak relative to history   |

---

## Capital Markets Score

Purpose:

Measures financing conditions and rate pressure.

Candidate Tier 1 inputs:

* policy_rate_score
* mortgage_rate_score
* spread_score

Interpretation:

| Score | Meaning                              |
| ----: | ------------------------------------ |
|  High | Financing conditions are favorable   |
|   Low | Financing conditions are restrictive |

---

# Property Type Rules

## Macro Regime Dimension Scores

Macro regime dimension scores use:

```text
property_type = ALL
```

This keeps macro regime scoring focused on geography-level cycle conditions.

---

## Local Regime Dimension Scores

Local regime dimension scores may use:

```text
property_type = ALL
```

or property-type-specific series, depending on the local use case.

Property-type-specific local scores are valid for:

* SFH opportunity analysis
* Condo opportunity analysis
* Townhome opportunity analysis
* asset-type overlays

---

# Missing Data Rules

If one feature is missing inside a dimension:

* exclude the feature from the weighted average
* renormalize remaining weights
* retain a coverage count

Example:

```text
Demand Score inputs available:
Employment, Unemployment, GDP

Demand Score inputs missing:
Population, Labor Force

Demand Score is calculated from available inputs only.
Coverage = 3 / 5
```

No missing feature should be imputed in v1.

---

# Confidence Score

Each dimension score should carry a confidence indicator.

Minimum v1 fields:

* feature_count_available
* feature_count_expected
* coverage_ratio

Example:

```text
Demand Score = 72
Available features = 4
Expected features = 5
Coverage ratio = 0.80
```

Future versions may include:

* history sufficiency score
* freshness score
* revision risk score
* volatility penalty

---

# Tier Handling

## Tier 1

Included in dimension scoring.

## Tier 2

Excluded from dimension scoring.

Used in Market Profile Engine.

## Tier 3

Excluded from production scoring.

Used for research only.

---

# Governance Rules

1. Dimension scores must be traceable to individual feature scores.

2. No Tier 2 or Tier 3 feature may enter dimension scoring without explicit promotion.

3. Missing features must be excluded and reported, not imputed.

4. Dimension scores must retain coverage metadata.

5. Weighting changes must be versioned.

6. Dimension scores are not final regime classifications.

---

# v1 Output Contract

Each dimension score record should contain:

* asof_date
* geography
* property_type
* dimension_family
* dimension_score
* feature_count_available
* feature_count_expected
* coverage_ratio
* score_version

These records become direct inputs into:

* Macro Regime Engine
* Local Regime Engine
* Market Profile Engine
* Market Comparison Engine
