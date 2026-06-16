# B2.3 Feature Catalog v1

## Purpose

This document defines the first-generation engineered feature catalog.

The goal is to establish a controlled set of features that will be generated from raw indicators and used throughout the forecasting, regime, profile, and comparison layers.

This document intentionally separates:

- Raw source metrics
- Engineered features
- Regime features
- Structural/Profile features

Not every raw metric should become a regime feature.

---

# Feature Classification Framework

All engineered features are classified into one of three categories.

## Tier 1 — Regime Features

Definition:

Features that directly contribute to market cycle classification.

Examples:

- Employment growth
- Inventory growth
- Permit growth
- Housing supply growth
- Mortgage rate pressure

These features feed:

- Demand Score
- Supply Score
- Affordability Score
- Liquidity Score
- Price Score
- Capital Markets Score

and ultimately the:

- Macro Regime Engine
- Local Regime Engine

---

## Tier 2 — Structural Profile Features

Definition:

Features that describe how a market functions.

These do not directly determine regime classification.

Examples:

- Multifamily permit share
- Construction employment share
- Government employment share
- Housing type mix

These features feed:

- Market Profile Engine
- Market Characterization Outputs
- Market Comparison Visualizations

---

## Tier 3 — Experimental Features

Definition:

Features retained for research and future testing.

Examples:

- Permit mix composites
- Economic concentration metrics
- Market specialization metrics

These features are not included in production scoring.

---

# Demand Features

## Employment

Raw Sources:

- CES
- LAUS

Tier 1 Features:

- employment_level
- employment_mom
- employment_yoy

Normalized Features:

- employment_level_pct
- employment_mom_pct
- employment_yoy_pct

Composite:

- employment_score

---

## Labor Force

Raw Sources:

- LAUS

Tier 1 Features:

- labor_force_level
- labor_force_mom
- labor_force_yoy

Normalized Features:

- labor_force_level_pct
- labor_force_mom_pct
- labor_force_yoy_pct

Composite:

- labor_force_score

---

## Population

Raw Sources:

- Census ACS

Tier 1 Features:

- population_level
- population_yoy

Normalized Features:

- population_level_pct
- population_yoy_pct

Composite:

- population_score

---

## GDP

Raw Sources:

- BEA

Tier 1 Features:

Quarterly GDP:

- qgdp_level
- qgdp_qoq
- qgdp_yoy

Annual GDP:

- agdp_level
- agdp_yoy
- agdp_5yr_change

Normalized Features:

- qgdp_level_pct
- qgdp_qoq_pct
- qgdp_yoy_pct

- agdp_level_pct
- agdp_yoy_pct
- agdp_5yr_change_pct

Composite:

- gdp_score

---

## Unemployment

Raw Sources:
- LAUS
- FRED Unemployment

Tier 1 Features:

- unemployment_rate_level
- unemployment_rate_mom
- unemployment_rate_yoy

Normalized Features:

- unemployment_rate_level_pct
- unemployment_rate_mom_pct
- unemployment_rate_yoy_pct

Composite:

- unemployment_score

---

# Supply Features

## Active Inventory

Raw Sources:

- Redfin

Tier 1 Features:

- inventory_level
- inventory_mom
- inventory_yoy

Normalized Features:

- inventory_level_pct
- inventory_mom_pct
- inventory_yoy_pct

Composite:

- inventory_score

---

## Building Permits

Raw Sources:

- Census BPS

Tier 1 Features:

- permits_total_level
- permits_total_mom
- permits_total_yoy

Normalized Features:

- permits_total_level_pct
- permits_total_mom_pct
- permits_total_yoy_pct

Composite:

- permits_score

---

## Housing Starts

Raw Sources:

- Census Housing Starts

Tier 1 Features:

- starts_level
- starts_mom
- starts_yoy

Normalized Features:

- starts_level_pct
- starts_mom_pct
- starts_yoy_pct

Composite:

- starts_score

---

## Construction Labor Activity

Raw Sources:
- CES

Tier 1 Features:

- construction_payroll_level
- construction_payroll_mom
- construction_payroll_yoy

Composite:

- construction_activity_score

---

# Affordability Features

Raw Sources:

- Redfin
- Census ACS
- FRED

Tier 1 Features:

- price_to_income ratio
- payment burden

---

# Price Features

Raw Sources:

- Redfin

Tier 1 Features:

- median_sale_price_level
- median_sale_price_mom
- median_sale_price_yoy

- median_ppsf_level
- median_ppsf_mom
- median_ppsf_yoy

Normalized Features:

- median_sale_price_level_pct
- median_sale_price_mom_pct
- median_sale_price_yoy_pct

- median_ppsf_level_pct
- median_ppsf_mom_pct
- median_ppsf_yoy_pct

Composite:

- price_score

---

# Liquidity Features

## Days on Market (DOM)

Raw Sources:

- Redfin

Tier 1 Features:

- dom_level
- dom_mom
- dom_yoy

Normalized Features:

- dom_level_pct
- dom_mom_pct
- dom_yoy_pct

Composite:

- dom_score

---

## Sale-to-List Ratio

Raw Sources:

- Redfin

Tier 1 Features:

- avg_sale_to_list_level
- avg_sale_to_list_mom
- avg_sale_to_list_yoy

Normalized Features:

- avg_sale_to_list_level_pct
- avg_sale_to_list_mom_pct
- avg_sale_to_list_yoy_pct

Composite:

- sale_to_list_score

---

## Transaction Volume

Raw Sources:

- Redfin

Tier 1 Features:

- homes_sold_level
- homes_sold_mom
- homes_sold_yoy

Normalized Features:

- homes_sold_level_pct
- homes_sold_mom_pct
- homes_sold_yoy_pct

Composite:

- homes_sold_score

---

## Months of Supply

Raw Sources:

- Redfin

Tier 1 Features:

- months_supply_level
- months_supply_mom
- months_supply_yoy

Normalized Features:

- months_supply_level_pct
- months_supply_mom_pct
- months_supply_yoy_pct

Composite:

- months_supply_score

---

# Capital Markets Features

Raw Sources:

- FRED

## Policy Rates

Tier 1 Features:

- fedfunds_level
- fedfunds_mom
- fedfunds_yoy

Normalized Features:

- fedfunds_level_pct
- fedfunds_mom_pct
- fedfunds_yoy_pct

Composite:

- fed_funds_score

--- 

## Mortgage Financing

Tier 1 Features:

- mortgage30_level
- mortgage30_mom
- mortgage30_yoy

- mortgage15_level
- mortgage15_mom
- mortgage15_yoy

Normalized Features:

- mortgage30_level_pct
- mortgage30_mom_pct
- mortgage30_yoy_pct

- mortgage15_level_pct
- mortgage15_mom_pct
- mortgage15_yoy_pct

Composite:

- mortgage_score

--- 

## Credit / Term Structure

Tier 1 Features:

- spread_2y_10y
- spread_10y_fedfunds
- spread_30y_fedfunds

- Not clear yet on if we need to normalize these and if so, how that will work  

Composite:

- rate_spread_score

---

# Structural Profile Features

## Employment Composition

Examples:

- construction_payroll_share
- government_payroll_share
- healthcare_payroll_share
- financial_payroll_share

Classification:

Tier 2

---

## Housing Composition

Examples:

- sf_permit_share
- mf_permit_share

Classification:

Tier 2

---

## Economic Composition

Examples:

- industry_concentration
- economic_diversification

Classification:

Tier 2

---

# Governance Rules

1. Every production feature must belong to exactly one taxonomy category.

2. Every regime feature must map to exactly one dimension score.

3. Structural features must not influence regime classification unless explicitly promoted.

4. Experimental features must never enter production scoring without governance approval.

5. New data sources must first map into this catalog before entering downstream systems.
