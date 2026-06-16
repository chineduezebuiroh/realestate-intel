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

# Supply Features

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

Composite:

- price_score

---

# Liquidity Features

Raw Sources:

- Redfin

Tier 1 Features:

- inventory_level
- inventory_mom
- inventory_yoy

- months_supply_level
- months_supply_mom
- months_supply_yoy

- dom_level
- dom_mom
- dom_yoy

Composite:

- liquidity_score

---

# Capital Markets Features

Raw Sources:

- FRED

Tier 1 Features:

- fedfunds_level
- fedfunds_mom
- fedfunds_yoy

- mortgage30_level
- mortgage30_mom
- mortgage30_yoy

- yield_curve_level
- yield_curve_mom

Composite:

- capital_markets_score

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
