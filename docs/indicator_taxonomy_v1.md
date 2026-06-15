# Indicator Taxonomy v1

## Purpose

This document defines the indicator universe for the Real Estate Intelligence Platform.

The purpose of this document is to establish **what economic, housing, liquidity, affordability, and capital market concepts will be measured**.

This document does **not** define:

* feature engineering
* scoring methodology
* normalization
* weighting
* regime classification

Those topics are addressed in subsequent phases:

* B2 Indicator Engineering
* Phase C Regime Engine

---

# Indicator Families

The platform organizes indicators into six primary families:

1. Demand
2. Supply
3. Affordability
4. Liquidity
5. Price
6. Capital Markets

---

# Demand Indicators

Demand indicators measure population growth, labor market strength, and economic expansion.

| Indicator    | Primary Source   | Frequency           | Supported Geo Levels         |
| ------------ | ---------------- | ------------------- | ---------------------------- |
| Population   | Census ACS       | Annual              | Nation, State, County        |
| GDP          | BEA QGDP / AGDP  | Quarterly / Annual  | Nation, State, County        |
| Labor Force  | BLS LAUS         | Monthly             | Nation, State, Metro, County |
| Employment   | BLS CES / LAUS   | Monthly             | Nation, State, Metro, County |
| Unemployment | BLS LAUS         | Monthly             | Nation, State, Metro, County |

### Notes

* Population serves as the long-term demand anchor.
* Labor Force captures workforce participation and market depth.
* Employment measures labor market expansion.
* Unemployment measures labor market weakness.

---

# Supply Indicators

Supply indicators measure existing inventory and future housing production.

| Indicator             | Primary Source | Frequency | Supported Geo Levels                 |
| --------------------- | -------------- | --------- | ------------------------------------ |
| Active Inventory      | Redfin         | Monthly   | Nation, State, Metro, County, ZIP    |
| Building Permits      | Census BPS     | Monthly   | Nation, Region, State, Metro, County |
| Housing Starts        | Census BPS     | Monthly   | Nation, Region, State                |
| Construction Activity | Derived        | Monthly   | Nation, State, Metro, County         |

### Notes

* Active Inventory measures currently available housing stock.
* Building Permits measure intended future supply.
* Housing Starts measure actual construction initiation.
* Construction Activity will be a derived indicator constructed during B2.

---

# Affordability Indicators

Affordability indicators measure housing cost burdens relative to household resources.

| Indicator             | Primary Source      | Frequency               | Supported Geo Levels |
| --------------------- | ------------------- | ----------------------- | -------------------- |
| Price-to-Income Ratio | Redfin + Census ACS | Monthly / Annual Hybrid | State, Metro, County |
| Payment Burden        | Redfin + FRED       | Monthly                 | State, Metro, County |

### Notes

* Price-to-Income captures structural affordability.
* Payment Burden captures financing-adjusted affordability.

---

# Liquidity Indicators

Liquidity indicators measure market velocity and transaction efficiency.

| Indicator            | Primary Source | Frequency | Supported Geo Levels              |
| -------------------- | -------------- | --------- | --------------------------------- |
| Days on Market (DOM) | Redfin         | Monthly   | Nation, State, Metro, County, ZIP |
| Sale-to-List Ratio   | Redfin         | Monthly   | Nation, State, Metro, County, ZIP |
| Transaction Volume   | Redfin         | Monthly   | Nation, State, Metro, County, ZIP |
| Months Supply        | Redfin         | Monthly   | Nation, State, Metro, County, ZIP |

### Notes

* Liquidity indicators are critical inputs into both Macro Regime and Local Opportunity analysis.

---

# Price Indicators

Price indicators measure housing market valuation and appreciation dynamics.

| Indicator                           | Primary Source | Frequency | Supported Geo Levels              |
| ----------------------------------- | -------------- | --------- | --------------------------------- |
| Median Sale Price                   | Redfin         | Monthly   | Nation, State, Metro, County, ZIP |
| Median Price Per Square Foot (PPSF) | Redfin         | Monthly   | Nation, State, Metro, County, ZIP |

### Notes

* Price indicators provide both valuation context and future trend inputs.
* Growth and momentum calculations will be defined during B2.

---

# Capital Market Indicators

Capital market indicators measure financing conditions affecting housing demand and development activity.

| Indicator       | Primary Source | Frequency | Supported Geo Levels |
| --------------- | -------------- | --------- | -------------------- |
| Mortgage Rate   | FRED           | Monthly   | National             |
| Mortgage Spread | FRED           | Monthly   | National             |

### Notes

* Capital market indicators are national-level factors.
* These indicators apply uniformly across all geographies.

---

# Tier 2 Future Indicators

The following indicators remain under consideration for future versions.

## Demand

* Household Formation
* Net Migration
* Population Migration Flows

## Supply

* Multifamily Completions
* Vacancy Rates

## Affordability

* Rent Burden
* Rent-to-Income Ratio

## Capital Markets

* Credit Availability
* Lending Standards
* Construction Financing Conditions

---

# Design Principles

## Principle 1

Indicator Taxonomy defines concepts, not calculations.

Example:

* Employment is an indicator.
* Employment YoY Growth is a derived feature.

---

## Principle 2

Feature engineering is handled separately.

Future derived features may include:

* Current level
* MoM change
* QoQ change
* YoY change
* Acceleration
* Trend
* Rolling averages
* Percentile rankings

These belong to B2 Indicator Engineering.

---

## Principle 3

Indicators may exist at different geography levels.

Not every indicator must exist at every geography level.

Scoring frameworks should account for varying geographic coverage.

---

## Principle 4

Macro Regime and Local Opportunity engines may use different subsets of indicators.

Macro Regime Engine primarily emphasizes:

* Demand
* Supply
* Affordability
* Liquidity
* Capital Markets

Local Opportunity Engine primarily emphasizes:

* Price
* Liquidity
* Local Supply

while incorporating macro regime overlays.

---

## Principle 5

All future scoring, weighting, and regime classification logic must trace back to this taxonomy.
