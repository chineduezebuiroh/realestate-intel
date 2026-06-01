# Indicator Matrix v1

## Purpose

This document defines the initial source-to-indicator matrix for the market intelligence system.

The goal is to distinguish:

1. raw metrics available in the database,
2. derived indicators created from raw metrics,
3. geo levels where each indicator is valid,
4. whether the indicator supports the Macro Regime Engine, Local Opportunity Engine, or both.

## Engine Definitions

### Macro Regime Engine

Used for broad market analysis across larger geographies:

* nation
* state
* metro
* county
* city

Primary question:

> What is the supply-demand regime and market-cycle position of this geography?

### Local Opportunity Engine

Used for ZIP-code and neighborhood comparison.

Primary question:

> Within a parent macro market, which local areas show better opportunity, momentum, liquidity, and relative risk?

## Indicator Matrix

| Family          | Indicator               | Raw Metric(s)                                             | Source                               | Direct Geo Levels                                         | Derived? | Macro Engine | Local Engine | Status          | Notes                                                                                          |
| --------------- | ----------------------- | --------------------------------------------------------- | ------------------------------------ | --------------------------------------------------------- | -------: | -----------: | -----------: | --------------- | ---------------------------------------------------------------------------------------------- |
| Economic Demand | Employment Growth       | `laus_employment_sa`, `laus_employment_nsa`               | LAUS                                 | state, metro/MSD, county, selected city                   |      Yes |          Yes |           No | Existing        | Use YoY and recent trend. Not available for neighborhood/ZIP.                                  |
| Economic Demand | Unemployment Trend      | `laus_unemployment_rate_sa`, `laus_unemployment_rate_nsa` | LAUS                                 | state, metro/MSD, county, selected city                   |      Yes |          Yes |           No | Existing        | Separate from employment growth; deterioration signal.                                         |
| Economic Demand | Payroll Momentum        | `ces_total_nonfarm_sa`, sector payroll metrics            | CES                                  | state, metro                                              |      Yes |          Yes |           No | Existing        | Strong macro demand signal. CES metadata should be governed separately from recurring refresh. |
| Economic Demand | Sector Payroll Momentum | CES sector metrics                                        | CES                                  | state, metro                                              |      Yes |          Yes |           No | Existing        | Construction, professional services, government, etc. Useful for market-specific drivers.      |
| Supply          | Inventory Growth        | Redfin inventory / active listings metric                 | Redfin                               | neighborhood, ZIP, city, county, metro, state, nation     |      Yes |          Yes |          Yes | Existing        | Core supply pressure variable.                                                                 |
| Supply          | Housing Starts          | `census_housing_starts_total_saar`                        | Census NRC / FRED                    | nation, region                                            |      Yes |          Yes |           No | Existing        | Better execution signal than permits, but limited local granularity.                           |
| Supply          | Building Permits        | Census BPS permit metrics                                 | Census BPS                           | nation, state, metro, county                              |      Yes |          Yes |      Limited | Existing        | Early supply intent signal. Use cautiously.                                                    |
| Supply          | Construction Intensity  | Starts or permits per capita / per housing stock          | Derived                              | depends on components                                     |      Yes |          Yes |      Limited | Future          | Needs denominator: population, housing units, or existing inventory.                           |
| Affordability   | Price-to-Income         | `median_sale_price`, `census_median_household_income`     | Redfin + ACS                         | city, county, state, nation; local may need parent income |      Yes |          Yes |          Yes | Future          | For local engine, income may be parent city/county unless local ACS available.                 |
| Affordability   | Payment Burden          | median price, mortgage rate, income                       | Redfin + FRED + ACS                  | derived                                                   |      Yes |          Yes |          Yes | Future          | More decision-useful than price-to-income. Requires mortgage payment calculation.              |
| Affordability   | Rent Burden             | rent / income                                             | External or future rent source + ACS | TBD                                                       |      Yes |       Future |       Future | Deferred        | Not V1 unless rent data is added.                                                              |
| Liquidity       | Days on Market          | `median_dom`                                              | Redfin                               | neighborhood, ZIP, city, county, metro, state, nation     |      Yes |          Yes |          Yes | Existing        | Higher DOM = weaker liquidity / friction.                                                      |
| Liquidity       | Sale-to-List Ratio      | sale-to-list metric                                       | Redfin                               | neighborhood, ZIP, city, county, metro, state, nation     |      Yes |          Yes |          Yes | Existing        | Buyer/seller power indicator.                                                                  |
| Liquidity       | Transaction Volume      | homes sold / sales count metric                           | Redfin                               | neighborhood, ZIP, city, county, metro, state, nation     |      Yes |          Yes |          Yes | Existing        | Demand/liquidity confirmation.                                                                 |
| Liquidity       | Months of Supply        | inventory / monthly sales pace                            | Derived from Redfin                  | neighborhood, ZIP, city, county, metro, state, nation     |      Yes |          Yes |          Yes | Future          | Likely better than DOM as supply-liquidity friction metric.                                    |
| Capital Markets | Mortgage Rate           | `fred_mortgage_30y_avg`, `fred_mortgage_15y_avg`          | FRED                                 | nation                                                    |       No |      Context |      Context | Existing        | Not useful for cross-geo differentiation by itself; useful as national backdrop.               |
| Capital Markets | Treasury Rates          | `fred_gs2`, `fred_gs10`, `fred_gs30`                      | FRED                                 | nation                                                    |       No |      Context |      Context | Existing        | Macro backdrop only.                                                                           |
| Capital Markets | Mortgage Spread         | mortgage rate minus Treasury rate                         | Derived from FRED                    | nation                                                    |      Yes |      Context |      Context | Future          | Captures financing stress / credit spread.                                                     |
| Outcome         | Median Sale Price       | `median_sale_price`                                       | Redfin                               | neighborhood, ZIP, city, county, metro, state, nation     |      Yes | Confirmation | Confirmation | Existing        | Outcome variable, not pure demand driver.                                                      |
| Outcome         | Median PPSF             | `median_ppsf`                                             | Redfin                               | neighborhood, ZIP, city, county, metro, state, nation     |      Yes | Confirmation |          Yes | Existing        | Better profitability proxy than median sale price.                                             |
| Outcome         | Price Momentum          | forward / trailing change in sale price or PPSF           | Derived from Redfin                  | neighborhood, ZIP, city, county, metro, state, nation     |      Yes |          Yes |          Yes | Existing/Future | Should feed local opportunity score carefully.                                                 |

## V1 Principle

Indicators should not be forced into geographies where they do not exist.

For example:

* neighborhood unemployment should not be fabricated,
* neighborhood GDP should not be fabricated,
* national mortgage rates should not be treated as cross-market differentiators.

Instead, the system should distinguish:

1. directly observed local signals,
2. parent macro context,
3. national capital-market backdrop.

## Near-Term Implementation Priority

1. Use Redfin-derived indicators for local ZIP/neighborhood scoring.
2. Use LAUS/CES/ACS/BPS/FRED/Redfin for macro scoring.
3. Use capital markets as national context, not local differentiation.
4. Add months-of-supply as a priority derived indicator.
5. Add price-to-income and payment burden after income mapping is defined.
