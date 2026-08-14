# County Labor Demand and Market Context Production Policy

## Status and authority

**Status:** Accepted production policy (supersedes the S25/C75 blended Core Demand policy)
**Scope:** County macro production
**Executable authority:** `config/feature_registry.csv`, `config/metric_dimension_registry.csv`, `config/axis_registry.csv`, and `config/normalization_registry.csv`

The persisted canonical dimension key remains `demand` for downstream compatibility. Its production meaning and display concept is **Labor Demand**. Structural evidence is persisted under the separate canonical `market_context` dimension.

## Labor Demand

The numeric `demand` dimension contains exactly these canonical metric families:

- `labor_force` (LF-IN);
- `employment` (existing governed CES-first/LAUS-fallback source resolution);
- `laus_unemployment_rate`.

Available metrics use equal configured weights and renormalize only among available Labor metrics. The former Structural/Cyclical block scorer and its S25/C75 allocation are superseded and are not on the production scoring path.

All governed LAUS feature families use calendar-aware MA9 with the shared two-thirds coverage requirement and exact lag semantics:

| Feature | Definition | Weight |
|---|---|---:|
| Level | MA9 | 0.40 |
| Short | MA9 / lag3(MA9) - 1 | 0.15 |
| Long | MA9 / lag12(MA9) - 1 | 0.45 |

Missing feature components are excluded and remaining feature weights renormalize. LAUS unemployment-rate direction remains negative; Labor Force and Employment remain positive.

## Market Context

`market_context` contains exactly `population`, `median_household_income`, and county `gdp_annual`, using their existing sources, feature engineering, normalization, as-of chronology, source dates, and freshness. It is a scored and persisted dimension surface, but is deliberately absent from `axis_registry.csv`. It therefore contributes neither to Labor Demand nor to any Demand-axis availability normalization.

## Demand axis

Active membership remains:

| Dimension | Weight |
|---|---:|
| `demand` (Labor Demand) | 0.650 |
| `price` | 0.175 |
| `affordability` | 0.075 |
| `capital_markets` | 0.100 |

Price, Affordability, and Capital Markets policy is unchanged.

## Normalization and lineage

Normalization math and parameters are unchanged. The historical method identifier `expanding_percentile` is retained for compatibility, but its production implementation is a bounded rolling-window percentile. A future governed migration may clarify the identifier; it must not change math. LAUS remains lookback 120, minimum 36, and clipping 0.01–0.99.

Production persists normalized features, aligned metric chronology, dimension and axis scores, plus feature/metric/dimension contribution artifacts. Together these retain configured weight, availability-normalized effective weight, weighted contribution, parent identifier, sign/direction, observation dates, and age so Axis → Dimension → Metric → Feature is reconstructable.

## Geography boundary

County annual GDP is Market Context. This decision does not assign quarterly CBSA GDP a role.

> Reassess quarterly GDP independently during CBSA calibration before determining its scoring role.

No CBSA change is implemented by this policy.
