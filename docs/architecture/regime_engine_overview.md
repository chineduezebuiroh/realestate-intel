# Regime Engine Architecture Overview

## Purpose

The Regime Engine converts heterogeneous real-estate, labor, affordability, supply, and capital-market observations into deterministic macro-regime coordinates and assignments.

## Current system boundary

The current production target is the county-level Macro Regime Engine. It is distinct from Market Balance, Market Profile, Local Regime, Forecast Regime, and downstream ranking products.

## Processing architecture

```text
source data
    ↓
canonical metric resolution
    ↓
derived metric calculation
    ↓
canonical source observations
    ↓
feature generation
    ↓
feature normalization
    ↓
metric scoring
    ↓
dimension scoring
    ↓
axis scoring
    ↓
coordinates
    ↓
geometry
    ↓
major / minor regime assignment
    ↓
validation and persisted artifacts
```

## Macro dimensions and axes

### Demand axis

| Dimension | Weight |
|---|---:|
| Demand | 0.65 |
| Price | 0.175 |
| Affordability | 0.075 |
| Capital Markets | 0.10 |

### Supply axis

| Dimension | Weight |
|---|---:|
| Supply | 0.85 |
| Capital Markets | 0.15 |

Capital Markets is shared. Liquidity and Transaction Activity are excluded from Macro Regime axis construction.

## Coordinate and geometry contract

```text
x_supply = Supply axis score
y_demand = Demand axis score
```

The engines calculate x/y, radius, angle, quadrant, boundary distance, transition-region information, major regime, minor regime, and assignment metadata.

## Observation and transform governance

Transform behavior is policy-driven. Shared production-safe implementations own algorithms; registries and run policies select the active transform.

The linked Price/Affordability policy establishes the precedent:

- direct price metrics use structural MA12 features;
- smoothed median sale price is substituted only into the linked derived-metric panel;
- price-to-income and payment burden are recalculated from the same structural price state;
- direct source metrics are not silently replaced;
- lineage is preserved and augmented explicitly.

## Persistence architecture

Each run persists a manifest and deterministic artifacts, including canonical observations, features, scores, coordinates, regimes, validation outputs, and lineage.

## Validation architecture

The suite includes history maturity, freshness and lineage, chronological review, contribution reconciliation, volatility and sign-flip analysis, transition attribution, challenger comparison, cancellation diagnostics, coordinate displacement, transition stability, and visual review.

Washington, DC county is mandatory. Automatic targeted reviews are county-first while CBSA labor and income behavior remains under deferred correctness audit.

## Current calibration sequence

1. Review tooling improvements
2. Inventory calibration
3. Price feature-weight calibration
4. CBSA labor and related engineering correctness
