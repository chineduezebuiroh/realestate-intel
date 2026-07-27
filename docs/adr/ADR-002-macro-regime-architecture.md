# ADR-002: Macro Regime Architecture

## Status

Accepted

## Context

The Macro Regime Engine requires a stable contract for how canonical metrics become dimension scores, how dimensions map into axes, and how axes become coordinates and regime assignments.

## Decision

The Macro Regime Engine uses two axes.

### Demand axis

- Demand
- Price
- Affordability
- Capital Markets

### Supply axis

- Supply
- Capital Markets

Capital Markets is shared across both axes. Liquidity and Transaction Activity do not contribute to Macro Regime axes; they remain diagnostic dimensions reserved for later Market Balance / Market Profile work.

```text
canonical observations
        ↓
engineered features
        ↓
normalized features
        ↓
metric scores
        ↓
dimension scores
        ↓
axis scores
        ↓
coordinates
        ↓
geometry
        ↓
major and minor regime assignments
```

The coordinate contract is:

```text
x = Supply axis
y = Demand axis
```

## Consequences

- Macro Regime comparisons must remain scoped to the approved dimensions.
- Liquidity and Transaction Activity may be reported for coverage but not used to support Macro Regime conclusions.
- Capital Markets logic must remain valid for both axes.
- Local and forecast regime engines require separate architecture and acceptance contracts.
