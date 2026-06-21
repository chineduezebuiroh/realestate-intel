# C2.1 — Cycle Wheel Geometry (v2)

## Purpose

The Regime Engine represents market conditions using a two-dimensional coordinate system.

Each market receives:

```text
x = Supply Pressure
y = Demand Strength
```

The resulting coordinate is used to classify markets within a real estate cycle framework.

The cycle wheel serves as the foundation for:

* Current regime classification
* Forecast regime classification
* Historical regime tracking
* Market comparison
* Regime transition analysis

---

# Coordinate System

The wheel uses a normalized coordinate system.

```text
x ∈ [-1, +1]
y ∈ [-1, +1]
```

Where:

```text
x = Supply Pressure
```

and

```text
y = Demand Strength
```

The origin represents neutral conditions.

```text
(0,0)
```

Positive and negative values indicate increasing strength in either direction.

---

# Supply Axis

The horizontal axis represents Supply Pressure.

Higher values indicate increasing supply pressure.

Representative inputs may include:

* Building permits
* Housing starts
* Inventory growth
* Construction activity

Conceptually:

```text
Low Supply Pressure ← → High Supply Pressure
```

---

# Demand Axis

The vertical axis represents Demand Strength.

Higher values indicate stronger demand conditions.

Representative inputs may include:

* Employment growth
* Labor force growth
* Population growth
* GDP growth
* Demand-related price pressure

Conceptually:

```text
High Demand
     ↑
     |
     |
     |
-----+-----
     |
     |
     |
     ↓
Low Demand
```

---

# Primary Dimensions

Supply and Demand are the primary dimensions of the cycle wheel.

Regime classification should emerge from the interaction of these two dimensions.

The cycle wheel should remain anchored to these dimensions even as additional dimensions are introduced.

---

# Secondary Dimensions

The framework also includes:

* Affordability
* Liquidity
* Capital Markets

These dimensions provide important context but do not define the cycle wheel directly.

Current design assumptions:

### Affordability

Affordability is expected to influence the Demand dimension.

Improving affordability generally supports demand.

Deteriorating affordability generally constrains demand.

### Capital Markets

Capital Markets may influence both Demand and Supply dimensions.

Examples include:

* Mortgage rates
* Credit availability
* Yield curve conditions
* Financing environment

The exact mapping will be defined in C3.

### Liquidity

Liquidity remains intentionally unresolved.

Liquidity may ultimately function as:

* a supporting dimension
* a confirmation dimension
* a confidence dimension
* a partial axis contributor

Final treatment will be determined during Dimension-to-Axis Mapping.

---

# Coordinate Interpretation

Supply and Demand scores create a coordinate pair.

The coordinate format is always:

```text
(x, y)
```

where

- x = Supply Pressure
- y = Demand Strength

Example:

```text
(-0.6, +0.8)
```

represents:

```text
Constrained Supply
Strong Demand
```

Example:

```text
(+0.7, +0.8)
```

represents:

```text
Elevated Supply Pressure
Strong Demand
```

Example:

```text
(+0.8, -0.7)
```

represents:

```text
Elevated Supply Pressure
Weak Demand
```

Example:

```text
(-0.7, -0.7)
```

represents:

```text
Constrained Supply
Weak Demand
```

---

# Regime Classification

The regime framework is primarily angle-driven.

The relationship between Supply and Demand determines a market's location on the cycle wheel. Supply and Demand dimensions generate a coordinate pair:

```text
(x, y)
```

where

- x = Supply Pressure
- y = Demand Strength

The coordinate pair is converted into an angle:

```text
Coordinate
→ Angle
→ Regime Classification
```

The cycle wheel should always use the standard geometric convention:

- x-axis = Supply Pressure
- y-axis = Demand Strength

This convention must remain consistent throughout:

- scoring
- visualization
- forecasting
- classification
- backtesting

Regimes should emerge from coordinates rather than being manually assigned.

---

# Strength Versus Confidence

Strength and Confidence are distinct concepts.

### Regime Strength

Regime Strength measures the magnitude of market conditions.

Strength may be derived from the underlying Demand and Supply coordinates.

Strength does not determine regime classification.

### Regime Confidence

Regime Confidence measures how strongly supporting dimensions reinforce the classification.

Potential contributors include:

* Liquidity
* Affordability
* Capital Markets

Confidence does not determine regime classification.

---

# Forecast Coordinates

Forecasted indicator values generate forecasted coordinates.

The same coordinate framework should be used for:

* Current regime classification
* Forecast regime classification

No separate forecast regime framework should exist.

---

# Design Principles

1. Demand and Supply are the primary dimensions.

2. Regime classification is angle-driven.

3. Affordability primarily influences Demand.

4. Capital Markets may influence both Demand and Supply.

5. Liquidity remains intentionally unresolved.

6. Strength and Confidence are separate concepts.

7. Forecasts use the same coordinate framework as current conditions.

8. The framework must support both Macro and Local Regime Engines.

9. All coordinates must use the standard format:

```text
(x, y)
=
(Supply Pressure, Demand Strength)
```

This convention must remain consistent across all regime calculations.


# Governance Rule:

- Cardinal points represent maximum expression of a regime's dominant force.

- Diagonal boundaries represent equilibrium points between competing forces and therefore define regime transitions.
