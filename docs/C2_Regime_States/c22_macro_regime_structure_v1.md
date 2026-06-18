# C2.2 — Macro Regime Structure v1

## Purpose

This document defines the geometric structure of the Macro Regime Cycle Wheel.

The cycle wheel serves as the primary visualization and classification framework for market regimes.

Every market receives:

- Supply Axis coordinate (X)
- Demand Axis coordinate (Y)

These coordinates determine:

- Regime quadrant
- Regime sub-state
- Position within the cycle
- Direction of travel
- Future forecasted regime location

---

# Coordinate System

## Axis Definitions

### Supply Axis (X)

Represents supply pressure.

Range:

-1.0 to +1.0

Interpretation:

| Value | Interpretation |
|---------|---------|
| -1.0 | Extremely low supply pressure |
| 0.0 | Neutral supply pressure |
| +1.0 | Extremely high supply pressure |

---

### Demand Axis (Y)

Represents demand strength.

Range:

-1.0 to +1.0

Interpretation:

| Value | Interpretation |
|---------|---------|
| -1.0 | Extremely weak demand |
| 0.0 | Neutral demand |
| +1.0 | Extremely strong demand |

---

## Coordinate Convention

Coordinates are always represented as:

(X, Y)

Where:

- X = Supply Pressure
- Y = Demand Strength

Example:

(0.60, 0.80)

means:

- Moderately high supply pressure
- Very strong demand

All future calculations, scoring, geometry, and visualization components must preserve this convention.

---

# Angular Framework

The cycle wheel uses polar coordinates.

Definitions:

- 0° = Positive Supply Axis
- Angles increase counter-clockwise
- Full cycle = 360°

---

## Cardinal Points

| Angle | Interpretation |
|---------|---------|
| 0° | Maximum Supply Pressure |
| 90° | Maximum Demand Strength |
| 180° | Minimum Supply Pressure |
| 270° | Minimum Demand Strength |

---

# Major Regime Quadrants

The cycle wheel contains four major regimes.

Each regime occupies 90 degrees.

---

## Hypersupply

Angle:

315° < θ ≤ 45°

Characteristics:

- High supply pressure
- Demand supportive or weakening
- Inventory growth elevated
- Construction activity elevated

Example coordinates:

- (1.0, 0.0)
- (0.7, 0.7)
- (0.7, -0.7)

---

## Expansion

Angle:

45° < θ ≤ 135°

Characteristics:

- Strong demand
- Limited supply pressure
- Tight market conditions
- Positive pricing environment

Example coordinates:

- (-0.7, 0.7)
- (0.0, 1.0)

---

## Recovery

Angle:

135° < θ ≤ 225°

Characteristics:

- Supply pressure largely worked off
- Demand stabilizing
- Early-stage market improvement

Example coordinates:

- (-1.0, 0.0)
- (-0.7, -0.7)

---

## Recession

Angle:

225° < θ ≤ 315°

Characteristics:

- Weak demand
- Excess supply pressure
- Market deterioration

Example coordinates:

- (0.7, -0.7)
- (0.0, -1.0)

---

# Sub-Regime Structure

Each major regime is divided into three equal 30-degree sections.

Total cycle states:

12

---

## Expansion

| Angle Range | State |
|------------|---------|
| 105°–135° | Early Expansion |
| 75°–105° | Mid Expansion |
| 45°–75° | Late Expansion |

---

## Hypersupply

| Angle Range | State |
|------------|---------|
| 15°–45° | Early Hypersupply |
| 345°–15° | Mid Hypersupply |
| 315°–345° | Late Hypersupply |

---

## Recession

| Angle Range | State |
|------------|---------|
| 285°–315° | Early Recession |
| 255°–285° | Mid Recession |
| 225°–255° | Late Recession |

---

## Recovery

| Angle Range | State |
|------------|---------|
| 195°–225° | Early Recovery |
| 165°–195° | Mid Recovery |
| 135°–165° | Late Recovery |

---

# Cycle Direction

Default cycle progression:

Recovery
→ Expansion
→ Hypersupply
→ Recession
→ Recovery

The framework assumes clockwise progression as the normal market path.

The system should not impose hard constraints on future movement, but forecast interpretation should be evaluated relative to this directional structure.

---

# Design Principles

The cycle wheel is intended to:

1. Preserve interpretability.
2. Provide consistent regime classification.
3. Enable comparison across markets.
4. Enable forecasted regime projection.
5. Support future confidence and intensity overlays without altering underlying geometry.

---

# Future Topics

Future documents will define:

- Regime state definitions
- Dimension-to-axis mapping
- Regime scoring
- Confidence scoring
- Forecasted regime classification
- Regime comparison methodology
