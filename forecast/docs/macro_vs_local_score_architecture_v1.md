# Macro vs Local Score Architecture v1

## Purpose

Define how macro geography scores and local opportunity scores interact.

## Core Principle

The platform has two scoring engines:

1. Macro Regime Engine
2. Local Opportunity Engine

They are related but not interchangeable.

## Macro Regime Engine

### Geo Levels
- nation
- census region
- state
- metro
- county
- city

### Purpose
Classify broad market cycle position and supply-demand backdrop.

### Output
- macro regime
- macro score
- demand score
- supply score
- affordability score
- liquidity score
- conviction level

## Local Opportunity Engine

### Geo Levels
- ZIP
- neighborhood

### Purpose
Compare local submarkets inside a parent macro context.

### Output
- local opportunity score
- local momentum score
- local liquidity score
- local supply tightness score
- local risk score

## Combined Interpretation

| Macro Context | Local Signal | Interpretation |
|---|---|---|
| Strong | Strong | High-conviction opportunity |
| Strong | Weak | Selective / investigate |
| Weak | Strong | Contrarian / higher-risk opportunity |
| Weak | Weak | Avoid / low priority |

## V1 Rule

Local scores should not fabricate unavailable macro indicators at ZIP/neighborhood level.

Instead:

```text
final_local_interpretation = parent_macro_context + local_redfin_signal
