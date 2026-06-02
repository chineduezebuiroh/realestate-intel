# Neighborhood Scoring Philosophy v1

## Purpose

Define how ZIP codes and neighborhoods should be scored when most macro indicators are unavailable at local geography levels.

## Core Principle

Neighborhoods and ZIP codes should be scored primarily using directly observed local housing-market behavior.

## V1 Local Indicator Families

### Price / Value Momentum
- median PPSF trend
- median sale price trend
- YoY growth
- 3m / 6m / 12m momentum

### Supply Tightness
- inventory trend
- active listings trend
- months of supply when available / derived

### Liquidity
- median DOM
- transaction volume
- sale-to-list ratio

### Relative Affordability
- local PPSF relative to parent city
- local sale price relative to parent city
- local affordability gap if income data becomes available

## Parent Macro Context

Each local geo should inherit context from parent geographies:

```text
ZIP/neighborhood → city → county/metro → state → nation
