# ADR-013: Adopt Corrected P6 Feature Policy for 10Y-2Y Spread

**Status:** Accepted  
**Date:** 2026-08-18

## Context

ADR-012 selected P7 using source chronology later confirmed to be inverted. Canonical source polarity was repaired without rewriting ADR-012 or its promotion record, and bounded P0-P9 revalidation was performed on governed `treasury_10y - treasury_2y` chronology.

## Decision

Adopt human-approved P6 (60% Level / 5% Short / 35% Long) for `spread_10y_2y`. Retain MA9 level, MA9-minus-lag-3 Short, MA9-minus-lag-12 Long, and positive normalization. Record historical P7 as `revalidation_failed`, without deleting its defect-era evidence.

## Rationale and alternatives

P6 best preserved stability, persistence, corrected raw-cycle responsiveness, material-move agreement, and response magnitude while remaining Level-led. P7 was too Long-dominated and over-damped; P8 and P9 remained too Long-biased. P4 was credible and preserved more detected turning points, but the governed turning-point sample was too sparse to outweigh P6's broader evidence.

## Consequences

The corrected spread calibration is closed and all six native Capital Markets feature policies are valid. Other feature policies, metric weights, axis weights, transforms, normalization, and source repair remain unchanged. F0-F9 family-weight calibration must be rerun against a fresh corrected feature-policy baseline; this ADR makes no family-weight production decision.
