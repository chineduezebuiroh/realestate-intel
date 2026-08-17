# ADR-011: Adopt calibrated Supply metric weights

**Status:** Accepted  
**Date:** 2026-08-17

## Context

Native Supply feature calibration was already closed by ADR-010 at Active
Inventory MA12/I4 (40/15/45), Permit Activity MA12/A2 (75/10/15, retaining its
governed lag-6 Short), and Permit Intensity MA12/N4 (40/15/45). This ADR closes
the separate metric-weight layer and therefore closes Supply dimension
calibration. Capital Markets is unchanged by this decision.

The preserved evidence sequence is Phase 1 Feature Anatomy and its date,
identity, calendar alignment, contribution, cancellation, and SVG repairs;
Phase 2 I0-I5/A0-A4/N0-N5 feature-weight calibration and correlation hardening;
the I4/N4 MA9-versus-MA12 check; native-feature promotion; the original S0-S7
metric-weight grid; and the S8/S9 upper-Inventory extension.

## Decision

Adopt human-selected policy **S8**: Active Inventory 0.65, Permit Activity 0.30,
and Permit Intensity 0.05. The decision identity is
`supply_metric_weight_s8_2026_08_17`; `automated_winner=false`. S0 (60/20/20),
S2, S4, S9, and the full S0-S9 evidence remain historical audit records.

S8 preserves Inventory dominance, reduces duplicate Permit-family voting,
retains meaningful Permit Activity responsiveness, makes Permit Intensity a
residual structural signal, and improves reversals, short-horizon whipsaw,
persistence, run length, and cancellation across availability panels, counties,
and periods. Its improvement over S4 was not mere amplitude suppression:
standard deviation increased while reversals, whipsaw, and monthly movement
decreased and persistence increased.

S4 remains the superseded Permit-responsive stability reference. S2 remains the
superseded incremental-Intensity compromise. S9 continued improving stability
but materially weakened Permit responsiveness, increased Inventory dominance,
and produced larger Supply-axis changes without superior economic evidence.
The practical upper boundary is therefore closed at 65%; no 75% test is
warranted for the current architecture.

## Consequences

Native feature policies, transforms, windows, normalization, membership,
missingness renormalization, Supply-to-axis weights, Demand, Labor, Price,
Affordability, and Capital Markets do not change. Supply is fully calibrated and
frozen under `supply_dimension_frozen_s8_2026_08_17` unless a governed revisit
trigger occurs. The next workstream must inventory the existing Capital Markets
production policy before deciding between confirmation/revalidation and a new
bounded recalibration.
