# ADR-010: Adopt calibrated native Supply feature policies

**Status:** Accepted
**Date:** 2026-08-17

## Context and objective

The governing objective for the native Supply metric family is to preserve meaningful cyclical chronology while remaining responsive, without introducing avoidable noise, whipsaw, or lag. This decision closes **feature-policy calibration only** for `active_inventory`, `permit_activity`, and `permit_intensity`. Supply metric-weight calibration remains pending through the bounded S0–S7 grid, so the Supply dimension is not fully frozen.

The complete native Supply calibration record is preserved in sequence:

1. Supply Phase 1 Feature Anatomy;
2. Phase 1 evidence repairs: native metric dates versus aligned evaluation dates; canonical source identity resolution; raw calendar-month cross-metric alignment; aligned Metric → Dimension contributions; cancellation semantics; and raw-feature SVG repair;
3. Supply Phase 2 Feature-Weight Calibration;
4. Phase 2 correlation hardening; and
5. the final MA9-versus-MA12 check for Active Inventory I4 and Permit Intensity N4.

Historical Supply metric-weight decisions, including the 2026-08-06 60/20/20 freeze, remain intact. The BPS-FINAL-80 Permit Activity promotion also remains superseded history rather than being deleted.

## Decision

Promote exactly these governed production feature policies:

| Native metric | Policy | Level | Short | Long |
|---|---|---:|---:|---:|
| Active Inventory | `MA12 / I4` | MA12 level, 0.40 | MA12 relative to lag 3, 0.15 | MA12 relative to lag 12, 0.45 |
| Permit Activity | `MA12 / A2` | MA12 level, 0.75 | **MA12 relative to lag 6**, 0.10 | MA12 relative to lag 12, 0.15 |
| Permit Intensity | `MA12 / N4` | MA12 level, 0.40 | MA12 relative to lag 3, 0.15 | MA12 relative to lag 12, 0.45 |

No feature transform or window changes are authorized. Permit Activity's governed lag-6 Short exception is explicitly retained; A2 changes only the Level and Long weights from BPS-FINAL-80.

## Human-approved rationale

### Active Inventory

I4 reduces the incumbent's excessive allocation to the noisier Short channel and increases Long chronology/stability. I5 (35/15/50) was the aggressive Long boundary stress and did not justify moving beyond I4. Although MA9 modestly improved some raw-cycle correlation, it materially increased reversals and whipsaw and reduced persistence. `MA12 / I4` is selected; `MA9 / I4` is rejected because it showed no material structural advantage sufficient to overcome that stability cost.

### Permit Activity

BPS-FINAL-80 remained a credible incumbent because Permit Activity is intrinsically noisy even at Level. A2 admits a modest increase in Long information without dismantling its strong Level anchor; more responsive or Long-heavy policies had less attractive noise/stability tradeoffs. Evidence did not warrant reopening MA12, so no separate MA-window experiment was conducted.

### Permit Intensity

Short was the weakest and noisiest incumbent channel. N4 reduces Short and increases Long while preserving useful chronology. N5 (35/15/50) was the aggressive Long boundary stress and did not earn the additional allocation. MA9 improved selected responsiveness/correlation measures but weakened directional fidelity, turning-point preservation, persistence, and/or whipsaw behavior. `MA12 / N4` is selected; `MA9 / N4` is rejected because it showed no material structural advantage sufficient to displace MA12.

## Consequences and governance

The machine-readable record is `config/supply_native_feature_policy_2026_08_17.json`. Governance is `native_supply_feature_calibration=closed`, `metric_weight_calibration=pending`, `production_feature_policy_promoted=true`, `automated_winner=false`, and `human_decision=supply_native_feature_policy_approved`.

Supply metric weights remain Active Inventory 0.60, Permit Activity 0.20, and Permit Intensity 0.20 until S0–S7 is adjudicated. No metric membership, normalization, dimension-to-axis weight, Demand, Price, Affordability, Labor, or Capital Markets policy changes. Capital Markets remains deferred until native Supply calibration, including metric weighting, is complete.
