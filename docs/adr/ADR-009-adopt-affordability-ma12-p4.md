# ADR-009: Adopt MA12/P4 for Affordability

**Status:** Accepted
**Date:** 2026-08-16

## Context

The incumbent `AFF-FW-A` policy weighted Level/Short/Long `50/20/30`. Affordability is derive-first: `price_to_income` is derived from governed raw price and income, and `payment_burden` from those inputs plus canonical raw monthly mortgage rate, before features are constructed. This decision applies exactly to `price_to_income` and `payment_burden`.

The common objective was to preserve meaningful cyclical chronology while remaining responsive, without avoidable noise, whipsaw, or lag. The bounded sequence comprised: (1) Phase 1 Feature Anatomy; (2) Phase 2 bounded feature-weight calibration; (3) raw-cycle orientation repair; (4) P5 Long-boundary stress; and (5) the final MA9 versus MA12 comparison, limited to MA12/P3, MA12/P4, MA9/P3, and MA9/P4.

## Decision

Adopt shared `MA12/P4`: Level 35%, Short 20%, and Long 45% for both metrics. Level is `MA12(raw derived metric)`; Short is `MA12 / lag3(MA12) - 1`; Long is `MA12 / lag12(MA12) - 1`. Derivation remains upstream of smoothing.

## Rationale

Long carries important cyclical chronology, but P5's aggressive 30/20/50 boundary did not justify Long dominance beyond the tested boundary. Level retains economically meaningful Affordability-state information. Short retains useful financing-driven responsiveness, especially for payment burden. MA9's modest gains on some surfaces were not sufficiently material to offset worse stability, direction agreement, and whipsaw. MA12/P4 is the more durable shared policy. Cross-metric divergence was not material enough to justify metric-specific policies. Remaining differences were incremental, not structural; the bounded search is closed.

## Consequences and audit history

`AFF-FW-A` is superseded, not erased. Its evidence and prior decision record remain historical. No metric, dimension, or axis weights; normalization; Price; Labor; Supply; or Capital Markets policy changes. The machine-readable promotion is `config/affordability_policy_promotion_2026_08_16.json`.

## Revisit standard

Reopen only for material evidence: changed source architecture or metric definition; material out-of-sample failure; CBSA-specific evidence invalidating county behavior; or structural data-frequency change. Marginal in-sample differences are insufficient.
