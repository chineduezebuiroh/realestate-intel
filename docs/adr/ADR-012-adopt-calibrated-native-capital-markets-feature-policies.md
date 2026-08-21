# ADR-012: Adopt calibrated native Capital Markets feature policies

**Status:** Accepted — 2026-08-18

## Context

Although MW-TEMPERED-C had closed the prior Capital Markets review, evidence-driven revalidation after Supply closure warranted reopening feature calibration. This was not preference-driven retuning. The incumbent used 60/20/20 Level/Short/Long for all six metrics.

The evidence sequence was: (1) Phase 1 Capital Markets anatomy; (2) repair of national-native versus aligned-evaluation geography; (3) cross-axis dtype repair; (4) Phase 2 P0–P7 feature-weight calibration; (5) Phase 2.5 incumbent-reference semantics correction, turning/delay validation, performance hardening, and responsiveness repair; (6) P8/P9 Long-boundary extension; and (7) human selection. Historical diagnostics remain preserved.

## Decision

| Metric | Policy | Level | Short | Long |
|---|---|---:|---:|---:|
| mortgage_30y | P4 | .55 | .10 | .35 |
| mortgage_15y | P2 | .60 | .10 | .30 |
| treasury_10y | P1 | .60 | .15 | .25 |
| fedfunds | P5 | .50 | .10 | .40 |
| spread_10y_2y | P7 | .35 | .10 | .55 |
| spread_10y_fedfunds | P9 | .40 | .10 | .50 |

A common policy was not forced: supported plateaus differed, and registry simplicity was insufficient reason to override metric evidence. The result remains economically coherent: long rates are Level-heavy, Fed Funds balances toward Long, and spreads carry substantially more Long.

## Rationale

- **mortgage_30y P4:** the plateau begins around P4. P5/P8 added no meaningful stability, while P9/P7 weakened chronology/stability. P4 retains a meaningful Level anchor and reduces Short noise.
- **mortgage_15y P2:** its plateau begins earlier. More Long brought insufficient incremental benefit; P2 retains the strongest selected Level anchor while reducing Short.
- **treasury_10y P1:** strongest reason to retain Short and Level. More Long inconsistently improved stability and could weaken chronology/turning fidelity; P1 is the earliest practical plateau.
- **fedfunds P5:** MA3 benefits from less Short and more Long, but not the P8/P9/P7 Long-majority region. P5 is the practical plateau.
- **spread_10y_2y P7:** evidence improved through the aggressive Long boundary. Its intentional Long-majority result is not normalized for aesthetics.
- **spread_10y_fedfunds P9:** the moderate Long majority gave the best tradeoff; P7 did not justify further Level reduction.

## Rejected boundaries

Twenty-percent Short was generally unsupported; five-percent Short was not broadly justified. Long-majority weighting was supported for some spreads, not uniformly. P7 is retained only where supported, not as a universal target.

## Consequences

Native feature calibration is closed and the production policies are frozen pending material evidence. Family metric-weight calibration remains **pending** at the unchanged conceptual 45/10/45 allocation. This ADR changes no Capital Markets metric weights, Demand or Supply axis weights, normalization, transforms, windows, lags, or non-Capital-Markets policy. Capital Markets is not fully frozen until family-weight calibration closes.
