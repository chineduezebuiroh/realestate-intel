# Final Demand architecture

## Status and authority

**Status:** Human-selected and promoted on 2026-08-12  
**Scope:** Production Core Demand  
**Decision authority:** Explicit human architecture decision, informed by the completed Demand diagnostic sequence  
**Automated winner:** No

## Final production architecture

| Governed element | Production policy |
|---|---|
| Labor Force | **IN** (`laus_labor_force` remains in Cyclical Demand) |
| LAUS moving average | **9 calendar months** |
| LAUS Level / Short / Long | **80 / 10 / 10** |
| Structural / Cyclical | **25 / 75** |

The LAUS definitions apply to labor force, employment, and unemployment rate:
Level is MA9; Short is `MA9 / lag3(MA9) - 1`; Long is
`MA9 / lag12(MA9) - 1`. They retain exact calendar-month construction, the
shared two-thirds coverage threshold, source-month anchoring, no forward-fill
or zero-fill, and their existing sign orientation.

## Human rationale

### Labor Force

Labor Force is retained because removing it reduced cancellation partly by
reducing information content and responsiveness. With the higher Level weight,
the LF-IN cancellation penalty became acceptable.

### LAUS weights

Under the selected MA9 architecture, 80/10/10 produced lower cancellation,
stronger magnitude, fewer reversals and zero crossings, and higher persistence
than 70/15/15.

### MA9

MA9 is the human-preferred balance: MA3 was excessively jagged, MA6 retained
noise, and MA12 introduced greater temporal delay. MA9 provides smoother
chronology while preserving meaningful movement and trending. This was an
explicit human judgment, not an automated statistical winner.

### Structural/Cyclical

S25/C75 retains stronger cyclical expression while preserving broadly the same
economic chronology as S35/C65. Under MA9 and 80/10/10, S25/C75 showed lower
Core Demand cancellation, stronger signal magnitude, slightly lower reversal
rates, and slightly higher persistence, with only a modest zero-crossing
difference.

## Structural turn-expression closeout

Monthly Structural turn expression remains non-applicable as a factorial
evaluation discriminator. Annual and infrequently updating Structural inputs
produce plateau-heavy chronology that is incompatible with the shared
contiguous-month turn detector. The prior lineage and detector-semantics
investigations remain closed and unchanged.

## Governance and closeout

```text
recommendation_state = human_selected
promotion_state = promoted
human_decision = approved
automated_winner = false
production_policy_changed = true
demand_calibration_state = closed
```

The diagnostic factorials and their scenario registries remain historical
decision provenance; this promotion does not rewrite them. The next workstream
is **Macro Regime Visualization MVP**.
