# ADR-007 — Adopt MA9 + B3 (40/15/45) for LAUS Feature Construction

## Status

**Accepted (Calibration Program)**

Production promotion is deferred until completion of the Structural/Cyclical calibration program.

---

## Context

The Demand calibration program originally promoted the following LAUS feature architecture:

- Labor Force Membership: LF-IN
- Moving Average: MA9
- Feature Weights:
  - Level = 80%
  - Short = 10%
  - Long = 10%

Subsequent diagnostics suggested that this weighting did not reflect the actual behavior of the underlying labor series.

A dedicated calibration program was initiated to answer four questions independently:

1. Is MA6 or MA9 preferable?
2. Should Level remain dominant?
3. Should Long receive additional emphasis?
4. Does increasing Long eventually stop producing meaningful improvements?

These questions were evaluated without modifying any production scoring logic.

---

## Calibration Program

The calibration proceeded in stages.

### Stage 1

Validated calendar-aware smoothing.

Corrected chronology alignment.

Corrected missing-data handling.

Verified as-of semantics.

---

### Stage 2

Confirmed MA9 as the preferred smoothing window.

MA9 reduced unnecessary reversals while preserving chronology.

---

### Stage 3

Reopened feature weighting.

Evaluated progressively long-heavier feature allocations rather than assuming the production architecture.

Policies included:

| Policy | Level | Short | Long |
| ------ | ----: | ----: | ---: |
| W0 | 25% | 35% | 40% |
| W1 | 40% | 25% | 35% |
| W2 | 50% | 20% | 30% |
| W3 | 60% | 15% | 25% |
| W4 | 70% | 15% | 15% |
| W5 | 80% | 10% | 10% |

This demonstrated that additional Long weight consistently improved chronology quality.

---

### Stage 4

A second bounded calibration explored the region surrounding the apparent optimum.

Balanced family:

| Policy | Level | Short | Long |
| ------ | ----: | ----: | ---: |
| B2 | 45% | 15% | 40% |
| B3 | 40% | 15% | 45% |

Long-heavy family:

| Policy | Level | Short | Long |
| ------ | ----: | ----: | ---: |
| L0 | 35% | 20% | 45% |
| L1 | 35% | 15% | 50% |

Evaluation included:

- seven-county panel
- county robustness
- chronology preservation
- reversal recovery
- whipsaw frequency
- persistence
- latency
- consensus turning points
- Core Demand
- Cyclical block
- metric-level behavior

---

## Findings

The calibration produced several consistent findings.

### 1. Relative feature behavior

Across all governed counties:

- Long behaved as the cleanest economic signal.
- Level behaved primarily as a slowly evolving structural signal.
- Short primarily captured seasonal movement.

This establishes the ordering:

Long > Level > Short

---

### 2. MA9 remains preferred

MA9 consistently reduced unnecessary reversals without materially delaying turning points.

No evidence supported reverting to MA6.

---

### 3. B3 and L1 form a stability plateau

The strongest candidates became:

- MA9 + B3
- MA9 + L1

Their chronologies were nearly identical.

Observed:

- chronology correlation ≈ 0.998
- direction agreement ≈ 99%
- identical consensus turning points
- essentially identical median latency

---

### 4. L1 provides only marginal downstream improvement

L1 slightly reduced:

- Core Demand reversals
- Cyclical reversals

However:

- Employment became marginally noisier.
- Unemployment became marginally noisier.
- The downstream advantage shrank substantially in recent history.

No evidence demonstrated a universally superior architecture.

---

## Decision

Adopt:

- Labor Membership: LF-IN
- Moving Average: MA9
- Feature Policy: B3

Feature weights:

- Level = 40%
- Short = 15%
- Long = 45%

---

## Rationale

B3 represents the front edge of the observed stability plateau.

It captures the demonstrated benefit of emphasizing Long while avoiding unnecessary additional smoothing.

Choosing the least aggressive member of an empirically flat optimum minimizes overfitting while preserving virtually identical downstream chronology.

The calibration therefore supports the design principle:

> Prefer the front edge of a stability plateau over a more aggressive configuration when downstream behavior is statistically indistinguishable.

---

## Consequences

Future Demand calibration shall treat the following as fixed:

- LF-IN
- MA9
- B3 (40/15/45)

Subsequent calibration experiments should not reopen:

- moving-average selection
- feature weighting
- labor-force membership

unless materially new evidence becomes available.

---

## Deferred Work

The remaining Demand calibration question is the Structural/Cyclical balance.

That calibration will hold the newly adopted LAUS architecture fixed while evaluating alternative Structural/Cyclical compositions.

Candidate policies:

| Policy | Structural | Cyclical |
| ------ | ---------: | -------: |
| S15 | 15% | 85% |
| S20 | 20% | 80% |
| S25 | 25% | 75% |
| S30 | 30% | 70% |
| S35 | 35% | 65% |
| S40 | 40% | 60% |

Completion of that experiment will conclude the Demand calibration program and establish the final production Demand architecture.
