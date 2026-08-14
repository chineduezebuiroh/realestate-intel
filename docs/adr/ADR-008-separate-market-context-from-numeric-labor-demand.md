# ADR-008 — Separate Market Context from Numeric Labor Demand

## Status

**Accepted**

This decision closes the county-level Demand architecture calibration program.

Production promotion and final end-to-end validation remain required before the architecture is considered live.

---

## Context

The Macro Regime Engine historically combined two conceptually different groups of signals inside the Demand dimension.

### Structural metrics

Slow-moving measures describing the longer-run state of a geography, including:

- Population
- Income
- GDP

### Cyclical / Labor metrics

Higher-frequency labor-market measures describing current demand direction, including:

- Labor Force
- Employment
- Unemployment Rate

The original architecture blended Structural and Cyclical metrics into a single Core Demand score.

A multi-stage Demand calibration program subsequently investigated:

1. Labor Force membership
2. LAUS smoothing window
3. LAUS feature weighting
4. Structural/Cyclical balance
5. The role of Structural information in the numeric Demand signal

The settled Labor architecture is:

- Labor Force membership = LF-IN
- Moving Average = MA9
- LAUS feature policy = B3
- Level = 40%
- Short = 15%
- Long = 45%

---

## Structural/Cyclical Balance Findings

Structural/Cyclical balance was evaluated across:

- S40/C60
- S35/C65
- S30/C70
- S25/C75
- S20/C80
- S15/C85
- S10/C90
- S5/C95

As Structural weight decreased:

- Cyclical amplitude retention increased
- Chronology correlation to Cyclical increased
- Cancellation decreased
- Turn preservation remained equal or improved
- Reversal and whipsaw behavior did not materially deteriorate

The response remained favorable through the S5 lower boundary.

No interior optimum or stable front edge was identified.

The result therefore indicated that the problem was not simply the amount of Structural weighting.

It raised a more fundamental architecture question:

> Should Structural information be numerically blended into monthly Demand at all?

---

## Structural Role Diagnostic

Three architectures were evaluated.

### Architecture A — S5 Blended Demand

Numeric Demand:

- Labor = 95%
- Structural = 5%

The resulting Demand dimension retained the existing 65% Demand-axis allocation.

### Architecture B — Labor-Only Numeric Demand

Numeric Demand:

- Labor = 100%
- Structural excluded

Demand axis:

- Labor Demand = 65%
- Price = 17.5%
- Affordability = 7.5%
- Capital Markets = 10%

### Architecture C — Labor-Only Numeric Demand + Market Context

Numeric Demand is identical to Architecture B.

Structural metrics are retained as a separate, non-scoring Market Context surface.

Market Context:

- contributes 0% to the Demand-axis score
- is not silently renormalized into other dimensions
- retains chronology
- retains freshness and source-vintage information
- remains available for visualization and drilldown
- provides descriptive long-run market context

---

## Mathematical Equivalence Finding

The diagnostic confirmed that merely moving Structural from inside the Demand dimension to a separately weighted axis component does not create a fundamentally different linear architecture.

For Structural share `s`:

```text
0.65 × ((1-s)L + sS) + O
```

is algebraically equivalent to:

```text
0.65(1-s)L + 0.65sS + O
```

under equivalent availability and normalization semantics.

For S5:

```text
Labor effective axis weight      = 61.75%
Structural effective axis weight = 3.25%
```

Therefore, creating a separately weighted Structural axis component while retaining equivalent weights would only move the same blend one level upward.

Availability differences can break exact parity when effective-weight renormalization differs, but this does not constitute a distinct conceptual architecture.

---

## Empirical Findings

The authoritative seven-county Structural Role Diagnostic found no meaningful numerical benefit from retaining Structural at 5%.

Relative to Labor-only Demand, S5:

- did not reduce 2-month whipsaw
- did not materially reduce 3-month whipsaw
- did not improve persistence
- did not improve turn latency
- missed more validated Cyclical turns
- slightly reduced chronology fidelity to Labor
- introduced Structural/Labor cancellation

Labor-only Demand produced equal or better stability and responsiveness across full-history and recent-period evidence.

Structural/Labor cancellation under S5 was material across governed counties, while Labor-only Demand contains no internal Structural/Labor cancellation by construction.

No compensating stability benefit was identified.

---

## Market Context Finding

The Structural metrics remain useful information.

However, their observed behavior differs materially from monthly Labor Demand.

Population, Income, and county-level GDP:

- update slowly
- are highly persistent
- have limited monthly turning-point suitability
- represent longer-run economic and demographic state

They are therefore better interpreted as:

> **Market Context**

rather than monthly effective Demand direction.

The Structural Role Diagnostic did not establish that these metrics are statistically redundant with Labor.

Instead, it showed that they provide a different class and frequency of information that does not improve the monthly numeric Demand signal when blended into it.

---

## Decision

Adopt **Architecture C**.

### Numeric Demand Axis

| Signal | Weight |
|---|---:|
| Labor Demand | 65.0% |
| Price | 17.5% |
| Affordability | 7.5% |
| Capital Markets | 10.0% |
| Market Context | 0.0% |

### Labor Demand Architecture

- LF-IN
- MA9
- B3
- Level = 40%
- Short = 15%
- Long = 45%

### Market Context

Retain the existing Structural metrics as a first-class but non-scoring surface.

Market Context shall retain:

- metric chronology
- normalized feature evidence
- source dates
- freshness / vintage
- geography coverage
- descriptive state
- visualization / drilldown capability

Market Context shall not:

- contribute numerically to Labor Demand
- contribute numerically to the Demand axis
- cause effective-weight renormalization of scoring dimensions

---

## Rationale

The evidence indicates that Structural and Labor signals answer different questions.

Labor Demand answers:

> What direction is effective housing demand moving now?

Market Context answers:

> What is the longer-run economic and demographic backdrop of this market?

Forcing both into a single scalar creates cancellation without measurable improvement in stability or responsiveness.

Separating the roles improves:

- interpretability
- chronology fidelity
- freshness transparency
- visualization design
- architectural coherence

without discarding Structural information.

---

## Consequences

The existing `demand` scoring dimension should be reinterpreted and, where safe, renamed as **Labor Demand**.

Price, Affordability, and Capital Markets retain their existing Demand-axis weights.

Structural metrics move from numeric Demand scoring to Market Context.

Visualization should display Market Context separately and must not imply that it contributes to the numeric Demand-axis score.

Freshness and source-vintage differences between Labor Demand and Market Context should remain visible to users.

---

## Normalization

This ADR does not change normalization policy.

Feature normalization remains geography-specific and historical.

Production currently uses bounded rolling-percentile normalization according to `config/normalization_registry.csv`.

For LAUS:

- lookback = 120 monthly observations
- minimum history = 36 observations
- percentile clipping = 0.01–0.99

Normalized feature scores therefore typically span approximately:

```text
-0.98 to +0.98
```

before weighted aggregation.

Metrics, dimensions, and axes are not independently renormalized after aggregation.

Observed compression in final axis magnitude is therefore expected when constituent dimensions have different signs or magnitudes.

Production promotion shall include a normalization-range sanity audit to verify this behavior end-to-end.

---

## Hierarchical Decomposition Implication

The visualization layer should make the score hierarchy auditable.

A user should be able to inspect:

```text
Axis
  → Dimension
      → Metric
          → Feature
```

for a selected geography and month, including:

- raw score
- configured weight
- effective weight after availability normalization
- weighted contribution
- sign
- source date / freshness
- cumulative cancellation between components

This should make it possible to explain why, for example, a Labor Demand score near `-0.75` can produce a final Demand-axis score near `-0.25` once Price, Affordability, and Capital Markets are included.

The MVP should preserve the data contract required for this decomposition even if the first visualization exposes only Axis → Dimension → Metric. Feature-level drilldown may be deferred to a later visualization iteration if necessary.

---

## CBSA GDP Caveat

This county-level decision must not automatically generalize to CBSA-level GDP.

County GDP is predominantly annual and behaves as slow Market Context.

CBSA GDP may be available quarterly.

Quarterly GDP may contain useful cyclical information that annual county GDP cannot provide.

Therefore:

> Reassess GDP independently during CBSA calibration before assigning quarterly metro GDP to Market Context or numeric Demand.

No CBSA GDP architecture decision is made by this ADR.

---

## Superseded Architecture

The following architecture is no longer recommended:

- blended Structural/Cyclical Core Demand
- S25/C75 production balance
- S5/C95 lower-bound blend

The final calibrated county-level Demand architecture is:

> **Labor-only numeric Demand + non-scoring Market Context**

---

## Next Steps

1. Promote the settled architecture into production registries and scorer semantics.
2. Preserve Structural evidence under the Market Context surface.
3. Add normalization-range validation to the final promotion audit.
4. Materialize a fresh production candidate run.
5. Perform end-to-end parity and artifact validation.
6. Return to Macro Regime visualization development.
