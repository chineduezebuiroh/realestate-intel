# Price / Affordability Production Smoothing Decision

## Current governed status — Price promotion (2026-08-15)

**Price status:** Accepted, promoted, and calibration closed

**Canonical Price policy:** `MA12/P6`

**Price scope:** `median_sale_price` and `median_ppsf` only

**Price feature weights:** Level 35%, Short 20%, Long 45%

The accepted production policy retains the MA12 structural feature construction:

```text
level = trailing MA12(raw metric)
short = trailing MA12(raw metric) / lag3(trailing MA12(raw metric)) - 1
long  = trailing MA12(raw metric) / lag12(trailing MA12(raw metric)) - 1
```

P6 changes only the feature weights of the two direct Price metrics to
`0.35 / 0.20 / 0.45`. All earlier Price weighting and MA candidates are
**superseded by production**, but the historical evidence and artifact references
remain preserved below for auditability and discoverability.

### Closed Price calibration record

1. **Phase 1 — Feature Anatomy** separated structural level, short movement,
   and long movement. It established that level did not need to remain dominant
   and that the long feature carried material cycle information.
2. **Phase 2 — Feature Weight Calibration** held feature construction,
   normalization, Price metric membership and weights, and all other dimensions
   fixed. P6 retained a meaningful structural level and short response while
   assigning the larger role supported by the evidence to long movement.
3. **Turning Point Calibration** tested the bounded finalists under governed
   detector semantics. Detector persistence was not changed, and the review did
   not reopen either the feature-weight or MA search space.
4. **Final MA Calibration** compared only MA9 and MA12 at the P4 and P6
   finalists. No later diagnostic demonstrated a structural advantage for MA9;
   remaining finalist differences were incremental rather than structural; the
   bounded search converged; and further MA exploration was unlikely to change
   the production decision.

MA12/P6 was therefore selected as the durable Price policy. The governed
machine-readable decision is
[`config/price_policy_promotion_2026_08_15.json`](../../config/price_policy_promotion_2026_08_15.json),
and the exact production rows are in
[`config/feature_registry.csv`](../../config/feature_registry.csv).

## Current Affordability production policy — promoted (2026-08-16)

Affordability calibration is closed at human-approved `MA12/P4`, with
Level/Short/Long weights `0.35 / 0.20 / 0.45` for exactly `price_to_income` and
`payment_burden`. The former `AFF-FW-A` 50/20/30 policy is superseded while its
historical evidence remains preserved.

The lineage remains derive-first: raw governed Price and canonical forward-filled
income produce price-to-income; payment burden additionally consumes the canonical
raw monthly `mortgage_30y`; only then are MA12 level, lag-3 ratio, and lag-12 ratio
features constructed. No mortgage pre-smoothing or Capital Markets structural state
crosses this boundary. Normalization, metric/dimension/axis weights, Price, Labor,
Supply, and Capital Markets are unchanged. ADR-009 and
`config/affordability_policy_promotion_2026_08_16.json` govern this closure.

---

## Historical MA12 structural-linked decision record (preserved)

The remainder of this document preserves the earlier smoothing decision and its
evidence references. Its direct-Price `0.50 / 0.25 / 0.25` policy and provisional
Price status are historical and superseded by the current MA12/P6 production
decision above. Its Affordability context remains part of the decision history;
where it says Affordability was pending, that describes the state at the time of
that record and is superseded only by the separately governed `AFF-FW-A`
Affordability decision cited above.


## Decision status

**Status:** Provisional production freeze
**Selected policy:** `MA12 structural-linked`
**Decision scope:** Price and Affordability metric family
**Implementation status:** Direct price metrics promoted on 2026-08-05;
affordability feature weights remain pending
**Revisit condition:** Completion of the broader Demand-axis architecture review

This decision freezes `MA12 structural-linked` as the preferred production
smoothing policy for the Price / Affordability family unless subsequent
integrated Demand-axis testing produces material contrary evidence. The
`settled_ma12_feature_policy_promotion_2026_08_05` production promotion applies
the governed MA12 direct feature family and Alternative A feature weights
(`0.50 / 0.25 / 0.25`) to `median_sale_price` and `median_ppsf` only.
`price_to_income` and `payment_burden` remain pending and unchanged.

“Provisional” does not mean that MA9 and MA12 remain equally preferred.
MA12 is the selected incumbent. A different policy must demonstrate a
material downstream advantage to displace it.

---

## 1. Background

The Price / Affordability family contributes to the Demand axis through:

- Price dimension: 15%
- Affordability dimension: 10%

The remaining Demand-axis construction is:

- Demand dimension: 65%
- Capital Markets dimension: 10%

The smoothing review was initiated because the unsmoothed Price and
Affordability series showed substantial short-term oscillation that reduced
visual interpretability and complicated regime assessment.

The review evaluated whether smoothing could improve structural
interpretability without:

- excessively delaying turning points;
- suppressing economically meaningful shocks;
- materially weakening Demand-axis conviction;
- increasing Demand-axis volatility;
- or worsening Price / Affordability cancellation.

---

## 2. Policies evaluated

The review considered the following Price / Affordability treatments:

1. Current / baseline treatment
2. MA6 structural-linked smoothing
3. MA9 structural-linked smoothing
4. MA12 structural-linked smoothing

The structural-linked design applies a common moving-average level to the
linked Price family and rebuilds dependent affordability metrics from the
smoothed price source while retaining the required income and mortgage-rate
lineage.

The linked family includes, where applicable:

- median sale price;
- median price per square foot;
- payment burden;
- price-to-income;
- Price dimension scores;
- Affordability dimension scores;
- downstream Demand-axis scores.

---

## 3. Evaluation framework

The decision used several complementary reviews rather than a single
optimization statistic.

### 3.1 Visual structural review

Monthly overlays were reviewed for:

- Demand axis;
- Price dimension;
- Affordability dimension;
- median sale price;
- median price per square foot;
- payment burden;
- price-to-income.

The review emphasized:

- smoothness;
- regime persistence;
- turning-point behavior;
- interpretability;
- responsiveness;
- shock retention.

### 3.2 Chronology review

Candidate turning points were compared with the current reference to identify:

- leads;
- lags;
- unmatched turning points;
- missing segment coverage;
- large chronology deviations.

Chronology flags were treated as human-review triggers rather than automatic
candidate failures.

### 3.3 Shock review

Candidate behavior during economically material periods was inspected for
plausibility.

For linked affordability metrics, mortgage-rate changes were not interpreted
in isolation because price changes may offset or amplify financing-cost
changes.

### 3.4 Demand-axis consequence review

MA9 and MA12 were compared using:

- mean absolute Demand-axis score;
- one-month absolute axis change;
- 90th-percentile absolute axis change;
- sign-flip rate;
- near-zero axis rate;
- strong-conviction rate;
- weighted Price / Affordability contribution;
- cancellation amount;
- magnitude-weighted cancellation rate;
- material-cancellation month rate.

---

## 4. Principal findings

### 4.1 All structural challengers improved interpretability

MA6, MA9, and MA12 materially reduced high-frequency oscillation in the
underlying Price and Affordability series relative to the current reference.

The resulting Demand-axis changes were comparatively modest because Price and
Affordability together represent 25% of the axis and because their
contributions frequently offset one another.

### 4.2 MA6 retained more responsiveness but less structural smoothness

MA6 reacted more quickly around some peaks and troughs but retained more
short-term movement.

It did not provide the same degree of structural clarity as MA12.

### 4.3 MA9 was a viable intermediate candidate

MA9 generally fell between MA6 and MA12 in smoothness and responsiveness.

However, “middle ground” was not treated as an independent reason for
production selection. MA9 was required to demonstrate a material downstream
benefit over MA12.

It did not do so.

### 4.4 MA12 provided the clearest structural representation

MA12 produced the smoothest and most easily interpreted Price and
Affordability histories.

It broadened some peaks and troughs and introduced more visible lag than MA6,
but the review did not find evidence that this lag created a material
Demand-axis disadvantage relative to MA9.

---

## 5. Final MA9 versus MA12 inspection

The final comparison used Alameda County, California and the District of
Columbia, covering the common available chronology period.

Aggregate results were:

| Metric | MA9 | MA12 | Interpretation |
|---|---:|---:|---|
| Mean absolute Demand-axis score | 0.135544 | 0.139226 | MA12 slightly stronger |
| Mean absolute one-month axis change | 0.090168 | 0.089887 | Effectively equal; MA12 marginally lower |
| 90th-percentile absolute axis change | 0.173648 | 0.172499 | Effectively equal; MA12 marginally lower |
| Axis sign-flip rate | 0.210938 | 0.226562 | MA9 lower |
| Near-zero axis rate | 0.449612 | 0.441860 | MA12 slightly lower |
| Strong-conviction rate | 0.139535 | 0.158915 | MA12 higher |
| Mean Price / Affordability gross contribution | 0.084371 | 0.094352 | MA12 preserves more signal magnitude |
| Mean absolute Price / Affordability net contribution | 0.045444 | 0.051962 | MA12 preserves more net contribution |
| Mean cancellation rate | 0.384709 | 0.383048 | Effectively equal |
| Magnitude-weighted cancellation rate | 0.458678 | 0.447268 | MA12 slightly lower |
| Material-cancellation month rate | 0.131783 | 0.151163 | MA9 lower |

MA9 produced fewer threshold-qualified material-cancellation months, but it
also generated materially smaller Price / Affordability gross and net
contributions.

The cancellation evidence therefore did not show that MA9 preserved the same
signal while more efficiently reducing offset. Instead, much of the apparent
improvement was associated with a weaker overall Price / Affordability signal.

MA12:

- produced slightly stronger Demand-axis conviction;
- produced a lower near-zero rate;
- produced a higher strong-conviction rate;
- preserved greater Price / Affordability contribution magnitude;
- had effectively identical monthly axis volatility;
- and had a slightly lower magnitude-weighted cancellation rate.

MA9’s lower sign-flip rate was not sufficient to establish a material
downstream advantage.

---

## 6. Decision

### Selected policy

**MA12 structural-linked smoothing**

### Rationale

MA12 is selected because it provides:

1. the clearest structural interpretation;
2. stronger regime persistence;
3. greater Price / Affordability signal magnitude than MA9;
4. slightly stronger Demand-axis conviction;
5. no meaningful one-month Demand-axis volatility penalty;
6. no demonstrated cancellation disadvantage;
7. no compelling downstream reason to prefer MA9.

MA9 remains a valid challenger design, but it did not earn promotion merely by
being an intermediate compromise between MA6 and MA12.

---

## 7. Meaning of the provisional freeze

This decision freezes MA12 as the preferred Price / Affordability production
policy while the broader Demand architecture is evaluated.

The following work remains:

1. attribute remaining Demand-axis volatility by dimension;
2. decompose Demand-dimension volatility by metric and source;
3. determine how much remaining volatility originates in LAUS / labor data;
4. compare metric-level smoothing with dimension-level or axis-level
   attenuation;
5. rebuild the integrated Demand axis using the preferred component policies;
6. evaluate whether the current dimension weights remain appropriate.

The MA12 choice should only be reopened if that integrated review identifies a
material failure such as:

- economically harmful lag;
- materially incorrect regime classification;
- systematic suppression of turning points;
- degraded out-of-sample stability;
- or a challenger producing a clearly superior integrated Demand-axis result.

Minor numerical differences are not sufficient to reopen the decision.

---

## 8. Configuration policy

The machine-readable production configuration now promotes the settled direct
price metrics:

| Canonical metric | Production feature definition | Production feature weights |
|---|---|---:|
| `median_sale_price` | `ma_level` (`12m`); `ma_pct_change` (`12m/lag3m`); `ma_pct_change` (`12m/lag12m`) | `0.50 / 0.25 / 0.25` |
| `median_ppsf` | `ma_level` (`12m`); `ma_pct_change` (`12m/lag3m`); `ma_pct_change` (`12m/lag12m`) | `0.50 / 0.25 / 0.25` |

The governed formulas are:

```text
level = trailing MA12(raw metric)
short = trailing MA12(raw metric) / lag3(trailing MA12(raw metric)) - 1
long  = trailing MA12(raw metric) / lag12(trailing MA12(raw metric)) - 1
```

The linked observation policy continues to substitute structural
`median_sale_price` only into the derived affordability input panel. Direct
source observations are not silently replaced.

This promotion does not change:

- `price_to_income`;
- `payment_burden`;
- Capital Markets;
- Demand or Supply dimension/axis weights;
- metric-to-dimension weights;
- source precedence;
- geography policy;
- normalization method;
- regime geometry, transition, or cancellation logic.

Broader Affordability and Demand-axis decisions remain pending.

---

## 9. Evidence artifacts

Primary review artifacts include:

```text
artifacts/regime/review_exports/price_family_phase2/
```

including:

- `candidate_trigger_evidence.csv`
- `chronology_outliers.csv`
- `review_summary.json`
- `shock_review.csv`
- `smoothing_scorecard.csv`

Final MA9 versus MA12 inspection artifacts include:

```text
artifacts/regime/review_exports/price_family_final_inspection/
```

including:

- `candidate_aggregate_summary.csv`
- `candidate_geo_summary.csv`
- `latest_state.csv`
- `ma9_minus_ma12_comparison.csv`
- `material_cancellation_months.csv`
- `monthly_weighted_contributions.csv`

Supporting candidate chronology and stability artifacts are stored beneath:

```text
artifacts/regime/comparisons/price_family_structural_windows/
```

---

## 10. Decision summary

| Field               | Value                                                            |
| ------------------- | ---------------------------------------------------------------- |
| Family              | Price / Affordability                                            |
| Selected policy     | MA12 structural-linked                                           |
| Status              | Provisional production freeze                                    |
| Displaced finalist  | MA9 structural-linked                                            |
| Primary reason      | Structural interpretability without material downstream penalty  |
| Immediate next step | Demand-axis volatility attribution                               |
| Revisit standard    | Material integrated evidence, not marginal candidate differences |
