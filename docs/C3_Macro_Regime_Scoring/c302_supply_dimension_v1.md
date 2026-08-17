# C3.02 — Supply Dimension v1

## Purpose

The Supply Dimension measures the degree of supply pressure present within a market.

It is one of the primary inputs into the Supply Axis (X-Axis) of the Macro Regime Engine.

The objective is not to measure housing supply directly, but rather the pressure that current and future supply exert on market conditions.

---

# Core Question

The Supply Dimension answers:

> How much supply pressure exists within this market relative to its own historical experience?

---

# Supply Dimension Structure

The Supply Dimension consists of three subcomponents:

1. Active Inventory
2. Permit Activity
3. Permit Intensity

---

# Subcomponent 1 — Active Inventory

## Purpose

Measures currently available housing supply.

## Primary Source

* Redfin

## Candidate Features

* Inventory Level
* Inventory Short-Term Change
* Inventory Long-Term Change

## Interpretation

Higher inventory generally indicates greater supply pressure.

Higher inventory scores contribute positively to Supply Pressure.

---

# Subcomponent 2 — Permit Activity

## Purpose

Measures future housing supply entering the development pipeline.

## Primary Source

* Census Building Permits Survey

## Candidate Features

* Total Permitted Units Level
* Total Permitted Units Short-Term Change
* Total Permitted Units Long-Term Change

## Interpretation

Higher permit activity indicates increasing future supply pressure.

Higher permit activity scores contribute positively to Supply Pressure.

---

# Subcomponent 3 — Permit Intensity

## Purpose

Measures housing supply creation relative to market size.

## Primary Sources

* Census Building Permits Survey
* Census Population Estimates

## Candidate Metric

Permit Intensity =

(Total Permitted Units ÷ Population) × 1,000

Units Permitted Per 1,000 Residents

## Candidate Features

* Permit Intensity Level
* Permit Intensity Short-Term Change
* Permit Intensity Long-Term Change

## Interpretation

Measures how aggressively a market is expanding relative to its population base.

Higher Permit Intensity scores contribute positively to Supply Pressure.

---

# Normalization

All features are normalized using the standard framework defined in:

B2.2 — Normalization Framework

Feature outputs range from:

-1.00 = Extremely Low

0.00 = Neutral

+1.00 = Extremely High

All normalization is performed relative to the market's own history.

No cross-market normalization is performed.

---

# Frozen Production Weighting

As of 2026-08-17, the frozen production policy is human-selected S8:

| Subcomponent | Configured weight |
| --- | ---: |
| Active Inventory | 65% |
| Permit Activity | 30% |
| Permit Intensity | 5% |

The historical 2026-08-06 S0 policy (60/20/20) remains preserved in its promotion
and freeze records. The S0-S9 campaign established S8 as the preferred balance
between Inventory dominance, reduced duplicate Permit-family voting, stability,
and Permit Activity responsiveness. S9 exposed excessive Inventory dominance.
This was an explicit human decision; the diagnostic did not automatically select
or promote a policy.

# Frozen Feature Contract Within Subcomponents

| Metric | Policy | Level | Short | Long |
| --- | --- | ---: | ---: | ---: |
| Active Inventory | MA12/I4 | 40% | 15% (`lag3`) | 45% (`lag12`) |
| Permit Activity | MA12/A2 | 75% | 10% (**governed `lag6`**) | 15% (`lag12`) |
| Permit Intensity | MA12/N4 | 40% | 15% (`lag3`) | 45% (`lag12`) |

Permit intensity is derived as a raw permit-activity/population ratio using the
carried-forward population first, and that derived raw series is then smoothed
once. It is never derived from smoothed permit activity and is never double
smoothed.

The membership, metric weights, feature definitions, feature weights, and
permit-intensity lineage are frozen together as
`supply_dimension_frozen_s8_2026_08_17`. The historical
`supply_dimension_frozen_v1` record is preserved.
Supply-axis configured weights remain unchanged. A future change requires a new
diagnostic identity, explicit human approval, a new promotion contract, and a
new frozen Supply version. Capital Markets is the next diagnostic workstream;
Capital Markets, affordability, Demand, and later geography work must not change
this policy incidentally.

# Geography Coverage

Expected Coverage:

## CBSA

* Inventory
* Permit Activity
* Permit Intensity

## County

* Permit Activity
* Permit Intensity

Inventory availability may vary.

Missing components are handled through weight re-normalization.

---

# Missing Data Handling

If a feature is unavailable:

* Remove feature
* Re-normalize remaining feature weights

If a subcomponent is unavailable:

* Remove subcomponent
* Re-normalize remaining subcomponent weights

Reduce confidence score accordingly.

No imputation in v1.

---

# Output Contract

The Supply Dimension produces:

* Supply Dimension Score
* Coverage Ratio
* Confidence Score
* Subcomponent Scores
* As-Of Date
* Score Version

---

# Interpretation

Higher Supply Dimension Scores indicate greater supply pressure.

Lower Supply Dimension Scores indicate more constrained supply conditions.

The Supply Dimension does not determine regime classification independently.

It contributes to the Supply Axis (X-Axis), which is evaluated jointly with the Demand Axis (Y-Axis) to determine regime placement.
