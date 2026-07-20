# Price / Affordability Production Readiness Challenge — Design Document

## Scope & Non-Goals

This design reviews the existing **Price / Affordability** implementation and defines the Production Readiness Challenge (PRC) needed before any transform can be promoted to production.

**Scope metrics**

* `median_sale_price`
* `median_sale_price_per_sqft` / `median_ppsf`
* `price_to_income`
* `payment_burden`

**Scope dimensions**

* Price
* Affordability

**Scope downstream effects**

* Feature normalization
* Metric scoring
* Price and affordability dimension scoring
* Demand-axis recomputation
* Isolation of all non-target metrics, dimensions, and axes

**Non-goals**

* No repository files were modified.
* No code was written.
* No production policy is promoted by this document.
* No PR was created because there were no codebase changes.

## Economic Hypothesis

The objective of this Production Readiness Challenge is not to maximize agreement with the incumbent production policy.

Instead, the objective is to determine whether a structural transform applied to the price root produces a more economically meaningful representation of housing-market regimes.

Hypothesis:

- Housing prices contain significant seasonal and short-term noise that obscures structural market shifts.
- Applying a structural moving-average transform to the price root should reduce non-economic volatility while preserving genuine housing-cycle turning points.
- Downstream affordability metrics should be recomputed from the transformed price root rather than independently smoothed.
- Mortgage-rate and income effects should remain economically interpretable after linked recomputation.
- The resulting Price, Affordability, and Demand signals should become more stable without materially delaying genuine structural transitions.

The Production Readiness Challenge exists to validate or reject this hypothesis.

---

## Repository Findings

### Governance Baseline

The transform governance documents establish that the incumbent policy is **not ground truth**, that challengers must be evaluated against explicit economic objectives, and that promotion requires a deterministic Production Readiness Challenge. `REGIME_TRANSFORM_EXPERIMENT.md` states the incumbent is not ground truth, challengers are evaluated against economic objectives, and promotion requires a deterministic PRC. ​:codex-file-citation[codex-file-citation]{line_range_start=18 line_range_end=23 path=regime/docs/REGIME_TRANSFORM_EXPERIMENT.md git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/docs/REGIME_TRANSFORM_EXPERIMENT.md#L18-L23"}​

ADR-001 defines the required evaluation categories as:

- Structural Fidelity
- Economic Responsiveness
- Regime Stability
- Interpretability
- Operational Cost

It also requires that a challenger satisfy hard gates and meet the minimum readiness score before promotion. ​:codex-file-citation[codex-file-citation]{line_range_start=31 line_range_end=43 path=docs/adr/ADR-001-feature-transform-governance.md git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/docs/adr/ADR-001-feature-transform-governance.md#L31-L43"}​

The Labor PRC now acts as the strongest available template: LAUS MA6 Structural was frozen only after diagnostics, immutable run validation, and acceptance testing. The labor contract is `level = MA6`, `short = MA6 / lag3(MA6) - 1`, and `long = MA6 / lag12(MA6) - 1`. ​:codex-file-citation[codex-file-citation]{line_range_start=123 line_range_end=135 path=regime/docs/REGIME_TRANSFORM_EXPERIMENT.md git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/docs/REGIME_TRANSFORM_EXPERIMENT.md#L123-L135"}​

### Current Experiment Status

The experiment history already identifies the Price Family work as implemented but not production-ready: linked source substitution, derived metric recomputation, and lineage preservation exist, while the Production Readiness Challenge is still pending. ​:codex-file-citation[codex-file-citation]{line_range_start=184 line_range_end=190 path=regime/docs/REGIME_TRANSFORM_EXPERIMENT.md git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/docs/REGIME_TRANSFORM_EXPERIMENT.md#L184-L190"}​

## Economic Success Criteria

A challenger is considered economically successful if it:

- materially reduces seasonal oscillation;
- preserves genuine housing-cycle turning points;
- improves interpretability of Price and Affordability dimensions;
- reduces unnecessary Demand-axis churn;
- preserves mortgage-rate shocks;
- produces a coherent housing-market narrative across all linked metrics.

Similarity to the incumbent production policy is not considered evidence of success.

---

# 1. Current Production Policy

## 1.1 Price Metrics

The current production registry treats price metrics as Redfin source metrics with traditional feature transforms:

| Metric | Level | Short | Long | Dimension |
|---|---:|---:|---:|---|
| `redfin_median_sale_price` | `level_zscore` | `mom_zscore`, 1m | `yoy_zscore`, 12m | price |
| `redfin_median_ppsf` | `level_zscore` | `mom_zscore`, 1m | `yoy_zscore`, 12m | price |

These are explicitly registered in `feature_registry.csv`. ​:codex-file-citation[codex-file-citation]{line_range_start=2 line_range_end=7 path=config/feature_registry.csv git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/config/feature_registry.csv#L2-L7"}​

The metric-dimension registry maps:

* `redfin_median_sale_price` → canonical `median_sale_price`, dimension `price`, subcomponent `sale_price_pressure`, weight `0.5`.
* `redfin_median_ppsf` → canonical `median_ppsf`, dimension `price`, subcomponent `ppsf_pressure`, weight `0.5`. ​:codex-file-citation[codex-file-citation]{line_range_start=1 line_range_end=3 path=config/metric_dimension_registry.csv git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/config/metric_dimension_registry.csv#L1-L3"}​

## 1.2 Affordability Metrics

The current production registry treats affordability as derived metrics:

| Metric | Level | Short | Long | Dimension |
|---|---:|---:|---:|---|
| `derived_price_to_income` | `level_zscore` | `mom_zscore`, 1m | `yoy_zscore`, 12m | affordability |
| `derived_payment_burden` | `level_zscore` | `mom_zscore`, 1m | `yoy_zscore`, 12m | affordability |

These are registered in `feature_registry.csv`. ​:codex-file-citation[codex-file-citation]{line_range_start=124 line_range_end=130 path=config/feature_registry.csv git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/config/feature_registry.csv#L124-L130"}​

The metric-dimension registry maps:

* `derived_price_to_income` → canonical `price_to_income`, dimension `affordability`, subcomponent `price_to_income`, weight `0.50`.
* `derived_payment_burden` → canonical `payment_burden`, dimension `affordability`, subcomponent `payment_burden`, weight `0.50`. ​:codex-file-citation[codex-file-citation]{line_range_start=52 line_range_end=55 path=config/metric_dimension_registry.csv git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/config/metric_dimension_registry.csv#L52-L55"}​

## 1.3 Current Derived Metric Implementation

The derived metric implementation defines monthly inputs as `median_sale_price`, `mortgage_30y`, and `permit_activity`, while annual forward-filled inputs include `median_household_income` and `population`. ​:codex-file-citation[codex-file-citation]{line_range_start=9 line_range_end=18 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/derived_metrics.py#L9-L18"}​

The component contract is:

* `price_to_income` = `median_sale_price` + `median_household_income`
* `payment_burden` = `median_sale_price` + `median_household_income` + `mortgage_30y` ​:codex-file-citation[codex-file-citation]{line_range_start=20 line_range_end=29 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/derived_metrics.py#L20-L29"}​

The monthly panel preserves source lineage and forward-fills annual inputs with original source dates and geographies. ​:codex-file-citation[codex-file-citation]{line_range_start=187 line_range_end=205 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/derived_metrics.py#L187-L205"}​

The implementation explicitly states that no freshness horizons or suppression rules are applied in the derived builder; the function exposes current behavior without changing it. ​:codex-file-citation[codex-file-citation]{line_range_start=412 line_range_end=428 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/derived_metrics.py#L412-L428"}​

`price_to_income` is calculated as `median_sale_price / median_household_income`. ​:codex-file-citation[codex-file-citation]{line_range_start=437 line_range_end=446 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/derived_metrics.py#L437-L446"}​

`payment_burden` is calculated using:

* 30-year mortgage rate
* 360-month term
* 80% principal assumption
* monthly payment divided by monthly household income ​:codex-file-citation[codex-file-citation]{line_range_start=469 line_range_end=499 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/derived_metrics.py#L469-L499"}​

## 1.4 Current Linked Recalculation / Experiment Implementation

The linked price-family experiment currently targets:

* `median_sale_price`
* `median_ppsf`
* `price_to_income`
* `payment_burden` ​:codex-file-citation[codex-file-citation]{line_range_start=18 line_range_end=31 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_features.py#L18-L31"}​

It defines:

* `LEVEL_WINDOW = 12`
* `SHORT_LAG_PERIODS = 3`
* `LONG_LAG_PERIODS = 12` ​:codex-file-citation[codex-file-citation]{line_range_start=33 line_range_end=35 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_features.py#L33-L35"}​

The candidate’s feature contract is documented directly in the experiment:

* `median_sale_price`: `level = MA12(raw price)`, `short = level / lag3(level) - 1`, `long = level / lag12(level) - 1`
* `median_ppsf`: same transform independently
* `price_to_income`: recompute level from substituted MA12 price and preserved income, then same-state lag3/lag12 features
* `payment_burden`: recompute level from substituted MA12 price, preserved income, preserved mortgage rates, and canonical payment formula, then same-state lag3/lag12 features ​:codex-file-citation[codex-file-citation]{line_range_start=430 line_range_end=458 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_features.py#L430-L458"}​

The rolling level is a full-window trailing MA12 with no partial-window values. ​:codex-file-citation[codex-file-citation]{line_range_start=163 line_range_end=202 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_features.py#L163-L202"}​

The same-state features are built as:

* `level = structural level`
* `short = level / lag3(level) - 1`
* `long = level / lag12(level) - 1` ​:codex-file-citation[codex-file-citation]{line_range_start=205 line_range_end=220 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_features.py#L205-L220"}​

The experiment substitutes MA12 `median_sale_price` into the source panel, rebuilds derived metrics, and therefore links affordability metrics to the smoothed sale-price root. ​:codex-file-citation[codex-file-citation]{line_range_start=474 line_range_end=508 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_features.py#L474-L508"}​

---

# 2. Candidate Transform Strategy

The current primary challenger is the MA12 Linked Structural candidate because it is already implemented in the experimental framework.

However, this document does not assume that MA12 will ultimately become the production policy.

The purpose of the Production Readiness Challenge is to determine whether MA12, MA9, MA6, or the incumbent production policy provides the strongest structural representation of housing-market conditions.

## 2.1 Primary Candidate: Linked Price Family MA12 Structural

### Candidate ID

Intended production identifier:

`price_family_ma12_structural_linked`

> **Implementation Note**
>
> The current experimental implementation may still use the identifier
> `price_family_ma12_momentum_lag3`.
> This document uses the intended production identifier
> `price_family_ma12_structural_linked`.
> Registry names and implementation identifiers should be standardized before promotion.

This ID is already used as the challenger ID in the linked comparison module. ​:codex-file-citation[codex-file-citation]{line_range_start=27 line_range_end=30 path=regime/experiments/linked_price_family_comparison.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L27-L30"}​

**Candidate contract**

For `median_sale_price`:

```text
level = MA12(raw median_sale_price)
short = level / lag3(level) - 1
long  = level / lag12(level) - 1
```

For `median_ppsf`:

```text
level = MA12(raw median_ppsf)
short = level / lag3(level) - 1
long  = level / lag12(level) - 1
```

For `price_to_income`:

```text
substitute MA12 median_sale_price into source metric panel
preserve median_household_income
recompute price_to_income = substituted_price / income
level = recomputed derived value
short = level / lag3(level) - 1
long  = level / lag12(level) - 1
```

For `payment_burden`:

```text
substitute MA12 median_sale_price into source metric panel
preserve median_household_income
preserve mortgage_30y
recompute canonical payment_burden formula
level = recomputed derived value
short = level / lag3(level) - 1
long  = level / lag12(level) - 1
```

This candidate is coherent because affordability pressure is not smoothed independently after the fact; instead, affordability is recomputed from a smoothed price root and preserved non-price components. The code’s current linked experiment already does this via source substitution and derived recomputation.
​:codex-file-citation[codex-file-citation]{line_range_start=484 line_range_end=508 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L484-L508"}​

## 2.2 Required Control: Current Production Policy

The challenger must be compared against `macro_regime_v1_bps120_sources`, which the linked comparison module already uses as its baseline run ID.
​:codex-file-citation[codex-file-citation]{line_range_start=23 line_range_end=25 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L23-L25"}​

Current production is not a promotion target; it is the frozen baseline/control.

## 2.3 Optional Research Controls

These should be diagnostic only unless explicitly selected for challenge:

**MA6 Linked Structural**

Same as the primary candidate but with `LEVEL_WINDOW = 6`.

Rationale:

- Labor promoted MA6 Structural.
- Housing prices are noisier and seasonally patterned, but they may not require a full MA12 if responsiveness is too delayed.

Risk:

- May retain more residual seasonality than MA12.

**MA9 Linked Structural**

Same as primary candidate but with `LEVEL_WINDOW = 9`.

Rationale:

- A compromise between MA6 responsiveness and MA12 seasonality suppression.

Risk:

- Added complexity if the team lacks a clear economic reason to choose it.

**MA12 Source-Only, Derived-Direct Control**

Smooth only `median_sale_price` and `median_ppsf`; leave `price_to_income` and `payment_burden` using their current production transforms.

Rationale:

- Demonstrates why linked recomputation is necessary.
- Should not be promoted if affordability becomes internally inconsistent.

## Linked Family Coherence Audit

Because the candidate transforms an interconnected family of metrics rather than a single indicator, the Production Readiness Challenge must evaluate whether the family tells a coherent economic story.

The audit should evaluate:

- median_sale_price
- median_ppsf
- price_to_income
- payment_burden

together rather than independently.

Questions to answer include:

- Do the four metrics move in economically consistent ways?
- Are improvements in affordability explainable by income growth, price moderation, or mortgage-rate changes?
- Are divergences between price and affordability economically plausible?
- Does the linked family collectively describe a coherent housing-market regime?

The objective is not identical movement across all metrics, but consistent economic interpretation.

---

# 3. Diagnostics Required

The diagnostics should be deterministic and should mirror the Labor PRC standard, but adapted to the linked nature of Price / Affordability.

## 3.1 Structural Fidelity Diagnostics

### Required outputs

1. Feature contract verification
	- Validate `level`, `short`, and `long` for all four target metrics.
	- Confirm no partial MA12 level values are emitted.
	- Confirm lag references are same-state, same-metric, same-geo.

2. Linked recomputation verification
	- Confirm `price_to_income` and `payment_burden` are recomputed from substituted MA12 `median_sale_price`.
	- Confirm income and mortgage inputs are preserved, not smoothed unless explicitly designed.
	- Confirm derived lineage records source dates, source geographies, component ages, and carry-forward flags.

3. Formula parity
	- `price_to_income` must equal substituted price divided by household income.
	- `payment_burden` must equal the canonical mortgage formula already implemented.

4. Metric coverage audit
	- Coverage by metric, geo, date.
	- First valid date per metric and geo.
	- Difference in valid-row counts versus baseline.
	- Dropout caused by full-window MA12 and lag requirements.

## 3.2 Economic Responsiveness Diagnostics

The challenge should avoid treating baseline similarity as success. Instead, it should answer whether the transform preserves meaningful market regime signals.

### Required outputs

1. Turning-point lag audit
	- Identify major local peaks/troughs in raw `median_sale_price`, raw `median_ppsf`, challenger levels, metric scores, price dimension, affordability dimension, and demand axis.
	- Report median and max lag in months.
	- Stratify by geography and period.

2. Shock preservation audit
	- Review known stress periods:
		- 2008–2012 housing correction where available.
		- 2020 pandemic disruption.
		- 2022–2024 mortgage-rate shock.
	- Confirm challenger does not suppress structural inflection beyond acceptable thresholds.

3. Affordability response decomposition
	- Decompose `payment_burden` changes into price-root contribution, mortgage-rate contribution, and income contribution.
	- This is essential because smoothing the price root while preserving mortgage rates may intentionally retain fast affordability shocks from rate moves.

4. Regime sign consistency
	- Check whether major price and affordability score sign changes occur in economically interpretable periods.
	- Challenger should not flip signs because of smoothing artifacts.

## 3.3 Regime Stability Diagnostics

The linked comparison module already computes several relevant stability summaries, including month-over-month absolute changes, sign flip rates, near-zero rates, correlations, and seasonal summaries. The stability summary function aggregates row counts, date ranges, value standard deviation, mean absolute 1-month change, p90 absolute change, maximum absolute change, sign flip rate, and near-zero rate.
​:codex-file-citation[codex-file-citation]{line_range_start=560 line_range_end=640 path=regime/experiments/linked_price_family_comparison.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L560-L640"}​

Required outputs:

1. Feature score volatility
	- Mean absolute 1-month change.
	- P90 absolute 1-month change.
	- Maximum absolute 1-month change.
	- Sign flip rate.

2. Metric score volatility
	- Same statistics for all four target metrics.

3. Dimension volatility
	- Price dimension score.
	- Affordability dimension score.

4. Demand-axis volatility
	- Demand-axis score.
	- Demand-axis near-origin rate.
	- Strong-conviction rate.

The comparison module already includes a demand conviction aggregation with near-origin and strong-conviction metrics.
​:codex-file-citation[codex-file-citation]{line_range_start=1408 line_range_end=1440 path=regime/experiments/linked_price_family_comparison.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L1408-L1440"}​

5. Seasonality suppression
	- Calendar-month profile of absolute monthly changes.
	- Ratio of seasonal spread challenger vs baseline.
	- This should be measured separately for raw features, normalized features, metric scores, dimension scores, and demand axis.

The comparison module already has a seasonality summary grouped by calendar month.
​:codex-file-citation[codex-file-citation]{line_range_start=776 line_range_end=802 path=regime/experiments/linked_price_family_comparison.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L776-L802"}​

## 3.4 Interpretability Diagnostics

Required outputs:

1. Lineage artifacts
	- Source substitution lineage.
	- Derived metric lineage.
	- Price-family feature lineage.

The linked comparison returns these lineage artifacts.
​:codex-file-citation[codex-file-citation]{line_range_start=1442 line_range_end=1452 path=regime/experiments/linked_price_family_comparison.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L1442-L1452"}​

2. One-row reproducibility examples
	- For each target metric, select a geography/date and show:
		- raw source values
		- MA12 level
		- lag reference
		- calculated feature value
		- normalized feature score
		- metric score contribution

3. Narrative interpretability
	- Confirm whether Price is still interpretable as pricing pressure.
	- Confirm whether Affordability is still interpretable as household affordability pressure.
	- Confirm whether demand-axis effects are explainable as intended second-order impacts.

## 3.5 Operational Cost Diagnostics

Required outputs:

1. Runtime for challenger feature build.
2. Runtime for normalization and downstream recomputation.
3. Artifact row counts.
4. Memory use if available.
5. Failure mode inventory:
	- missing `median_sale_price`
	- missing `median_ppsf`
	- missing income
	- missing mortgage rates
	- duplicated metric rows
	- non-finite outputs

The linked feature builder validates required columns, invalid dates, and duplicate canonical metric rows.
​:codex-file-citation[codex-file-citation]{line_range_start=93 line_range_end=160 path=regime/experiments/linked_price_family_comparison.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L93-L160"}​

---

# 4. Visual Review Plan

Visual review must be a required PRC stage, not an optional appendix.

## 4.1 Focus Geographies

Use the existing linked comparison focus geographies as the minimum required set:
- `district_of_columbia_dc__county`
- `alameda_county_ca__county`

These are already configured as focus geographies in the linked comparison module.
​:codex-file-citation[codex-file-citation]{line_range_start=32 line_range_end=35 path=regime/experiments/linked_price_family_comparison.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L32-L35"}​

Add at least four more before final readiness:

1. High-cost coastal market.
2. Lower-cost interior market.
3. Volatile boom/bust market.
4. Sparse/noisy Redfin history market.

## 4.2 Required Charts

For each focus geography:

### Price charts

1. Raw `median_sale_price` vs MA12 level.
2. Raw `median_ppsf` vs MA12 level.
3. Baseline vs challenger normalized price features.
4. Baseline vs challenger metric scores.
5. Baseline vs challenger Price dimension score.

### Affordability charts

1. Baseline vs challenger `price_to_income`.
2. Baseline vs challenger `payment_burden`.
3. Income source age / carry-forward panel.
4. Mortgage-rate overlay for payment_burden.
5. Baseline vs challenger Affordability dimension score.

### Downstream charts

1. Baseline vs challenger Demand-axis score.
2. Demand-axis score delta.
3. Near-origin transition dates.
4. Strong-conviction transition dates.

### Diagnostic visuals

1. Calendar-month seasonality heatmap.
2. Month-over-month absolute change distribution.
3. Sign-flip timeline.
4. Coverage / missingness ribbon.
5. Component lineage age chart for derived metrics.

## 4.3 Visual Review Questions

Each chart packet should answer:

1. Does MA12 remove unwanted seasonal sawtooth behavior?
2. Does the challenger still respond to true structural turns?
3. Does affordability remain responsive to mortgage-rate shocks?
4. Are downstream Demand-axis moves explainable and bounded?
5. Are any sign flips artifacts of normalization or missingness?
6. Does full-window MA12 create unacceptable early-history loss?

---

# 5. Chronological Review Plan

The chronological review should be modeled after the Labor closeout, where chronology review was one of the explicit diagnostics performed before acceptance.
{add codex-file-citation to lines 165 through 172 of "regime/docs/REGIME_TRANSFORM_EXPERIMENT.md"}

## 5.1 Period Segments

Review the full available history but explicitly segment:

1. Pre-GFC / early history, where available.
2. GFC and housing correction, approximately 2007–2012.
3. Recovery and expansion, approximately 2013–2019.
4. Pandemic disruption, 2020–2021.
5. Rate shock / affordability shock, 2022–2024.
6. Most recent available period in the immutable run.

## 5.2 Chronology Artifacts

For each target geography and segment:

1. Baseline price metric score chronology.
2. Challenger price metric score chronology.
3. Baseline affordability metric score chronology.
4. Challenger affordability metric score chronology.
5. Price dimension chronology.
6. Affordability dimension chronology.
7. Demand-axis chronology.
8. Regime-coordinate chronology if available.

## 5.3 Required Chronology Tables

For each focus geography:

| Date | Baseline Price | Challenger Price | Baseline Affordability | Challenger Affordability | Baseline Demand Axis | Challenger Demand Axis | Interpretation |
| ---- | -------------: | ---------------: | ---------------------: | -----------------------: | -------------------: | ---------------------: | -------------- |

## 5.4 Turning-Point Lag Standards

The PRC should compute:

- Lag of challenger level vs raw series.
- Lag of challenger metric score vs baseline metric score.
- Lag of Price dimension turn vs baseline.
- Lag of Affordability dimension turn vs baseline.
- Lag of Demand-axis turn vs baseline.

Important: Lag is not automatically a failure. As with Labor, a lag can be accepted if the transform is intentionally structural rather than tactical. Labor accepted a documented maximum Demand-axis lag because the transform was used as a structural regime signal and reduced churn.
{add codex-file-citation to lines 176 through 179 of "regime/docs/REGIME_TRANSFORM_EXPERIMENT.md"}

---

# 6. Production Readiness Challenge Scorecard

The scorecard should follow ADR-001 categories.
{add codex-file-citation to lines 31 through 37 of "docs/adr/ADR-001-feature-transform-governance.md"}

The Production Readiness Challenge evaluates three related but distinct outcomes:

1. Price representation
2. Affordability representation
3. Linked family behavior

These should be considered separately before determining the overall readiness score.

## 6.1 Scoring Weights

| Category                | Weight | Hard Gate? |
| ----------------------- | -----: | ---------: |
| Structural Fidelity     |    25% |        Yes |
| Economic Responsiveness |    25% |        Yes |
| Regime Stability        |    25% |        Yes |
| Interpretability        |    15% |        Yes |
| Operational Cost        |    10% |        Yes |

Minimum overall readiness score: **0.85**

Hard gate failures allowed: **0**

## 6.2 Category Rubrics

### A. Structural Fidelity — 25%

| Check                                                            | Points |
| ---------------------------------------------------------------- | -----: |
| MA12 level exactly matches full-window trailing mean             |      5 |
| Short/long features exactly match lag3/lag12 same-state formulas |      5 |
| Derived metrics recompute from substituted price root            |      5 |
| Lineage is complete and non-ambiguous                            |      4 |
| Coverage loss is quantified and acceptable                       |      3 |
| No duplicate/non-finite production feature rows                  |      3 |

Hard gate failures:

- Any formula mismatch.
- Any duplicate production feature key.
- Any infinity in production feature output.
- Any derived metric calculated from unsmoothed price when challenger requires substituted price.

### B. Economic Responsiveness — 25%

| Check                                                    | Points |
| -------------------------------------------------------- | -----: |
| Major price turning points remain visible                |      5 |
| Affordability responds to mortgage-rate shock            |      5 |
| 2020–2024 chronology is economically interpretable       |      5 |
| Challenger does not suppress real structural inflections |      4 |
| Lag is quantified and accepted                           |      3 |
| Price and affordability remain conceptually distinct     |      3 |

Hard gate failures:

- Challenger misses a major structural housing correction or affordability shock.
- Payment burden no longer responds materially to mortgage-rate changes.
- Chronological review finds unexplained inversions.

### C. Regime Stability — 25%

| Check                                                        | Points |
| ------------------------------------------------------------ | -----: |
| Feature volatility materially improves or remains acceptable |      5 |
| Metric score volatility improves or remains acceptable       |      5 |
| Price dimension churn improves or remains acceptable         |      4 |
| Affordability dimension churn improves or remains acceptable |      4 |
| Demand-axis near-origin churn does not worsen materially     |      4 |
| Seasonality is reduced materially                            |      3 |

Hard gate failures:

- Challenger increases unexplained sign flips.
- Challenger causes non-target downstream drift.
- Demand-axis instability materially worsens without economic rationale.

### D. Interpretability — 15%

| Check                                                   | Points |
| ------------------------------------------------------- | -----: |
| Lineage artifacts explain every linked value            |      4 |
| One-row reproducibility examples pass                   |      4 |
| Visual review supports narrative                        |      3 |
| Documentation clearly explains accepted lags/trade-offs |      2 |
| Candidate name and registry semantics are unambiguous   |      2 |

Hard gate failures:

- Reviewer cannot reproduce a target feature from lineage.
- Candidate behavior cannot be explained in terms of price pressure and affordability pressure.

### E. Operational Cost — 10%

| Check                           | Points |
| ------------------------------- | -----: |
| Runtime acceptable              |      3 |
| Artifact size acceptable        |      2 |
| Deterministic rerun behavior    |      2 |
| Smoke tests are stable          |      2 |
| Failure messages are actionable |      1 |

Hard gate failures:

- Non-deterministic outputs.
- Runtime or memory cost blocks routine production.
- Smoke test failures.

Developer Experience

Although not scored independently, reviewers should document:

- ease of debugging;
- clarity of lineage;
- maintainability of the implementation;
- readability of smoke-test outputs;
- reproducibility of derived calculations.

A production transform should be understandable and maintainable in addition to being economically valid.

---

# 7. Immutable Acceptance Criteria

The acceptance criteria should be immutable and should mirror the LAUS closeout pattern: immutable run creation, artifact/hash verification, formula validation, non-target parity, intended downstream changes reported, and acceptance artifact retained. Labor’s acceptance test verifies immutable runs, formula matches, non-LAUS parity, non-Demand parity, and intended Demand/coordinate/regime changes.
{add codex-file-citation to lines 151 through 164 of "regime/docs/REGIME_TRANSFORM_EXPERIMENT.md"}

## 7.1 Required Immutable Run IDs

Proposed baseline:

- `macro_regime_v1_bps120_sources`

Proposed challenger:

- `macro_regime_v1_bps120_price_family_ma12`

Proposed experiment ID:

- `production_price_family_ma12`

## 7.2 Required Acceptance Artifact

Proposed path:

- `artifacts/regime/comparisons/price_family_ma12_immutable_acceptance/acceptance_summary.json`

## 7.3 Immutable Hard Criteria

The candidate is accepted only if all criteria are true:

1. Both immutable runs exist and pass artifact/hash verification.
2. Persisted `median_sale_price` features match:
	- `level = MA12(raw median_sale_price)`
	- `short = level / lag3(level) - 1`
	- `long = level / lag12(level) - 1`
3. Persisted `median_ppsf` features match:
	- `level = MA12(raw median_ppsf)`
	- `short = level / lag3(level) - 1`
	- `long = level / lag12(level) - 1`
4. Persisted `price_to_income` features match recomputation from substituted MA12 `median_sale_price` and preserved income.
5. Persisted `payment_burden` features match recomputation from substituted MA12 `median_sale_price`, preserved income, preserved mortgage rates, and the canonical mortgage formula.
6. Non-price-family raw features remain exactly unchanged.
7. Non-price-family normalized features remain exactly unchanged.
8. Non-price-family metric scores remain exactly unchanged.
9. Non-price and non-affordability dimensions remain exactly unchanged.
10. Non-Demand axes remain exactly unchanged.
11. Supply axis remains exactly unchanged.
12. Intended Price, Affordability, Demand-axis, coordinate, and regime changes are reported rather than treated as failures.
13. No duplicate rows exist at any production key grain.
14. No non-finite target feature values enter normalization.
15. Derived lineage contains one row per derived observation per component.
16. Component source ages are non-negative.
17. Full-window MA12 early-history row loss is documented and accepted.
18. Visual review packet is complete.
19. Chronological review packet is complete.
20. Overall readiness score is at least **0.85**.
21. Failed hard gates equal **0**.

The current linked comparison already includes an isolation audit that checks non-target normalized features, non-target metric scores, non-price/affordability dimensions, non-Demand axes, and Supply-axis exact parity.
​:codex-file-citation[codex-file-citation]{line_range_start=805 line_range_end=923 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L805-L923"}​

---

# 8. Risks

## 8.1 MA12 May Be Too Slow

The inventory history rejected MA12 Structural because it was over-smoothed and delayed structural response.
{add codex-file-citation to lines 106 through 119 of "regime/docs/REGIME_TRANSFORM_EXPERIMENT.md"}

Price data may justify MA12 more than inventory, but this is not guaranteed. The PRC must prove the lag is acceptable.

## 8.2 Affordability Could Become Internally Mixed-Frequency

The linked candidate smooths the price root but preserves income and mortgage rates. That is intentional, but it creates mixed responsiveness:

- price changes become structural;
- mortgage-rate changes remain fast;
- income changes remain annual / forward-filled.

This is economically plausible, but the PRC must show `payment_burden` remains interpretable.

## 8.3 Annual Income Carry-Forward Can Dominate Lineage Age

Annual `median_household_income` is forward-filled in the derived panel.
​:codex-file-citation[codex-file-citation]{line_range_start=187 line_range_end=205 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L187-L205"}​

Because no freshness horizon is applied in the current derived builder, stale income can drive derived affordability values unless governed elsewhere. The PRC should audit source age and carry-forward rates for every derived observation. The current implementation explicitly says no freshness horizons or suppression rules are applied.
​:codex-file-citation[codex-file-citation]{line_range_start=412 line_range_end=428 path=regime/derived_metrics.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L412-L428"}​

#8.4 Full-Window MA12 Reduces Early-History Coverage

The MA12 level emits no partial-window values.
​:codex-file-citation[codex-file-citation]{line_range_start=163 line_range_end=202 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L163-L202"}​

Then short and long features require additional lag references. This may materially shrink valid early history, especially for geographies with sparse Redfin availability.

## 8.5 `median_ppsf` Is Smoothed Independently

The candidate smooths `median_ppsf` independently rather than deriving it from smoothed price and square footage. This is probably correct because Redfin PPSF is its own observed metric, but the PRC should verify no contradictory narrative emerges where price and PPSF diverge due to data composition.

## 8.6 Downstream Demand-Axis Effects May Be Non-Local

Price and affordability both affect demand-axis behavior through downstream scoring. The linked comparison targets Price and Affordability dimensions and the Demand axis.
​:codex-file-citation[codex-file-citation]{line_range_start=37 line_range_end=44 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L37-L44"}​

The PRC must ensure changes are intended and isolated.

## 8.7 Registry Semantic Risk

Current registry rows still describe transforms as `level_zscore`, `mom_zscore`, and `yoy_zscore` for price and affordability.
​:codex-file-citation[codex-file-citation]{line_range_start=2 line_range_end=7 path=config/feature_registry.csv git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/config/feature_registry.csv#L2-L7"}​

​:codex-file-citation[codex-file-citation]{line_range_start=124 line_range_end=130 path=config/feature_registry.csv git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/config/feature_registry.csv#L124-L130"}​

If the candidate is promoted, registry semantics must be updated to avoid misrepresenting production behavior.

---

# 9. Suggested Smoke Tests

No smoke tests were run for this design-only review. The following are suggested for implementation after approval of the PRC design.

## 9.1 Formula / Feature Contract Smoke Test

Proposed name:

- `scripts/smoke_tests/50_59/51_price_family_ma12_feature_contract.py`

Required checks:

- `median_sale_price` MA12 level formula.
- `median_ppsf` MA12 level formula.
- lag3 short formula.
- lag12 long formula.
- no partial-window MA12 outputs.
- no duplicate feature rows.
- no non-finite feature values.

## 9.2 Linked Derived Recalculation Smoke Test

Proposed name:

- `scripts/smoke_tests/50_59/52_price_family_linked_derived_recalculation.py`

Required checks:

- substituted MA12 `median_sale_price` is used in `price_to_income`.
- substituted MA12 `median_sale_price` is used in `payment_burden`.
- income is preserved.
- mortgage rate is preserved.
- canonical formulas match.
- derived lineage has the expected component rows.

## 9.3 Isolation Smoke Test

Proposed name:

- `scripts/smoke_tests/50_59/53_price_family_isolation_audit.py`

Required checks:

- non-price-family normalized features exact-match baseline.
- non-price-family metric scores exact-match baseline.
- non-price/affordability dimensions exact-match baseline.
- non-Demand axes exact-match baseline.
- Supply axis exact-matches baseline.

The linked comparison’s isolation audit already encodes the right scopes for these checks.
​:codex-file-citation[codex-file-citation]{line_range_start=805 line_range_end=923 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L805-L923"}​

## 9.4 Chronology Smoke Test

Proposed name:

- `scripts/smoke_tests/50_59/54_price_family_chronology_review.py`

Required checks:

- emits chronology tables for focus geographies.
- computes turning-point lags.
- flags unexplained lag outliers.
- produces period-segment summaries.

## 9.5 Stability / Seasonality Smoke Test

Proposed name:

- `scripts/smoke_tests/50_59/55_price_family_stability_seasonality.py`

Required checks:

- feature volatility summaries.
- metric volatility summaries.
- dimension volatility summaries.
- Demand-axis volatility summaries.
- calendar-month seasonality summaries.

The linked comparison already returns stability, correlation, seasonality, and demand-conviction outputs that can become smoke-test artifacts.
​:codex-file-citation[codex-file-citation]{line_range_start=1442 line_range_end=1505 path=regime/experiments/linked_price_family_features.py git_url="https://github.com/chineduezebuiroh/realestate-intel/blob/main/regime/experiments/linked_price_family_comparison.py#L1442-L1505"}​

## 9.6 Immutable Acceptance Smoke Test

Proposed name:

- `scripts/smoke_tests/50_59/56_price_family_ma12_immutable_acceptance.py`

Required checks:

- immutable baseline run exists.
- immutable challenger run exists.
- artifacts pass hash verification.
- formulas match persisted production features.
- non-target parity holds.
- intended target downstream changes are reported.
- writes acceptance summary JSON.

## Implementation Status

### Phase 1 — Feature Contract and Linked Derived Recalculation

Status: Complete and locally validated

Merged to `main`: 2026-07-18

Implemented:

- linked MA12 structural feature-contract validation;
- preferred and legacy experiment identifier parity;
- linked `price_to_income` recalculation;
- linked `payment_burden` recalculation;
- complete derived-component lineage validation;
- perturbation propagation and isolation validation;
- deterministic artifact generation;
- lazy-import regression coverage.

Validation completed:

- `51_price_family_ma12_feature_contract.py`
- `52_price_family_linked_derived_recalculation.py`
- `41_linked_price_family_comparison.py`
- `40_linked_price_family_features.py`
- `39_linked_price_family_recalculation.py`
- `21_derived_input_lineage.py`
- targeted `py_compile`

Outcome:

- all tests passed locally;
- no unexpected tracked-file changes;
- non-target comparison scopes exact-matched baseline;
- Phase 1 approved and merged.

### Phase 2 — Chronology, Turning-Point Lag, Stability, and Seasonality Diagnostics

Status: Implemented; pending local validation

Implemented scope:

- chronological baseline-versus-challenger review for the linked price family;
- explicit historical period coverage summaries, including unavailable segments;
- deterministic turning-point lag diagnostics for target metric, Price dimension,
  Affordability dimension, and Demand-axis scores;
- affordability-shock evidence flags for the 2022-2024 mortgage-rate period;
- stability summaries for target feature components, target metric scores, Price
  and Affordability dimensions, and the Demand axis;
- calendar-month seasonality profiles and spread summaries;
- Phase 1 isolation-audit reuse for non-target exact-match expectations; and
- deterministic rerun checks for all required Phase 2 artifacts.

Smoke tests added:

- `scripts/smoke_tests/50_59/53_price_family_chronology_review.py`
- `scripts/smoke_tests/50_59/54_price_family_stability_seasonality.py`

Artifact paths:

- `artifacts/regime/comparisons/price_family_ma12_structural_linked/phase2_chronology/`
- `artifacts/regime/comparisons/price_family_ma12_structural_linked/phase2_stability_seasonality/`

Phase 2 chronology now also persists `affordability_shock_summary.csv` for
compact 2022-2024 mortgage-rate shock review evidence.

Important limitation:

- Phase 2 is diagnostic only. It does not constitute immutable acceptance, final
  PRC scorecard adjudication, registry promotion, or production policy
  replacement.

Compatibility note:

- fixture month-end dates use `freq="M"` for compatibility with the
  repository’s supported pandas environment rather than the newer `"ME"` alias.
  
---

# Recommended PRC Decision Flow

1. Freeze baseline and challenger IDs.
2. Run formula contract diagnostics.
3. Run linked derived recomputation diagnostics.
4. Run isolation diagnostics.
5. Run volatility / seasonality diagnostics.
6. Run chronology review.
7. Generate visual packet.
8. Score against PRC scorecard.
9. If score ≥ 0.85 and hard gates = 0, create immutable production run.
10. Run immutable acceptance smoke test.
11. Only then update registries, ADR, roadmap, and experiment history.

The Production Readiness Challenge is complete only after:

- implementation;
- diagnostics;
- chronology review;
- visual review;
- Production Readiness scoring;
- immutable production run;
- immutable acceptance validation; and
- documentation updates.

No production registry changes should occur before all stages are successfully completed.

---

# Commands Used

Inspection commands
- `pwd && rg --files -g 'AGENTS.md' -g 'REGIME_TRANSFORM_EXPERIMENT.md' -g 'ADR-001-feature-transform-governance.md' -g '*price*' -g '*afford*' -g '*metric*' -g '*registry*' -g '*smooth*' -g '*transform*' | sed -n '1,200p'`
- `find .. -name AGENTS.md -print`
- `sed -n '1,240p' regime/docs/REGIME_TRANSFORM_EXPERIMENT.md && printf '\n---ADR---\n' && sed -n '1,240p' docs/adr/ADR-001-feature-transform-governance.md`
- `sed -n '1,260p' regime/derived_metrics.py && printf '\n---linked features---\n' && sed -n '1,260p' regime/experiments/linked_price_family_features.py && printf '\n---comparison---\n' && sed -n '1,220p' regime/experiments/linked_price_family_comparison.py`
- `sed -n '260,620p' regime/derived_metrics.py && printf '\n---features cont---\n' && sed -n '120,360p' regime/experiments/linked_price_family_features.py && printf '\n---compare selected---\n' && sed -n '220,520p' regime/experiments/linked_price_family_comparison.py`
- `sed -n '360,760p' regime/experiments/linked_price_family_features.py && sed -n '520,980p' regime/experiments/linked_price_family_comparison.py`
- `sed -n '980,1400p' regime/experiments/linked_price_family_comparison.py`
- `rg -n "median_sale_price|median_ppsf|price_to_income|payment_burden|affordability|price" config/feature_registry.csv config/metric_dimension_registry.csv config/source_metric_registry.csv config/metric_metadata.csv config/indicator_regime_registry.csv | sed -n '1,240p'`
- `sed -n '1,220p' sources/redfin/transform.py && printf '\n---smoothing policy---\n' && sed -n '1,240p' regime/experiments/smoothing_policy.py && printf '\n---generic smoothing---\n' && sed -n '1,220p' regime/experiments/smoothing_features.py && printf '\n---docs price/afford---\n' && sed -n '1,220p' docs/C3_Macro_Regime_Scoring/c305_price_dimension_v1.md && sed -n '1,220p' docs/C3_Macro_Regime_Scoring/c303_affordability_dimension_v1.md`
- `nl -ba regime/derived_metrics.py | sed -n '1,240p;240,520p' > /tmp/derived.nl`
- `nl -ba regime/experiments/linked_price_family_features.py | sed -n '1,520p' > /tmp/linked.nl`
- `nl -ba regime/experiments/linked_price_family_comparison.py | sed -n '1,1800p' > /tmp/compare.nl`
- `nl -ba config/feature_registry.csv | sed -n '1,140p' > /tmp/feature_reg.nl`
- `nl -ba config/metric_dimension_registry.csv | sed -n '1,70p' > /tmp/metric_dim.nl`
- `nl -ba regime/docs/REGIME_TRANSFORM_EXPERIMENT.md | sed -n '1,200p' > /tmp/exp.nl`
- `nl -ba docs/adr/ADR-001-feature-transform-governance.md | sed -n '1,120p' > /tmp/adr.nl`

Testing

- ⚠️ No tests run — design-only review requested; no code or repository files were modified.

---

## Phase 2 Review / Adjudication Layer

`regime/experiments/price_family_phase2_review.py` adds a deterministic review
packet for the existing `price_family_ma12_structural_linked` challenger. The
review layer consumes the persisted Phase 2 chronology and stability /
seasonality CSV artifacts under:

- `artifacts/regime/comparisons/price_family_ma12_structural_linked/phase2_chronology/`
- `artifacts/regime/comparisons/price_family_ma12_structural_linked/phase2_stability_seasonality/`

It does **not** rebuild, duplicate, or recompute those diagnostic pipelines. Its
role is to transform their persisted outputs into compact adjudication artifacts
under:

- `artifacts/regime/comparisons/price_family_ma12_structural_linked/phase2_review/`

The review determines whether Phase 2 evidence indicates that an MA6 linked
challenger deserves finalist evaluation, whether MA12 remains the current
finalist, whether evidence is insufficient, or whether diagnostic coverage /
contract issues block adjudication.

This review is intentionally bounded:

- it does **not** constitute immutable acceptance;
- it does **not** promote a registry policy;
- it does **not** alter production weights or configuration; and
- it does **not** close cancellation, weight, or axis-architecture questions.

Cancellation / weight / axis-architecture adjudication remains a separate next
layer unless reliable persisted comparison artifacts already contain sufficient
contribution decomposition evidence.

The attenuation review thresholds also include named near-zero/sign-flip signal
suppression checks (`NEAR_ZERO_RATE_SUPPRESSION_CHANGE` and
`SIGN_FLIP_RATE_SUPPRESSION_CHANGE`). These are Phase 2 review thresholds only,
are emitted in `review_summary.json`, and are not production policy constants.

The aggregate Phase 2 review treats seasonality overkill as supporting evidence
only. Independent aggregate trigger-family counts are limited to chronology
lag/delay evidence, attenuation evidence, and explicit adverse shock-suppression
evidence; derivative seasonality confirmation does not independently warrant an
MA6 finalist evaluation.
