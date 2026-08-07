# Capital Markets Diagnostic Evidence Contract

## Status and purpose

`capital_markets_diagnostic_v1` is an immutable, diagnostic-only evidence
contract. It explains the authoritative incumbent and cannot select a challenger,
change a registry, or promote production behavior.

## Authoritative registry chain

The chain is resolved at runtime from the production registries and fails closed
on missing or ambiguous ownership:

`feature_registry.csv` → `metric_dimension_registry.csv` (`capital_markets`) →
`axis_registry.csv` (`supply` and `demand`). Source identity, geography, cadence,
and lineage come from `source_metric_registry.csv`. The diagnostic calls the
production decomposition functions, which implement the same available-child
weight renormalization as the production metric, dimension, and axis scorers.

At this contract revision the registry resolves six active canonical metrics:

| canonical metric | dimension weight | configured features |
|---|---:|---|
| `mortgage_30y` | 0.35 | level 0.40; short-term 0.30; long-term 0.30 |
| `mortgage_15y` | 0.05 | level 0.40; short-term 0.30; long-term 0.30 |
| `fedfunds` | 0.15 | level 0.40; short-term 0.30; long-term 0.30 |
| `treasury_10y` | 0.15 | level 0.40; short-term 0.30; long-term 0.30 |
| `spread_10y_2y` | 0.20 | level 0.40; short-term 0.30; long-term 0.30 |
| `spread_10y_fedfunds` | 0.10 | level 0.40; short-term 0.30; long-term 0.30 |

The physical `fred_2y` metric is diagnostic-only and is not an independent
dimension contributor. Capital Markets has configured axis weight 0.15 on Supply
and 0.10 on Demand. Runtime registry evidence remains authoritative over this
human-readable snapshot.

## Identity and governed scope

The contract identity is its version plus the ordered review-geography tuple.
Evidence distinguishes `native_source` rows from `county_aligned` rows. The
governed county tuple is DC, Essex NJ, Montgomery MD, Prince George's MD,
Fairfax VA, San Francisco CA, and Los Angeles CA. Replicated national values are
not counted as independent native observations.

## Tables

The immutable evidence object owns twelve tables: registry audit,
feature-to-metric and metric-to-dimension decomposition, volatility, sign flips,
largest jumps, coverage/missingness, effective weights, cancellation,
reconstruction provenance, and Supply/Demand propagation. Reconstruction status
is controlled: `reconciled`, `not_applicable`, `not_reconcilable`, or `failed`.
Configuration-era mismatch is retained as `failed`; it is never waived.

Missing children are dropped and remaining configured weights are renormalized.
The diagnostic explicitly materializes zero-child dates even though production
correctly emits no parent on those dates. Scores retain production clipping at
the metric and dimension boundaries; the diagnostic performs no additional
clipping, normalization, smoothing, or alignment.

## Review bundle

The review bundle contains a compact HTML landing page, deterministic linked CSV
files, a hash manifest with `promotion_state: none`, and a deterministic ZIP.
Generated exports are review artifacts and are not committed. Human decision
status is always pending in this phase.

## Structural-MA decomposition extension

`capital_markets_ma_decomposition_v1` extends the incumbent evidence without
changing it. It accepts only the complete, hash-valid
`macro_regime_v1_frozen_supply_20260806` run produced by
`supply_metric_weight_promotion_2026_08_06`, and additionally requires manifest
proof of `supply_dimension_frozen_v1`, Supply weights 0.60/0.20/0.20, and the
settled Supply MA12 structural formulas.

The extension caches MA6, MA9, and MA12 structural feature families once per
active Capital Markets metric. Short means lag three of the same moving-average
state, not MA3 divided by MA12. Each challenger replaces one metric only and
retains the incumbent feature and metric weights. It then calls the production
normalizer and metric, dimension, axis, coordinate, geometry, and regime
engines. Native review is one `united_states__nation` chronology; the governed
seven counties are explicitly aligned copies rather than seven independent
observations.

The deterministic review includes registry/lineage, raw and transformed
chronologies, both decomposition levels, missingness and effective weights,
volatility, cancellation, exact-calendar directional agreement, governed
persistence/prominence turning points, propagation and parity evidence, and a
payment-burden dependency audit. Its recommendation and promotion states are
both `none`; human interpretation remains pending.

The same v1 contract also owns a backward-compatible second-stage control set:
MA6, MA9, and MA12 applied separately to the mortgage, policy/yield, and spread
families, plus three all-active-metric controls (12 policies total). The three
families must be an exact disjoint partition of the registry-discovered active
set. These controls reuse the metric-policy caches built by the primary
one-metric pass and cannot replace its evidence.

The variance budget reports standalone weighted-contribution variance and a
separate additive absolute-movement attribution at feature, metric, family, and
dimension levels. Because correlated child variances are not additive, a
separate covariance budget reconciles each parent using
`Var(sum Xi) = sum Var(Xi) + 2 sum Cov(Xi, Xj)`. Standalone variance shares are
therefore not presented as covariance-inclusive variance explained. Family
interaction rows compare observed stability changes with the sum of the
corresponding one-metric changes and remain descriptive rather than causal.

## Ratio-versus-arithmetic-difference Phase 1

The primary matrix now retains ratio MA3/6/9/12 and adds arithmetic-difference
MA3/6/9/12 for every active metric.  Both transforms consume one common MA
level-state cache per metric and window. Ratio short/long features divide by
the exact three- and twelve-month calendar-lagged MA states; arithmetic features
subtract those same states. Governed rate and spread sources are recorded as
percentage-point observations and arithmetic differences are exposed as basis
points with the deterministic factor 100.

Native chronology must be unique, monotonic, finite, and calendar-contiguous.
Warmup remains null. Ratio evidence records the governed absolute near-zero
threshold `1e-8`, denominator signs and sign changes, finiteness, and magnitude;
values are never clipped. The phase creates 48 one-metric challengers from 24
common MA caches. It selects no winner, runs no combined challenger, and leaves
human decisions pending with recommendation and promotion states `none`.

Transform evidence is materialized rather than aliased to the earlier
metric-policy tables. The 54-row scorecard joins unit, coverage, separate
short/long ratio-risk, raw-feature, normalized-feature, metric, turning-point,
and exact-overlap dimension evidence. Its 54-row compact matrix contains no
composite score. The 24 ratio-versus-difference rows are calculated on actual,
gap-free date intersections rather than minimum observation counts. Cache
evidence is split into a 24-row common MA-state audit, with both transform
consumers recorded, and a 48-row transform-cache audit.

## Combined family-policy diagnostic

The next diagnostic stage compares the persisted incumbent with three national,
all-six-metric interventions. Challenger A uses ratio structural features with
MA12 long rates, MA3 Fed Funds, and MA9 spreads. Challenger B is identical except
for MA12 spreads. Challenger C has A's windows and replaces ratio changes with
arithmetic differences (with basis-point evidence). The six active metrics are
partitioned locally into long-rate (`mortgage_30y`, `mortgage_15y`,
`treasury_10y`), policy-rate (`fedfunds`), and spread (`spread_10y_2y`,
`spread_10y_fedfunds`) families; this is not a project-wide classification.

All six metric chronologies are replaced together, Capital Markets is scored
once nationally, and only then is it aligned to the seven governed counties.
Feature, metric, dimension, and axis weights are preserved; all other dimensions,
including frozen Supply and Affordability, are exact controls. Human decisions
remain `pending`; recommendation and promotion remain `none`.

The future equal-family metric-weight hypothesis (one third each for long-rate,
policy-rate, and spread families) is documentation only and is not executed.
The intended sequence is combined transform/window selection, feature-weight
diagnostic, metric-weight diagnostic, then Capital Markets freeze. A separate
future Affordability hypothesis is also documentation only: derive raw payment
burden from raw median sale price and raw `mortgage_30y`, then apply the governed
structural transform once to the derived measure. Affordability is not changed
or frozen by this diagnostic.

## Superseded feature-weight diagnostic

The historical diagnostic fixed the then-selected all-ratio architecture at MA12 for
`mortgage_30y`, `mortgage_15y`, and `treasury_10y`; MA3 for `fedfunds`; and MA9
for both spreads. It compares the persisted production incumbent (context only)
with common all-six-metric mixes 40/30/30, 50/25/25, and 60/20/20 for
level/short/long. The three settled policies reuse the same transformed and
normalized feature objects and differ only in the weights passed to the
production-equivalent missingness-renormalizing metric aggregation.

That historical stage did not select a winner, execute metric weights, change
Affordability, or mutate production policy. The future metric-weight hypothesis
remains equal family totals: long-rate metrics each 1/9, Fed Funds 1/3, and
spreads each 1/6 (`future_metric_weight_hypothesis_only = true`).

## Spread polarity correction gate

The canonical metric is `spread_10y_2y`, and its governed formula is explicitly
`treasury_10y - treasury_2y`. `spread_10y_fedfunds` is
`treasury_10y - fedfunds`. The naming convention is
`spread_<long leg>_<short/policy leg>`. Thus positive values mean
an upward-sloping curve and negative values mean inversion. Spread level is MA9
and spread short/long structural features are arithmetic differences from lag 3
and lag 12 of that same MA9 state; basis-point values are persisted as a separate
exact 100-times representation. Rate-level families continue to use ratio
features (MA12 for long rates and MA3 for Fed Funds).

Feature-key normalization overrides give both spreads direct polarity while the
four rate series retain inverse FRED-rate polarity. The diagnostic fixes the
feature control at 40/30/30, retains all metric and axis weights, and records
`recommendation_state = none`, `promotion_state = none`, and human decision
`pending`. Earlier feature-weight evidence is
`superseded_for_final_calibration`; it remains historical evidence but cannot
freeze weights until this corrected architecture is rerun.

Immutable historical artifacts may contain the legacy canonical key
`spread_2y10y`. They are never rewritten: the Capital Markets diagnostic maps
that key to `spread_10y_2y` at its artifact-read boundary. This compatibility
mapping is the only runtime use of the legacy identity. New production and
diagnostic writes must not emit `spread_2y10y` as a canonical metric key. The
governed sequence remains: spread polarity/key correction → rerun final
feature-weight diagnostic → metric-weight diagnostic → Capital Markets freeze.

The spread-correction decision pass materializes an isolated two-policy review.
`legacy_spread_architecture` reconstructs the immutable artifact boundary as
2Y-minus-10Y with MA9 ratio structural spread features.
`corrected_spread_architecture` uses 10Y-minus-2Y and 10Y-minus-Fed-Funds with
MA9 arithmetic-difference features (`MA9 - lag3(MA9)` and
`MA9 - lag12(MA9)`). The four rate metrics retain their governed MA12/MA3 ratio
architecture. The comparison has no similarity reward, rank, recommendation,
or promotion; its two-row decision matrix remains pending. The earlier A/B/C
matrix is retained only as historical/secondary evidence, and prior
feature-weight evidence remains `superseded_for_final_calibration`.

## Settled Capital Markets diagnostic architecture

The architecture used by all future Capital Markets diagnostic calibration is
settled (this is not production promotion):

| metric | MA window | structural transform |
|---|---:|---|
| `mortgage_30y` | 12 | ratio |
| `mortgage_15y` | 12 | ratio |
| `treasury_10y` | 12 | ratio |
| `fedfunds` | 3 | ratio |
| `spread_10y_2y` | 9 | arithmetic difference |
| `spread_10y_fedfunds` | 9 | arithmetic difference |

Spread ratio features are not permitted in future final calibration because
sign-changing spreads create denominator and direction pathologies. Pathology
counts use unique `metric_key × feature × date` observations and fail closed on
duplicate keys. Cancellation summaries use only exact-calendar rows whose six
child contribution movements reconstruct the parent within `1e-12`; excluded
warmup and availability rows remain explicitly classified in audit evidence.

Prior feature-weight evidence remains `superseded_for_final_calibration`, and
`next_valid_feature_weight_experiment_must_use_settled_capital_markets_architecture
= true`. The next step is final feature-weight calibration on this settled
architecture. No final feature, metric, or axis weights are encoded here;
recommendation and promotion remain `none`, and the human decision remains
`pending`.

## Final feature-weight rerun

The focused final rerun compares exactly three policies on the settled mixed
architecture above. FW-A applies 50/25/25 to all six metrics; FW-B applies
60/20/20 to all six; FW-C applies 60/20/20 except for Fed Funds, which retains
50/25/25. Metric weights and every non-feature-weight input are held constant.
The stage consumes the six settled transformed and normalized caches directly;
it does not rebuild the historical 48 one-metric challenger matrix.

Final evidence uses the `capital_markets_final_feature_weight_*` namespace and
has an exactly three-row decision matrix. It reports stability, turning-point,
family, extreme-jump, comparable-only cancellation, recent chronology,
directional context, and county-regime context without a composite score, rank,
or automated winner. Recommendation and promotion remain `none`, every
decision remains `pending`, and empirical interpretation requires the
authoritative local frozen-run artifacts.

## Tempered metric-weight finalist diagnostic

The metric-weight stage reuses the settled metric-score chronology and the
60/20/20 feature-weight policy. It compares exactly four finalists, displayed
in conservative-to-aggressive policy-rate order: the production incumbent
(long-rate/policy-rate/spread family totals 55/15/30), `MW-TEMPERED-C`
(45/10/45), `MW-TEMPERED-A` (40/20/40), and `MW-TEMPERED-B` (40/25/35).
Only configured metric weights may differ; normalized features, metric scores,
scorers, availability behavior, axes, geography, and downstream engines are
exact controls. Every policy must sum to one within the governed tolerance.

The previous equal-family policy with one-third Fed Funds exposure is retained
only as historical evidence: it established an upper-bound warning and is not a
finalist. The finalist evidence includes concentration, family contributions,
dimension stability and tails, cancellation, turning-point and extreme-jump
attribution, recent chronology, Fed Funds stress, and downstream context. It
does not compute a composite score, rank, recommendation, or winner.
Recommendation and promotion remain `none`; the human decision remains
`pending`, and no production registry is mutated.
