# GDP and ACS Demand-Dimension Diagnostic

> This is supporting diagnostic evidence. The authoritative accepted Demand
> policy is `docs/decisions/demand_dimension_production_policy.md`.

## Status and scope

Diagnostic complete for the persisted `macro_regime_v1_bps120_sources` run.
No production registry, normalization, feature-weight, metric-weight, Price, or
Supply policy is changed by this decision record.

## Reused implementation

The review extends `core_demand_dimension_diagnostic` rather than recreating
Demand reconstruction, effective metric weighting, contribution, cancellation,
or pairwise interaction calculations. It adds all-geography source coverage,
cadence, freshness, time-series, feature-policy, normalization, and clipping
summaries for `gdp_annual`, `median_household_income`, and `population`.

## Current policy

All three canonical metrics have equal nominal Demand metric weight (`0.1667`).
Their Level, Short, and Long weights are `0.25`, `0.35`, and `0.40`.
GDP and ACS Short features use annual change; Long uses the three-year rolling
annual-change transform. Annual GDP is the production fallback because
quarterly GDP is diagnostic-only and is not present in the production run.

## Evidence

The persisted canonical observations are annual, complete within their emitted
rows, and spaced at 365–366 days. GDP covers 123 counties, five states, and the
nation from 2001–2024. ACS income and population cover 163 counties, 54 CBSAs,
five states, and the nation from 2005–2024. At the run's July 2026 as-of date,
the latest annual observations are 577 days old. That age is expected from the
available annual panel but confirms that these metrics are structural backdrop,
not responsive monthly signals.

Normalization warm-up moves first usable GDP features to 2008 (Level), 2009
(Short), and 2011 (Long), and ACS features to 2012, 2013, and 2015. GDP and ACS
Level percentiles frequently sit at the upper clip, especially for national and
state series. This reflects applying expanding within-series level percentiles
to strongly trending nominal-size series; Level consequently carries limited
cyclical information. Short and Long components retain substantially more score
dispersion and materially less saturation.

In the two mandatory core-Demand review counties, mean absolute contribution to
the Demand axis is approximately 0.039–0.070 for GDP, 0.054–0.069 for income,
and 0.045–0.055 for population. The metrics are therefore not inert. Alameda
shows moderate reinforcement among GDP, income, and population, while District
of Columbia correlations are mostly weak; the differing geography behavior
argues against treating the three metrics as universally redundant.

## Recommendations

1. **Retain all three metrics pending challenger evidence.** Current evidence
   does not support removal.
2. **Do not increase their metric weights.** Annual cadence and 577-day endpoint
   age already give slow-moving structural signals a substantial combined share.
3. **Prioritize a Level-transform challenger.** Evaluate detrended, real/per
   capita, or growth-relative Level definitions before changing weights. The
   high upper-clip rates make raw expanding Level percentile the clearest policy
   concern.
4. **Keep Short and Long definitions provisionally.** They remain distinct and
   less saturated, but an immutable challenger is required before promotion.
5. **Retain equal feature and metric weights for now.** Weight changes should be
   evaluated only after the Level challenger separates construction effects
   from weighting effects.
6. **Do not infer coordinate or regime sensitivity.** Generate an immutable
   counterfactual run before making downstream classification claims.
7. **Defer physical-source and vintage conclusions.** Canonical persisted rows
   do not distinguish ACS1 from ACS5 and the artifact contract has no revision-
   vintage history. Those lineage gaps must be addressed before source-specific
   promotion or freshness changes.

## Generated review exports

The diagnostic writes focused CSV summaries and `diagnostic_manifest.json`
under `artifacts/regime/review_exports/gdp_acs_demand_diagnostic/`. These are
review artifacts and are not source-controlled.
