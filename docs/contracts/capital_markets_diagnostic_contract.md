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
| `spread_2y10y` | 0.20 | level 0.40; short-term 0.30; long-term 0.30 |
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
