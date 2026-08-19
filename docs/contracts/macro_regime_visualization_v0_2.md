# Macro Regime Visualization v0.2 Presentation and Output Contract

## Status and identity

This contract governs Macro Regime Visualization **v0.2.0** and snapshot schema
**2.0**. The visualization is a deterministic, static consumer of an explicit
immutable county Macro Regime run. It is not an engine, calibration surface,
forecast, or Streamlit application.

## Product questions and section hierarchy

The county page answers, in order: where the county is now; what that state
means; what drives it; how it arrived there and whether it persisted; what
metric evidence supports it; and how fresh and traceable the evidence is.

The stable sections are:

1. `market-regime` — current state, cadence summary, and governed plane;
2. `market-interpretation` — deterministic condition, drivers, and movement;
3. `regime-drivers` — primary Demand and Supply dimension contributions;
4. `market-trajectory` — five-year axes followed by regime history;
5. `evidence-detail` — collapsed Axis → Dimension → Metric evidence;
6. `data-methodology` — collapsed provenance and freshness methodology.

The first four sections form the decision view. Methodology is analytical
context. Metric panels are audit detail and are collapsed by default.

## Authoritative inputs and no-recompute boundary

Required run artifacts are `regime_assignments.parquet`,
`coordinates.parquet`, `axis_scores.parquet`, `dimension_scores.parquet`, and
`metric_scores.parquet`. Axis and metric membership come from the governed axis
and metric-dimension registries. Cadence comes from the governed source-metric
registry joined through governed metric aliases.

The renderer may reconcile persisted contributions against registries. It must
not calculate transforms, normalization, metric scores, dimensions, axes,
geometry, assignments, probabilities, confidence, or forecasts. Persisted
assignments are authoritative. Missing, contradictory, conflicting-cadence, or
non-county publication inputs fail closed; unknown cadence is labeled unknown
rather than guessed. Outputs must never be written into the source run.

## Geometry and hierarchy

Supply is x and Demand is y. The fixed ±0.60 plane, zero lines, governed sector
boundaries, and radial references do not classify points. The display uses the
latest 12 persisted months and visually emphasizes later observations.

Demand and Supply membership remains registry driven. Capital Markets appears
under both axes. Demand metric evidence preserves 25% Structural and 75%
Cyclical governed blocks, availability renormalization, and effective weights.
Feature-level decomposition is outside v0.2.

## Interpretation

Interpretation uses only current persisted scores, reconstructed dimension
contributions, and the displayed persisted trajectory. “Largest” is explicitly
relative and accompanied by contribution magnitude; v0.2 introduces no
materiality score or hidden threshold. Movement reports signed start-to-end
axis differences and makes no predictive claim.

## Cadence-aware freshness

Monthly/cyclical evidence reports the latest actual persisted metric evidence
date among metrics governed as monthly. Structural/annual evidence reports the
year of the latest actual persisted metric evidence governed as annual. Annual
as-of carry is never relabeled as a monthly update. Unmapped frequency remains
in an explicit `unknown` category. Raw maximum evidence age and metric counts
remain available in JSON and methodology detail.

## Outputs

Single-county mode writes `<geo_id>.html` and
`<geo_id>_snapshot.json`. Batch mode writes:

```text
<output>/
  index.html
  manifest.json
  counties/
    <geo_id>.html
    <geo_id>_snapshot.json
```

Batch input is an explicit county-only manifest. Rows and output inventory are
sorted deterministically. `manifest.json` records schema and visualization
versions, source-run identity, generated counties, registry hashes, and SHA-256
plus byte size for every county output and the index. It intentionally does not
duplicate the Regime Engine manifest.

## Snapshot schema 2.0

The JSON retains useful v0.1 fields and adds `schema_version`,
`visualization_version`, `latest_state`, `cadence_freshness`, `interpretation`,
`trajectory`, and `provenance`. Driver and metric collections retain configured
and effective weights, contributions, age, and Demand block metadata. Metric
records add actual evidence date and governed frequency.

## Navigation, responsiveness, and accessibility

County pages use sticky anchor navigation without hiding primary content and a
back-to-index link in batch mode. Native links, headings, and
`details`/`summary` controls remain keyboard usable. Desktop places summary and
plane, then Demand and Supply drivers, side by side. At 820 px and below these
layouts stack; at 480 px KPI cards stack. Charts retain textual headings and
signed values so meaning does not rely exclusively on color.

## Validation and human acceptance

Smoke 107 owns numerical reconciliation, hierarchy, semantic sections,
collapsed evidence, cadence categories, JSON identity, county-only batch
publication, output hashes, and deterministic rerendering. Python compilation
and `git diff --check` remain required.

Final acceptance additionally requires generating from an authoritative frozen
F4 run and reviewing Washington, DC plus a representative county at desktop and
narrow widths. Review clipping, labels, plane readability, sticky navigation,
freshness, collapsed/expanded evidence, and county links. Disposable screenshots
must not be committed.

Example authoritative commands are:

```bash
python scripts/build_regime_visualization_mvp.py \
  --run artifacts/regime/runs/<authoritative-f4-run-id> \
  --geo-id district_of_columbia_dc__county \
  --market-name "Washington, DC" \
  --output artifacts/product_mvp/macro_regime_v0_2/<authoritative-f4-run-id>/counties

python scripts/build_regime_visualization_mvp.py \
  --run artifacts/regime/runs/<authoritative-f4-run-id> \
  --county-manifest <explicit-county-only-manifest.csv> \
  --output artifacts/product_mvp/macro_regime_v0_2/<authoritative-f4-run-id>
```

## Explicitly deferred

Feature decomposition, transition probability, confidence, recommendation,
forecasting, ranking, alerts, comparison dashboards, maps, PDF, CBSA/metro,
ZIP/local regime, mutable DuckDB, server state, and Streamlit migration are not
part of v0.2.
