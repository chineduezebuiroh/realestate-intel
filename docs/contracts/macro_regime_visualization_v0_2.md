# Macro Regime Visualization v0.3 Presentation and Output Contract

## Status and identity

This contract governs Macro Regime Visualization **v0.3.0** and snapshot schema
**2.0**. The visualization is a deterministic, static consumer of an explicit
immutable county Macro Regime run. It is not an engine, calibration surface,
forecast, or Streamlit application.

## Product questions and section hierarchy

The county page is an executive decision-support product. Its stable narrative
answers six questions, in order:

1. `executive-brief` — **What do I need to know?** Current persisted regime,
   deterministic one-sentence interpretation, Demand and Supply direction,
   largest displayed-year axis movement, supporting dimensions, primary
   headwind, and cadence-aware freshness.
2. `current-position` — **What is the market doing today?** Governed plane,
   coordinates, persisted regime assignment, and deterministic interpretation.
3. `market-drivers` — **What's driving the market?** Demand- and Supply-side
   governed dimension contributions.
4. `market-trajectory` — **What changed?** Five-year axis history, major-regime
   transitions, displayed-year movement, and descriptive governed-dimension
   callouts.
5. `supporting-evidence` — **What supports this conclusion?** Collapsed
   Axis → Dimension → Metric evidence.
6. `audit` — **How was this determined?** Collapsed freshness/methodology and
   full provenance, registry identities, and hashes.

The first four sections form the default decision story. Evidence and audit use
native progressive disclosure. All narrative is deterministic and derived from
persisted artifacts; it introduces no forecasts, confidence, or materiality
classification. Legacy deep-link anchors remain available for v0.2 URLs.

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
under both axes. Demand is an ordinary governed dimension whose current active
members are `labor_force`, `employment`, and `laus_unemployment_rate` at equal
configured weights. Available metrics are renormalized across their ordinary
metric weights, matching production dimension scoring. The former 25%
Structural / 75% Cyclical hierarchy is superseded; active block metadata is an
error and is never reconstructed or presented. `market_context` remains a
separate, non-axis dimension and its structural metrics are not Demand evidence.
Current production membership takes precedence over historical architecture
documents. Feature-level decomposition is outside v0.2.

## Interpretation

Interpretation uses only current persisted scores, reconstructed dimension
contributions, and the displayed persisted trajectory. “Largest” is explicitly
relative and accompanied by contribution magnitude; v0.2 introduces no
materiality score or hidden threshold. Movement reports signed start-to-end
axis differences and makes no predictive claim.

## Cadence-aware freshness

Monthly evidence reports the latest actual persisted metric evidence date among
active axis metrics governed as monthly. Annual/structural axis evidence reports
the year of the latest actual persisted evidence only when an active axis metric
is governed as annual; otherwise it explicitly reports that none is active. Annual
as-of carry is never relabeled as a monthly update. Unmapped frequency remains
in an explicit `unknown` category. Raw maximum evidence age and metric counts
remain available in JSON and methodology detail.

A dedicated Data Health / Lineage monitoring surface is a future product idea.
It may expose source, canonical metric, expected cadence, latest observation,
age, expected freshness window, and stale/late or missing status. That
source-level operational monitoring belongs outside the main decision dashboard;
v0.3 defines no health calculations, thresholds, or alerts.

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

## Snapshot schema 2.0 and v0.3 presentation metadata

The JSON retains useful v0.1 fields and adds `schema_version`,
`visualization_version`, `latest_state`, `cadence_freshness`, `interpretation`,
`trajectory`, and `provenance`. Driver and metric collections retain configured
and effective metric weights, contributions, and age. Schema 2.0 no longer emits
`demand_block`, `block_weight`, or `effective_block_weight`; their removal is the
explicit migration from the superseded v0.1.2 hierarchy. Metric records add
actual evidence date and governed frequency. v0.3 adds an `executive_summary`
presentation object derived only from current persisted contributions and the
displayed persisted trajectory; it does not change schema semantics or analytical
outputs. Cadence keys are
`monthly_indicators`, `annual_structural_axis_evidence`, and `unknown`.

## Navigation, responsiveness, and accessibility

County pages use sticky anchor navigation without hiding primary content and a
back-to-index link in batch mode. Native links, headings, and
`details`/`summary` controls remain keyboard usable. Desktop uses a two-column executive brief and current-position layout, with
Demand and Supply drivers side by side. Intermediate layouts stack at 1080 px;
primary panels stack at 820 px; compact cards and metadata stack at 560 px. Charts retain textual headings and
signed values so meaning does not rely exclusively on color.

## Validation and human acceptance

Smoke 107 owns numerical reconciliation, governed membership, semantic sections,
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
part of v0.3.
