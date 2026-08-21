# Macro Regime Visualization MVP — State Reconstruction and Handoff

**State reconstructed:** 2026-08-18 (reviewed from repository state on 2026-08-19)

**Active workstream:** Macro Regime Visualization MVP

**Current implementation version:** v0.1.2

**Scope of this document:** state reconstruction only; no engine, visualization, roadmap, ADR, registry, or production-artifact change is made by this handoff.

## Executive resume point

The MVP is not a Streamlit page. It is a standalone, artifact-only Python/Plotly renderer that reads five tables from one immutable production run and emits one self-contained county HTML snapshot plus one compact JSON snapshot. The current page already has a complete vertical narrative—current state, regime plane, explanation, dimension and metric drivers, five-year axis chronology, and major-regime chronology—but is still visually closer to a diagnostic report than to a finished decision dashboard.

Work stopped after v0.1.2 added a then-assumed Demand hierarchy and metric drilldowns. That visualization assumption is now superseded by the frozen production scorer: Demand uses ordinary governed metric weights and active block metadata must be blank. The next intended iteration is the documented v0.2 layout and information-hierarchy refinement: make the top-level drivers decision-oriented, subordinate forensic detail, and split freshness by governed cadence. Capital Markets calibration subsequently changed the values supplied by new production runs, but did not change the visualization implementation or its contracts.

---

## 1. Current visualization architecture

### 1.1 Production snapshot path

```text
immutable Regime Engine run directory
  ├── regime_assignments.parquet
  ├── coordinates.parquet
  ├── axis_scores.parquet
  ├── dimension_scores.parquet
  └── metric_scores.parquet
          + config/axis_registry.csv
          + config/metric_dimension_registry.csv
                         │
                         ▼
       visualization/regime_snapshot.py
          resolve_snapshot()  -> validated in-memory Snapshot
          Plotly helpers       -> plane, bars, chronology, strip
          render_snapshot()    -> standalone HTML + compact JSON
                         ▲
                         │
       scripts/build_regime_visualization_mvp.py
       (thin command-line entry point)
```

`visualization/regime_snapshot.py` owns both the read/validation boundary and presentation. Its `Snapshot` model carries the latest state, axis-to-dimension drivers, dimension-to-metric drivers, 12-month path, five-year history, transitions, and generated explanation. It deliberately has no Streamlit or DuckDB dependency.

The CLI requires an explicit run directory, geography ID, market name, and output directory. It uses the production axis and metric-dimension registries from `config/`; it does not select a latest run, discover counties, or publish an index.

### 1.2 Inputs and reconciliation

The renderer fails closed when a required artifact is absent, when latest regime/coordinate/axis state is not unique and complete, or when active registry membership cannot reconstruct persisted dimension scores. The latest display month is the latest common date across assignments, coordinates, and axis scores for the requested county.

Axis drivers are reconstructed as persisted dimension score × configured axis weight. Metric drivers use persisted metric scores and governed metric membership. County evidence wins; national metric evidence is allowed to supply governed Capital Markets inputs. Missing available components are explicitly renormalized, and every dimension's metric contributions must reconcile to the persisted dimension score within tolerance. Demand follows the production dimension scorer: available active Demand metrics are normalized directly by ordinary `metric_weight`. The former Structural/Cyclical blend is superseded, and Market Context is not an axis dimension.

### 1.3 Rendering stack

The output uses Plotly figures embedded into hand-assembled responsive HTML. The first chart embeds the Plotly JavaScript runtime inline, making the report self-contained; subsequent charts reuse it. There is no template engine, component framework, CSS module, shared design system, server-side state, or client-side navigation layer.

The plotting helpers are private functions in the same module:

- `_plane`: governed Demand/Supply coordinate plane and trailing path;
- `_drivers`: dimension-contribution horizontal bars;
- `_metric_chart`: metric-contribution horizontal bars;
- `_history`: five-year Demand and Supply axis time series;
- `_regime_strip`: five-year major-regime band and transition markers.

### 1.4 Relationship to Streamlit

There are two legacy Streamlit entry points, `app/streamlit_app.py` and root `app.py`. They query mutable DuckDB serving data for generic metric browsing, market comparison, macro rate/spread overlays, and forecast views. They do **not** read immutable Regime Engine artifacts, import `visualization.regime_snapshot`, expose macro-regime assignments, or host the MVP snapshot. Their geography options also extend beyond the current county-only Macro Regime production contract. They are adjacent application code, not the current Macro Regime Visualization MVP architecture.

### 1.5 Review/diagnostic visualization boundary

The repository also contains a mature, separate review ecosystem:

- typed review bundles and deterministic package writing under `regime/review/`;
- chronology, transition, contribution, cancellation, volatility, and geography-selection diagnostics under `regime/diagnostics/` and `regime/experiments/`;
- campaign-specific static PNG/SVG figures, HTML review pages/indexes, evidence CSVs, manifests, and deterministic ZIPs;
- a small set of tracked Demand chronology SVG decision evidence under `docs/decisions/demand_chronology_attenuation_visuals/`.

Those facilities are evidence/review tooling. They are not imported by the MVP renderer, and generated review exports must stay separate from immutable production runs and from tracked source.

---

## 2. Existing MVP capabilities

### 2.1 What works now

1. **Single-county snapshot generation.** A caller can render any requested county that has complete persisted run evidence. Washington, DC county is the smoke-test fixture and mandatory review geography, but the renderer is not hard-coded to DC.
2. **Latest current-state card.** Displays market name, latest common as-of month, minor regime in prominent type, Demand score, Supply score, regime strength, and maximum contributing-axis input age.
3. **Coordinate/regime-plane plot.** Displays the governed coordinate convention (`x = Supply`, `y = Demand`), fixed ±0.60 axes, zero lines, four shaded major-regime sectors, exact 45°/135°/225°/315° boundaries, 0.25/0.50 radial references, a trailing 12-month line/marker path, and a highlighted latest state. Hover shows month, coordinates, major regime, and minor regime. Reference geometry never reclassifies persisted points.
4. **Plain-language explanation.** Produces one deterministic sentence per axis describing net direction and strongest positive/negative dimension contribution.
5. **Axis → dimension contribution views.** Separate Demand and Supply horizontal bars show weighted dimension contributions. Hover exposes raw dimension score, configured weight, and contribution. Capital Markets correctly appears in both axes.
6. **Dimension → metric drilldowns.** Collapsible details exist for all active dimensions: Demand, Price, Affordability, Capital Markets under Demand, plus Supply and Capital Markets under Supply. Bars are sorted by absolute contribution and expose metric score, configured/effective metric weight, weighted contribution, and evidence age.
7. **Demand metric drilldown.** Demand presents the active governed labor-family metrics directly, using ordinary availability-normalized metric weights. No Structural/Cyclical grouping is active.
8. **Five-year axis timeline.** Interactive monthly Demand and Supply lines cover the latest five-year window (inclusive month-end behavior yields 61 points in the test fixture), with unified hover and persisted major/minor regime context.
9. **Five-year major-regime chronology.** A colored monthly strip displays Expansion, Hypersupply, Recession, and Recovery, with dark vertical markers at changes in major regime and hover for minor assignment.
10. **Responsive single-page composition.** Two-column driver/drilldown sections collapse to one column below 700 px. Plotly mode bars are disabled for a cleaner review surface.
11. **Self-contained HTML.** The county report has inline Plotly runtime and requires no application server or network fetch.
12. **Machine-readable snapshot JSON.** The companion JSON contains run/geography/market identity, current state, Demand and Supply dimension drivers, all metric drivers, ordinary configured/effective metric weights, contributions, ages, and explanation; retired Demand block fields are omitted.
13. **Deterministic contract smoke test.** Smoke 107 constructs a fixed artifact fixture; validates hierarchy, geometry, chronology windows, reconciliation, missing-component behavior, anchors, and output payload; verifies fail-closed missing artifacts; and confirms production registries are not mutated.

### 2.2 Capabilities that exist elsewhere but are not integrated into the MVP

- **County comparisons:** generic market overlays exist in legacy Streamlit, and multi-geography comparison plots exist in review diagnostics, but the MVP has no county-comparison page or panel.
- **Static SVG/PNG review figures:** numerous diagnostics generate them, but the MVP itself uses interactive Plotly HTML and emits no SVG or PNG.
- **HTML review indexes/pages:** calibration diagnostics and review packages produce these, but the MVP emits only a per-county snapshot; it has no multi-county landing index.
- **Review packages:** manifest/hash validation and deterministic ZIP support exist in the review platform, but an MVP snapshot is not currently wrapped in that package contract.
- **PDF:** no PDF output exists for the MVP, and no general PDF publication path was found for this subsystem.

---

## 3. Remaining unfinished work

There is no standalone visualization TODO file. The authoritative explicit follow-up is the v0.2 paragraph in the roadmap plus the feature-level decomposition deferral in ADR-008. The lists below distinguish those documented commitments from reasonable product completion work inferred from the current code boundary.

### 3.1 Required for MVP completion

1. **Resume the paused panel-layout/information-hierarchy pass.** The current page is a vertical diagnostic report. Refine section grouping, spacing, headings, and the relationship between current state, plane, explanation, driver summary, forensic drilldowns, and history.
2. **Make top-level drivers decision-oriented.** Replace the current generic two-sentence explanation and equally prominent diagnostic bars with a clearer answer to “what is driving the current state?” while retaining auditable numeric detail.
3. **Keep forensic metric drilldowns secondary.** They are already collapsible, but the surrounding composition still gives the whole driver area diagnostic-report weight. Establish progressive disclosure and clearer hierarchy.
4. **Implement governed-cadence freshness.** Replace the single “oldest contributing axis input” headline with separate monthly indicator freshness and annual/structural active-axis evidence status. Do this only from persisted evidence and membership metadata; do not infer unsupported cadences or recompute engine state.
5. **Validate v0.2 against the final frozen engine.** Generate a disposable review output from an accepted post-freeze county run and verify Capital Markets contributions on both axes, national-to-county as-of behavior, current assignments, hierarchy, missingness, and deterministic rerendering.
6. **Define the user-facing entry/publishing path.** Decide whether v0.2 remains a static artifact, gains a deterministic county index, or is exposed by an application shell. Do not silently couple it to the legacy mutable-DuckDB Streamlit app.
7. **Add multi-county navigation/selection appropriate to county-only production.** The current CLI and output are one county at a time, with no index or next/previous navigation. A usable visualization MVP needs a bounded way to reach generated county snapshots without introducing non-county scope.
8. **Add layout/visual regression acceptance.** Smoke 107 asserts semantic HTML anchors and data contracts but not clipping, responsive rendering, accessibility, or panel appearance. The resumed layout work needs browser-level review and a stable screenshot/checklist baseline.
9. **Clarify publication metadata.** Surface run ID and as-of identity more clearly than the small footer, and decide whether manifest/config hashes or validation status belong in a secondary provenance panel.

### 3.2 Nice-to-have refinements

- clearer regime labels/legend and trajectory time direction on the coordinate plane;
- transition annotations or a concise “what changed” summary tied to the latest movement;
- configurable history horizon rather than fixed 12-month path and five-year chronology;
- downloadable JSON/evidence link from the HTML;
- keyboard and screen-reader improvements for Plotly charts and `<details>` controls;
- shared HTML/CSS templates or presentation components to reduce the monolithic renderer;
- human-friendly county naming/ordering from a governed geography registry rather than CLI-supplied labels;
- an explicit “data unavailable/partial evidence” presentation while preserving fail-closed core-state rules;
- optional static export (SVG/PNG) for sharing, provided it remains deterministic and review-only;
- harmonized visual tokens with review pages without coupling their computation paths.

### 3.3 Future roadmap, not MVP

- feature-level decomposition (`Axis → Dimension → Metric → Feature`), including raw score, configured/effective weights, contribution, sign, source date/freshness, and cumulative cancellation;
- CBSA/metro Macro Regime visualization, only after separate geography and GDP/labor correctness contracts are accepted;
- Local Regime/ZIP visualization under its own architecture (ZIP is reserved for future local work);
- Market Balance, Market Profile, comparison, ranking, confidence, and alerting products;
- regime-informed forecast visualization/integration;
- broader automated regime-health monitoring and assignment/transition trace tooling;
- PDF publication, if a real product requirement is later adopted.

---

## 4. Current dashboard layout

### 4.1 Macro Regime MVP page

The MVP has one generated page per county and no site-level navigation. Its vertical order is:

1. **Current state** (`#current-state`): eyebrow, county name, as-of month, prominent minor regime, three score KPIs, and one freshness line.
2. **Regime plane** (`#regime-plane`): explanatory caption plus the 12-month coordinate trajectory.
3. **Why this regime?** (`#why-this-regime`): deterministic Demand/Supply explanation text.
4. **Dimension drivers** (`#dimension-drivers`): a two-column Demand/Supply dimension row followed by a two-column Demand/Supply metric-drilldown row. Six `<details>` panels are present because Capital Markets is shown under both axes. Demand expands directly into its active metric-contribution chart.
5. **Historical chronology** (`#historical-chronology`): five-year Demand/Supply line chart.
6. **Major regime chronology** (`#major-regime-chronology`): five-year colored assignment strip.
7. **Footer:** visualization version and published run directory name.

Navigation consists only of browser scrolling and the native expand/collapse behavior of `<details>`. There is no sidebar, tab bar, table of contents, county picker, comparison link, breadcrumb, review index, or deep-link control beyond the section IDs.

### 4.2 Where layout refinement had reached

The code already expresses the beginning of a panel hierarchy: a current-state hero; primary plane; a short explanation; side-by-side Demand/Supply summaries; collapsible forensic details; then historical context. Responsive CSS collapses the two-column panels on narrow screens. The paused refinement was not about inventing the analytical content—the content exists—but about reducing the report-like density, clarifying what is primary versus forensic, and making the driver presentation more useful for decisions. The roadmap explicitly assigns this refinement to v0.2, not v0.1.x.

### 4.3 Legacy Streamlit layouts

`app/streamlit_app.py` is a single-page “Washington, DC — Market Pulse” browser with geography-level radio buttons, market/metric/property-type selectors, freshness/KPI blocks, a series history, and a compare-markets panel; it conditionally adds national Rates/Spreads controls. Root `app.py` is a separate broad market/forecast dashboard. Neither is navigation for the Macro Regime snapshot, and neither should be treated as evidence that non-county Macro Regime views already exist.

---

## 5. Generated artifacts

### 5.1 Artifacts produced directly by the Macro Regime Visualization MVP

For each CLI invocation and requested `geo_id`, exactly two files are produced in the caller-supplied output directory:

| Type | Name | Contents/status |
|---|---|---|
| HTML | `<geo_id>.html` | Self-contained interactive county page with inline Plotly runtime and all six page sections. |
| JSON | `<geo_id>_snapshot.json` | Compact current-state, dimension-driver, metric-driver, freshness-age, explanation, run, and geography payload. |

The MVP does **not** currently produce CSV, Parquet, SVG, PNG, PDF, a manifest, a ZIP, or an index. The HTML/JSON outputs are generated review/presentation artifacts and should not be committed.

### 5.2 Immutable production artifacts consumed, not produced

- `regime_assignments.parquet`
- `coordinates.parquet`
- `axis_scores.parquet`
- `dimension_scores.parquet`
- `metric_scores.parquet`

The renderer also consumes `config/axis_registry.csv` and `config/metric_dimension_registry.csv`. It never writes any of these inputs.

### 5.3 Separate review-platform artifact forms available in the repository

These are not automatic MVP outputs, but are relevant reusable infrastructure and historical visualization evidence:

| Form | Current producer/boundary |
|---|---|
| CSV evidence tables | Diagnostic scripts and `ReviewArtifactWriter`; normally under generated evidence/table directories. |
| PNG review plots | Demand, inventory, calibration, reconciliation, and other diagnostic modules. |
| SVG review plots | Calibration/chronology diagnostics; 14 Demand chronology-attenuation SVGs are intentionally tracked as decision evidence. |
| HTML review page/index | Campaign-specific diagnostics and review-bundle renderers. |
| `manifest.json` | Review bundle authority, containing package metadata and declared outputs/hashes. |
| `decision_summary.json` | Optional typed review decision output. |
| `tables/selected_review_geographies.csv` | Optional review geography selection output. |
| `tables/review_geography_rationale.csv` | Optional review geography rationale output. |
| Deterministic ZIP | Optional portable mirror of a validated review directory. |
| README/review notes | Some diagnostics and inventory review bundles emit explanatory Markdown. |
| PDF | No current Macro Regime MVP or general review-package PDF producer identified. |

Generated regime runs, comparison/review exports, CSV/Parquet evidence, PNG/SVG plots, ZIPs, logs, diagnostics, and temporary outputs are ignored by repository policy. No generated visualization artifacts are present in the current working tree under `artifacts/`; only the deliberately tracked decision-evidence SVGs are in Git.

---

## 6. Visualization-only technical debt

1. **Monolithic module.** Data loading, contract validation, contribution reconstruction, Plotly construction, CSS, HTML assembly, and JSON serialization live in one file with private helpers.
2. **Hand-built HTML/CSS.** A large f-string is the page template; there is no reusable layout or escaping boundary beyond selected text fields.
3. **No explicit visualization artifact schema/version.** “v0.1.2” appears in display/test text, but the JSON has no schema version and the CLI/docstring still says v0.1.
4. **No manifest or lineage envelope for visualization outputs.** The JSON records the run directory name but not source artifact hashes, config hashes, generator commit, output hashes, or validation status.
5. **Single-geography invocation.** County enumeration, friendly names, index creation, batch rendering, and partial-failure reporting are absent.
6. **Presentation and audit priorities compete.** All metric drilldowns are generated into the page even when collapsed, producing a large HTML payload (the smoke threshold expects more than 1 MB) and a diagnostic-report feel.
7. **Freshness is oversimplified.** A single maximum-axis-age number conflates monthly freshness with any annual/structural active-axis evidence.
8. **Limited narrative.** The explanation selects only strongest positive/negative dimensions and does not describe trajectory, transition proximity, cancellation, missingness, or materiality.
9. **Hard-coded presentation policy.** Colors, extents, horizons, regime labels/sector specs, chart sizes, and breakpoint are module constants rather than a presentation contract. Geometry boundaries are correctly governed, but presentation policy is not separated from it.
10. **No visual/accessibility test.** The smoke is strong on numerical/structural contracts but does not render in a browser, detect overlap/clipping, inspect mobile layout, or audit keyboard/color/alternative-text accessibility.
11. **No integration with a publishing shell.** Static MVP, review package, and two legacy Streamlit applications remain separate surfaces with no explicit chosen product entry point.
12. **Duplicate application entry points.** `app.py` and `app/streamlit_app.py` represent different legacy dashboards and create ambiguity about which app is authoritative; neither is governed as the Macro Regime UI.
13. **No shared visualization primitives with diagnostics.** Review diagnostics have extensive plotting/export capability, but the MVP cannot reuse them directly without risking production/experiment coupling. A production-safe presentation boundary has not been formalized.
14. **Generated-output lifecycle is generic rather than MVP-specific.** Ignore policy exists, but naming, storage location, retention, publication, invalidation, and review promotion rules for county snapshots are not documented as a dedicated contract.

---

## 7. Architectural constraints

Implementation must preserve all of the following:

1. **County-only Macro Regime scope.** The generated geography manifest/crosswalk governs identity. CBSA/metro is deferred; ZIP is reserved for Local Regime; city and other levels are outside current Macro Regime scope.
2. **Frozen engine outputs are authoritative.** Demand, Supply, Price, Affordability, and Capital Markets policy are inputs to visualization, not UI tuning parameters. Visualization work must not reopen calibration.
3. **No recomputation of production behavior.** Read persisted scores, coordinates, assignments, dates, and lineage. Contribution reconstruction is permitted only where it is a transparent registry-governed reconciliation to persisted values; never rerun transforms, normalization, scoring, geometry, or assignment inside the visualization.
4. **Artifact-first deterministic execution.** The exact immutable run must be explicit. A rerender from identical inputs must be reproducible; no implicit “latest,” mutable database query, current-date-dependent classification, or silent data substitution belongs in rendering.
5. **Fail closed/no silent fallbacks.** Missing or contradictory required state must be reported rather than filled or reclassified.
6. **Registry-driven hierarchy.** Active dimensions and metric membership/weights come from governed registries. Liquidity and Transaction Activity may not be represented as Macro Regime axis drivers.
7. **Coordinate contract.** Supply is x; Demand is y. Major-regime sectors and boundaries must match the geometry engine. Display aids must not classify or alter points.
8. **Capital Markets is shared.** It contributes independently at its governed weight to both Demand and Supply axes; national evidence may support county rendering only under the production as-of behavior already encoded in persisted evidence.
9. **Demand membership contract.** Preserve Axis → Dimension → Metric using current production membership and ordinary availability-normalized metric weights. The retired Structural/Cyclical blocks must not be restored; Market Context stays non-axis. Preserve a clean path to later Feature drilldown.
10. **Freshness/lineage semantics.** Source date and age are evidence attributes. Annual observations carried as-of are not monthly source updates. Future cadence presentation must use persisted membership/lineage metadata.
11. **Production/review separation.** Never write a visualization into an immutable production-run directory. Review/presentation exports reference source runs and have their own lifecycle.
12. **Generated artifact discipline.** Do not commit generated snapshots, review exports, plots, or production runs unless explicitly governed as small decision evidence.
13. **Deterministic review packaging.** If the MVP adopts review-package infrastructure, the manifest is authoritative, members/hashes must validate, ordering/timestamps must remain deterministic, and package extensions must not invent global requirements silently.
14. **Immutable production contracts.** Visualization is a consumer. It may improve layout, explanation, navigation, and export, but must not mutate registries or change engine scoring, geography, transition, or assignment policy.

---

## 8. Recommended next implementation sequence

1. **Lock a representative accepted source run and county review set.** Include Washington, DC and a small governed range of regimes, transitions, freshness mixes, and missing-component cases. This prevents layout decisions from being optimized against one synthetic or stale state.
2. **Write a short v0.2 presentation/output contract before changing panels.** Specify primary user questions, section order, freshness categories, county navigation, provenance, required JSON fields, responsive expectations, and what remains forensic. This minimizes churn in HTML and tests.
3. **Separate resolved view data from presentation.** Preserve `resolve_snapshot` reconciliation behavior, but introduce explicit presentation models/schema version before decomposing the renderer. This reduces the risk that layout work accidentally alters numerical contracts.
4. **Implement governed-cadence freshness at the data boundary.** Establish the persisted metadata needed for monthly versus annual/structural active-axis freshness, validate it, and add it to the JSON before designing the card. Freshness cannot be safely solved as CSS or label logic.
5. **Refine the top narrative and primary panel composition.** Rework current state, plane, latest movement, and driver summary together so the dashboard answers state/why/change before exposing audit detail.
6. **Move metric detail into clear progressive disclosure.** Retain complete Axis → Dimension → Metric reconciliation, but reduce initial page density and payload where practical. Do not begin feature drilldown in this pass.
7. **Add deterministic batch rendering and a county-only index/navigation layer.** Build from the governed geography identity and explicit source run. Do not bind to legacy Streamlit or a mutable DB merely to obtain selection UI.
8. **Add publication provenance and output validation.** Schema-version JSON, explicit run identity, generator identity, source/config hashes or references, output inventory, and deterministic rerender checks should precede deployment.
9. **Expand automated tests.** Keep Smoke 107's numerical tests; add final frozen-registry fixtures, national Capital Markets evidence coverage, row-order invariance where applicable, batch/index tests, and output-schema validation.
10. **Perform browser visual/accessibility review.** Capture desktop and narrow-width screenshots, exercise collapsed/expanded states, check labels/colors/keyboard behavior, and document acceptance. Fix layout only after data and schema stabilize.
11. **Choose and document the serving surface.** Static publication is the lowest-coupling default. If Streamlit is chosen later, make it an artifact browser that consumes generated outputs, not a second Regime Engine or a mutable-DuckDB reimplementation.
12. **Only then consider future exports or feature drilldown.** SVG/PNG/PDF and Axis → Dimension → Metric → Feature should build on the stable v0.2 view contract, avoiding a second layout rewrite.

This ordering establishes data semantics and output contracts before visual polish, then navigation/publishing before optional exports. It therefore minimizes rework and protects the frozen engine boundary.

---

## 9. Risks after the pause

1. **Version confusion.** The CLI docstring says v0.1, the renderer/footer and smoke say v0.1.2, and the roadmap describes v0.2. Resume from v0.1.2 behavior; v0.2 is unfinished.
2. **Streamlit confusion.** The visible Streamlit apps may be mistaken for the paused MVP. They are unrelated generic/forecast dashboards and violate several current MVP assumptions if used directly (mutable DuckDB, non-county geography, recomputation/aggregation in UI).
3. **Final Capital Markets values differ from pre-pause screenshots.** Recent commits promoted corrected P6 `spread_10y_2y`, F4 family weights, and the freeze. New snapshot values may change even though visualization code did not. Compare only runs with explicit identities.
4. **Registry drift versus old runs.** The renderer reads current registries alongside a supplied historical run. A run made under an older policy can fail reconciliation or, worse, be misinterpreted if provenance is not explicit. Use a compatible accepted post-freeze run and add provenance validation.
5. **Generated evidence may be absent locally.** Artifact policy intentionally ignores review/runs/plots. Historical screenshots or review pages discussed in prior work may not be in the checkout and should be regenerated from authoritative sources, not reconstructed by eye.
6. **“Visualization framework” is overloaded.** Project memory uses that phrase for production review diagnostics as well as the newer consumer MVP. Keep calibration review pages, typed review packages, and the county snapshot conceptually separate.
7. **County identity migration.** Older persisted IDs may require the governed Phase 8c crosswalk. The CLI accepts an arbitrary string and does not perform geography resolution itself.
8. **Freshness semantics are easy to corrupt.** `metric_age_days` and `max_axis_age_days` are not substitutes for cadence-aware vintage. Annual carry-as-of must not be displayed as monthly freshness.
9. **The current explanation can overstate importance.** “Strongest” is relative even when contributions are tiny, and cancellation is not shown. Layout refinement should add materiality/context without inventing new scores.
10. **Reference geometry versus classification.** The plane draws sectors for communication; persisted assignments remain authoritative. Future animation, hover, or rescaling must not locally classify points.
11. **Capital Markets appears twice by design.** Six drilldown panels are correct, not duplication to remove: Capital Markets is a member of both axes.
12. **Review roadmap items are not all MVP requirements.** Combined chronologies, heatmaps, calibration plots, and broader review automation exist in the Phase D review-tooling roadmap. Do not indiscriminately pull them into the user dashboard.
13. **No durable visual acceptance record exists.** Numerical smoke success does not prove the layout iteration was accepted. Treat existing CSS/composition as a working state, not final design approval.

### Recent commits relevant to rendered behavior

- `a8f835d` (2026-08-12), **Add macro regime visualization MVP**: created the standalone renderer, CLI, and Smoke 107.
- `ee44345` (2026-08-12), **Align visualization with Demand hierarchy**: established v0.1.2 metric drilldowns, governed structural/cyclical Demand presentation, missing-component renormalization, and the explicit v0.2 refinement roadmap.
- `a40d0c3`, `6af67d4`, and `5edc02a` (2026-08-18): finalized Capital Markets feature/family policy and freeze. They affect values in new immutable run artifacts and current registries, not page layout or renderer code.
- `7d4a60c` (2026-08-18): fixed F4 production validation aligned-date keys; relevant to selecting a validated source run, not to visualization composition.

No commit after `ee44345` modifies `visualization/regime_snapshot.py`, `scripts/build_regime_visualization_mvp.py`, or Smoke 107 in the reviewed history.

---

## 10. Files expected to change next

Probable, not authorized by this handoff:

| File/path | Expected reason |
|---|---|
| `visualization/regime_snapshot.py` | v0.2 view model, cadence freshness, panel composition, narrative, HTML/CSS, provenance, and/or renderer decomposition. |
| `scripts/build_regime_visualization_mvp.py` | batch county rendering, governed geography selection, index generation, explicit metadata, or revised CLI/version handling. |
| `scripts/smoke_tests/90_99/107_regime_visualization_mvp.py` | v0.2 schema, freshness, final frozen hierarchy, batch/index, provenance, and deterministic-output assertions. |
| `visualization/__init__.py` (new, if needed) | public visualization API/version boundary. |
| `visualization/` additional modules/templates (new, if needed) | separate view models, charts, HTML template/styles, artifact schema, or batch/index writer from numerical resolution. |
| a new visualization contract under `docs/contracts/` | document the v0.2 output/publishing/freshness contract before implementation. |
| targeted visualization fixtures under the existing test structure | representative frozen-run cases without committing generated production artifacts. |
| `.gitignore` | only if a new generated snapshot output root is introduced and is not already covered. |
| `requirements.txt` | only if a deliberately chosen browser/static-export dependency is added; none is needed for the current renderer. |

Files that should **not** change merely to resume visualization layout include engine scoring modules, geometry/assignment engines, production registries, freeze/promotion JSON, roadmap, ADRs, project memory, immutable run artifacts, and generated review exports. `app/streamlit_app.py` and root `app.py` should change only after an explicit serving-surface decision; they are not the default next files for v0.2.

---

## Handoff conclusion

Resume at **Visualization MVP v0.1.2**, not at the legacy Streamlit dashboard and not inside the calibration/review diagnostics. Preserve the resolver's artifact-only, fail-closed reconciliation; establish a v0.2 presentation/output contract; implement cadence-aware freshness; then complete the paused panel hierarchy and county navigation against an explicit validated post-freeze run. The analytical content is already sufficient for the MVP. The unfinished work is principally product hierarchy, freshness semantics, navigation/publication, provenance, and visual acceptance—not new engine computation.
