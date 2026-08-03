# Inventory Calibration Campaign Contract

## Phase 8c v1 geography boundary

Macro Phase 8c v1 is county-only. `allowed_geo_levels=("county",)` is a level
eligibility contract distinct from the optional, deterministically ordered
`manual_geo_ids` subset. An empty subset means every authoritative county with
both target-feature and target-source coverage, not every geography in the
baseline run. A non-empty subset fails closed for unknown IDs, out-of-scope ZIP
IDs, or counties without required target coverage.

The generated `config/geo_manifest.generated.csv` is the authoritative runtime
registry (`geo_slug` to `level`). Older persisted `geo_id` values resolve first
through the governed `config/inventory_phase8c_geo_identity_crosswalk.csv`.
Raw candidate-construction frames are filtered before materialization, while
persisted normalized features retain their complete upstream dependency
geography universe through every production scoring stage. Only after scoring
are dimensions, axes, coordinates, geometry, and regimes filtered to the
resolved campaign counties. The immutable
`inventory_campaign_geography_scope` evidence table records included and
excluded source and canonical IDs, resolution methods, levels, reasons, and
metadata source; compact campaign metadata
records deterministic included IDs/count, exclusions by level, and whether a
manual subset applied. Campaign, evidence, scoring, and bundle identities must
reconcile and fail closed on a scope mismatch.

The old `config/geo_manifest.csv` is migration evidence only, never runtime
authority. `cbsa_metro` is a future macro extension. ZIP remains supported and
reserved for future local-regime campaigns; it is excluded here. City is
outside both the current macro regime and planned local regimes. Every
non-county level is excluded from campaign outputs, but governed upstream
dependency rows are not inferred from ID suffixes or filtered before scoring.
The earlier MA12 recommendation and score are provisional until corrected
county-only authoritative Smokes 84 and 86 pass. No promotion follows from this
advisory campaign without human review.

## 1. Purpose

This contract defines the deterministic campaign used to evaluate and select the production active-inventory policy for Section 8c of the Regime Engine roadmap.

The campaign compares structural moving-average policies for active inventory, measures their effects throughout the Regime Engine, applies explicit promotion gates, and exports the resulting evidence through the existing Review and Decision Platform.

This contract does not create a second review architecture, evidence repository, recommendation engine, or campaign metadata schema. It composes existing production-safe smoothing implementations, experimental evidence generators, typed review contracts, geography policy, artifact writer, package validator, and ZIP exporter.

The campaign exists to answer two sequential production-policy questions:

1. Which active-inventory structural smoothing window should be used?
2. Given the selected or retained window, what level, short, and long feature weights should be used?

These questions must be evaluated sequentially so that smoothing-window effects and feature-weight effects remain attributable.

---

## 2. Scope

The campaign covers the production active-inventory metric and its downstream effects on:

* active-inventory engineered features;
* normalized feature values;
* active-inventory metric score;
* Supply dimension score;
* Supply-axis score;
* regime coordinates and geometry;
* major-regime assignments;
* minor-regime assignments;
* micro-transition behavior where currently supported;
* transition frequency and persistence;
* contribution and cancellation behavior;
* selected review geographies.

The initial production-calibration target is county geography.

Washington, DC county is mandatory in every targeted review.

CBSA geographies may be retained as contextual comparison geographies under the existing review geography policy, but CBSA evidence must not determine the county-level production policy while the separate CBSA source-correctness review remains unresolved.

---

## 3. Governing Engineering Contracts

The campaign must preserve the repository’s existing engineering contracts.

### 3.1 Deterministic execution

Identical campaign inputs must produce identical:

* candidate identities;
* selected geographies;
* evidence tables;
* gate results;
* manifest metadata;
* output inventory;
* artifact hashes;
* ZIP member ordering.

Any timestamps included in campaign artifacts must follow the existing review-manifest contract and must not affect analytical results.

### 3.2 Immutable production artifacts

Persisted production artifacts are authoritative inputs.

The campaign must not modify the incumbent production run.

Challenger artifacts may be built in memory for diagnostic evaluation. A persisted challenger run may be used when required by an existing comparison implementation, but it must remain separate from the incumbent run.

### 3.3 Registry-driven policy

Candidate formulas and parameters must resolve through the existing smoothing experiment registry and production-safe smoothing implementation.

The campaign contract identifies candidate policy IDs and roles. It does not duplicate or redefine their computational formulas.

No candidate may be silently substituted when its registry entry or required input is unavailable.

### 3.4 Shared production-safe implementation

Production and challenger paths must use the same production-safe computational implementation wherever their intended behavior matches.

Production code must not import computational implementations that live exclusively under `regime/experiments`.

Experimental modules may define:

* candidate policies;
* campaign orchestration;
* evidence generation;
* comparisons;
* diagnostics;
* artifact persistence;
* acceptance criteria.

### 3.5 Explicit lineage

Every challenger must preserve or produce sufficient lineage to identify:

* source observations;
* smoothing experiment;
* policy ID;
* affected metric;
* replaced feature rows;
* retained non-target rows;
* generated feature components;
* downstream challenger identity.

Non-active-inventory features and metrics must remain unchanged unless the campaign explicitly enters the later feature-weight phase and the changed weights legitimately affect downstream aggregation.

### 3.6 No silent fallbacks

Missing source data, unresolved policies, incomplete artifacts, duplicate keys, invalid dates, non-finite values, or failed reconciliation must produce explicit failures.

The campaign must not fall back to:

* another smoothing policy;
* raw observations;
* an alternate geography;
* partial downstream scoring;
* an incomplete evidence package.

---

## 4. Campaign Identity

The campaign must have a stable campaign type:

```text
inventory_calibration
```

A campaign execution must have a unique `campaign_id` and `run_id`.

Recommended identifiers are:

```text
campaign_id:
inventory_calibration_<campaign_version>

run_id:
inventory_calibration_<campaign_version>_<execution_identity>
```

The execution identity should be deterministic or explicitly supplied according to existing repository conventions.

The campaign metadata must identify:

* campaign type;
* campaign version;
* campaign phase;
* incumbent production run ID;
* baseline policy ID;
* incumbent policy ID;
* candidate policy IDs;
* target metric;
* target dimension;
* target axis;
* feature-weight policy;
* geography-policy metadata;
* source data-as-of identity when available;
* challenger materialization method;
* gate contract version.

Campaign metadata must be stored through the existing `ReviewManifest.metadata` object. No additional manifest schema is introduced.

---

## 5. Campaign Phases

Inventory calibration is one campaign with two controlled phases.

## 5.1 Phase A — Structural Window Selection

Phase A compares the structural smoothing-window candidates while holding active-inventory feature weights constant.

Required candidates are:

```text
inventory_ma3_structural
inventory_ma6_structural
inventory_ma9_structural
inventory_ma12_structural
```

Foundation implementation note: the smoothing registry contains canonical
entries for all four IDs. Each MAx policy uses the same coherent structural
definition: level is MAx, short is `MAx / lag3(MAx) - 1`, and long is
`MAx / lag12(MAx) - 1`.

All four candidates are structural policies.

The exact registry IDs may differ if an accepted repository alias already exists. The campaign must resolve one canonical candidate identity for each of the four windows and persist any alias-to-canonical mapping in campaign metadata.

Phase A must not vary active-inventory level, short, or long feature weights.

### Phase A evidence scoring (Phase 8c, Slice 3)

Slice 2 produces the eleven descriptive foundation-evidence tables. Slice 3
consumes that in-memory evidence without rebuilding challengers or rerunning
feature normalization and produces deterministic eligibility, metric-detail,
weighted-score, ranking, and campaign-recommendation tables. The recommendation
is advisory, requires human review, and cannot update a registry or production
policy; promotion is deferred to the next slice.

The provisional `inventory_candidate_scoring_v1` policy is controlled by
`config/inventory_candidate_scoring.csv`:

| Metric | Source | Aggregation | Direction | Weight |
|---|---|---|---|---:|
| `warmup_coverage_retention` | `inventory_candidate_feature_coverage.valid_rows / rows` | ratio at candidate/component, then equal mean of level/short/long | higher | 0.20 |
| `seasonality_suppression` | `inventory_candidate_calendar_month_behavior.mean_absolute_monthly_change` | equal mean of months within component, then equal component mean | lower | 0.25 |
| `volatility_reduction` | `inventory_candidate_feature_statistics.standard_deviation` | equal component mean | lower | 0.20 |
| `sign_flip_reduction` | `inventory_candidate_feature_statistics.sign_flip_rate` | equal component mean | lower | 0.15 |
| `trend_shape_preservation` | `inventory_candidate_baseline_feature_comparison.correlation` | equal component mean | higher | 0.20 |

Warmup is deliberately a bounded coverage tradeoff, not an eligibility failure:
longer windows may have legitimate baseline-only history. Hard gates instead
cover candidate/component identity, positive overlap, replacement identities,
zero challenger-only target rows, non-target parity, coverage integrity,
evidence grain, complete and unique finite calendar months 1 through 12 for
every component, and finite scoring inputs. Non-target parity accepts actual
boolean values only; malformed serialized or truthy values fail closed.

Eligible candidate metrics use direction-aware min-max normalization. A metric
that is constant across eligible candidates receives `0.5`; ineligible candidates
do not influence extrema. Ranking uses total score, then trend preservation,
then coverage retention, then the canonical MA3/MA6/MA9/MA12 order, with a
`1e-12` tie tolerance.

The same strict policy validator governs CSV-loaded and directly constructed
scoring policies. Before evidence extraction, the scoring campaign must also
reconcile with the evidence campaign across campaign/version, run and policy
identities, ordered candidates, target fields, and manual geography identity.
Each ranking row's `tie_break_reason` is a controlled value naming the actual
decisive criterion rather than listing tie-breaks that were not exercised.

The output of Phase A is one of:

```text
retain_incumbent_window
select_candidate_window
no_eligible_window
needs_review
```

A Phase A result identifies a provisional selected window for Phase B. It does not itself update production policy.

## 5.2 Phase B — Feature-Weight Calibration

Phase B begins only after Phase A identifies a selected or retained structural window.

Phase B holds that structural observation and feature-generation policy constant while evaluating active-inventory feature weights for:

```text
level
short
long
```

Particular attention must be given to the influence of the short feature.

Phase B candidate weights must be explicitly configured, registry-driven where the repository supports registry-driven challenger weights, and evaluated as isolated challenger policies.

Phase B must not reopen the smoothing-window comparison unless Phase A is invalidated by a documented implementation or evidence defect.

The output of Phase B is one of:

```text
retain_incumbent_weights
select_candidate_weights
no_eligible_weight_policy
needs_review
```

The combined campaign result identifies the proposed active-inventory production policy but does not automatically modify a production registry.

---

## 6. Campaign Roles

The campaign uses the existing review-role model.

### Baseline

The stable reference used to calculate deltas.

For Phase A, the baseline will normally be the current production active-inventory behavior.

### Incumbent

The policy currently accepted for production.

The baseline and incumbent may be the same policy, but both identities must be recorded when conceptually distinct.

### Challenger

A structural-window or feature-weight candidate being compared with the incumbent.

The Review Manifest must use:

* `source_run_id` for the reviewed incumbent or authoritative source run;
* `challenger_run_id` when a single challenger is packaged;
* manifest metadata for baseline, incumbent, and multi-candidate identities.

A campaign comparing multiple challengers may either:

1. produce one campaign-level package with candidate IDs represented in evidence tables and metadata; or
2. produce deterministic candidate subpackages plus one campaign summary package.

The first implementation should prefer one campaign-level package unless existing exporters make candidate subpackages materially simpler.

---

## 7. Campaign Model

A minimal typed campaign contract should represent:

```python
CalibrationCampaign
```

Required conceptual fields are:

```text
campaign_id
campaign_version
campaign_phase
baseline_run_id
incumbent_run_id
baseline_policy_id
incumbent_policy_id
candidate_policy_ids
target_metric
target_dimension
target_axis
geography_policy
manual_geo_ids
evidence_generators
promotion_gates
metadata
```

The campaign is responsible for:

1. validating its definition;
2. resolving candidate policies;
3. loading authoritative baseline artifacts;
4. constructing challenger artifacts;
5. executing evidence generators;
6. constructing geography candidates;
7. selecting review geographies;
8. evaluating promotion gates;
9. assembling review sections;
10. constructing the `ReviewBundle`;
11. populating `ReviewManifest.metadata`;
12. producing a `DecisionSummary`;
13. exporting and validating the review package.

The campaign must not contain production scoring algorithms.

---

## 8. Evidence Generator Contract

An evidence generator executes one coherent analytical review and returns a typed `ReviewResult`.

Existing diagnostic builders should be adapted or wrapped rather than rewritten.

An evidence generator must:

* have a stable generator ID;
* declare the campaign phase it supports;
* declare required source artifacts;
* declare whether it requires in-memory or persisted challengers;
* return named tables;
* return generated plot references when applicable;
* return generator metadata;
* validate its own critical assumptions;
* avoid writing directly into version-controlled paths.

The existing `ReviewResult` remains the standard diagnostic return object:

```text
tables
plots
metadata
```

A campaign converts evidence-generator results into the existing `ReviewBundle`.

---

## 9. Review Sections

A review section is a lightweight organizational concept used to group evidence in the campaign implementation.

It does not create a new artifact type, persistence system, or package schema.

A conceptual review section contains:

```text
section_id
title
evidence_generator_ids
table names
plot names
metadata
```

Recommended Inventory campaign sections are:

```text
campaign_definition
coverage_and_lineage
structural_window_behavior
normalization_and_metric_behavior
supply_transmission
chronology_and_shocks
regime_transitions
geography_comparison
promotion_gates
decision
```

Review sections are flattened into ordinary `ReviewBundle` tables and plots.

Section identity may be represented through:

* table subdirectories;
* deterministic table-name prefixes;
* plot sections;
* manifest metadata.

The first implementation should use the least invasive option compatible with the existing Review Platform.

---

## 10. Required Evidence

The campaign must collect evidence in three tiers.

# 10.1 Descriptive Evidence

Descriptive evidence explains each policy independently.

Required descriptive evidence includes:

### Policy definition

* canonical policy ID;
* structural window;
* transform strategy;
* level configuration;
* short configuration;
* long configuration;
* required warmup;
* active feature weights;
* source run identity.

### Coverage and lineage

* source rows;
* valid feature rows;
* first source date;
* first valid date;
* last valid date;
* warmup rows;
* geography coverage;
* lineage row count;
* target feature-key coverage;
* duplicate-key validation;
* non-target parity status.

### Feature behavior

For level, short, and long:

* mean;
* standard deviation;
* mean absolute one-month change;
* median absolute one-month change where available;
* 90th-percentile absolute one-month change;
* maximum absolute one-month change;
* sign-flip rate or turning-point frequency;
* calendar-month dependence;
* within-policy feature correlation;
* feature redundancy.

### Normalized behavior

For each feature component:

* percentile distribution;
* percentile range;
* tail rates;
* feature-score volatility;
* feature-score sign flips;
* calendar-month behavior.

### Metric, dimension, and axis behavior

* active-inventory metric-score distribution and volatility;
* Supply-dimension distribution and volatility;
* Supply-axis distribution and volatility;
* large-jump frequency;
* sign-flip frequency;
* rolling volatility where supported.

### Transition behavior

* major-transition count;
* minor-transition count;
* micro-transition count where supported;
* transition persistence at supported horizons;
* regime dwell duration;
* boundary distance;
* regime strength;
* Recovery/Hyper Supply flip count where supported.

# 10.2 Comparative Evidence

Comparative evidence measures candidate behavior against the baseline or incumbent.

Required comparative evidence includes:

### Feature comparison

* correlations with baseline;
* mean absolute difference;
* turning-point timing difference;
* seasonal-dependence difference;
* volatility difference;
* sign-flip difference;
* coverage loss.

### Metric-to-axis transmission

At minimum:

```text
active-inventory feature
→ active-inventory metric
→ Supply dimension
→ Supply axis
→ coordinate
→ regime assignment
```

The campaign must persist explicit attribution at each available level.

Required comparison measures include:

* metric-score delta;
* Supply-dimension delta;
* Supply-axis delta;
* coordinate displacement;
* regime-strength delta;
* boundary-distance delta;
* major-assignment changes;
* minor-assignment changes;
* transition-count changes;
* persistence changes;
* dwell changes.

### Contribution and cancellation

Required evidence includes:

* active-inventory weighted contribution;
* Supply-dimension contribution change;
* Supply-axis contribution change;
* reconstruction residuals;
* gross contribution activity;
* cancellation ratio;
* hidden-volatility events;
* months in which active-inventory movement was offset by other Supply inputs.

### Historical periods

Comparisons must distinguish meaningful historical periods rather than rely only on full-history averages.

At minimum, use the repository’s accepted historical segmentation where available, including:

```text
2009–2012
2013–2019
2020–2021
2022 rate shock
2023–latest available
```

The exact end date must come from the authoritative run rather than being hardcoded as a claim about available data.

# 10.3 Promotion Evidence

Promotion evidence answers whether a policy is eligible to replace the incumbent.

Promotion evidence consists of deterministic gate results, gate measurements, thresholds, exceptions, and rationale.

A candidate cannot be recommended for promotion using descriptive or comparative evidence alone.

---

## 11. Geography Selection

Geography selection must use the existing deterministic `GeographySelectionPolicy`.

The campaign must provide a geography-candidate table containing:

```text
geo_id
geo_type
selection_reason
selection_rank
selection_metric
selection_value
```

Washington, DC county must always be present as a mandatory candidate.

Automatic targeted selections must be county-first.

The campaign should nominate representative counties from the following evidence categories:

* highest candidate delta;
* highest volatility;
* lowest volatility;
* highest transition change;
* largest shock divergence;
* largest seasonality change;
* coverage exceptions.

Existing allowed selection reasons should be reused.

Where multiple evidence categories nominate the same geography, the campaign must deduplicate deterministically and preserve all relevant rationale in campaign evidence or metadata.

The campaign must include enough geographic evidence to assess whether the optimal policy appears:

```text
market_invariant
market_dependent
inconclusive
```

A market-dependent conclusion must be based on a documented, repeatable pattern. It must not be inferred merely because individual counties have different summary values.

The first production contract should prefer one production policy across county markets unless evidence demonstrates that a deterministic market-dependent policy can be defined, validated, operated, and governed without silent fallback.

---

## 12. Promotion Gate Contract

A promotion gate is a deterministic eligibility test applied to one candidate.

A minimal typed gate should represent:

```python
PromotionGate
PromotionGateResult
```

### 12.1 PromotionGate

Required conceptual fields are:

```text
gate_id
gate_version
title
description
campaign_phase
severity
required_evidence
evaluation_scope
thresholds
```

Supported severities are:

```text
blocking
warning
informational
```

### 12.2 PromotionGateResult

Required conceptual fields are:

```text
gate_id
gate_version
candidate_policy_id
status
severity
measured_values
thresholds
evaluation_scope
evidence_references
rationale
exceptions
```

Supported statuses are:

```text
pass
warn
fail
not_evaluated
```

A blocking gate with `fail` status makes the candidate ineligible.

A blocking gate with `not_evaluated` status makes the candidate ineligible unless the campaign contract explicitly defines the evidence as conditionally unavailable.

Warnings do not automatically disqualify a candidate but must appear in the final decision rationale.

Gate results must be persisted as ordinary review tables and referenced in campaign metadata.

---

## 13. Required Promotion Gates

Initial thresholds should not be invented inside the framework implementation.

Thresholds must be:

* supplied in campaign configuration;
* derived from a documented accepted policy;
* or initially marked as requiring review.

The campaign must nevertheless implement the following gate identities and measurement contracts.

### Gate 1 — Candidate Resolution

The candidate policy:

* exists;
* resolves to active inventory;
* is structural;
* has complete level, short, and long definitions;
* uses a production-safe implementation.

Severity: blocking.

### Gate 2 — Source and Lineage Integrity

The candidate:

* uses canonical active-inventory source observations;
* preserves required lineage;
* creates no duplicate target keys;
* introduces no non-finite target feature values;
* does not silently replace non-target source observations.

Severity: blocking.

### Gate 3 — Experiment Isolation

Non-target normalized features and metric scores must remain exactly unchanged during Phase A.

Non-Supply dimensions and the Demand axis must remain unchanged except where existing engine construction proves a legitimate shared-component effect. Any such effect must be explicitly attributed.

Severity: blocking.

### Gate 4 — Coverage and Maturity

The candidate must provide sufficient mature history across required review geographies.

The gate must measure:

* first valid date by feature component;
* coverage loss versus incumbent;
* number and share of mature observations;
* geography coverage exceptions.

Severity: blocking.

### Gate 5 — Feature Completeness

Level, short, and long features must all be present with their expected feature keys and configured weights.

The minimum feature count and weight sum must remain valid.

Severity: blocking.

### Gate 6 — Seasonality Reduction

The candidate must reduce unwanted seasonal persistence relative to the incumbent or remain within an accepted bound.

The gate must evaluate level, short, and long independently, with particular attention to the short feature.

Severity: blocking or warning according to configured thresholds.

### Gate 7 — Volatility Control

The candidate must reduce or acceptably control unnecessary volatility at:

* active-inventory feature level;
* normalized feature level;
* active-inventory metric level;
* Supply-dimension level;
* Supply-axis level.

The gate must not equate lower volatility with better policy in isolation.

Severity: blocking or warning according to configured thresholds.

### Gate 8 — Sign Flips and Large Jumps

The candidate must not create excessive:

* feature-score sign flips;
* metric-score sign flips;
* Supply-dimension sign flips;
* Supply-axis sign flips;
* large monthly jumps.

Severity: blocking or warning according to configured thresholds.

### Gate 9 — Structural Shock Preservation

The candidate must not suppress genuine structural shocks beyond an accepted timing or magnitude tolerance.

The gate must evaluate:

* raw active-inventory shock dates;
* candidate feature response;
* turning-point lag;
* metric response;
* Supply-axis response;
* downstream transition context.

A policy cannot pass solely because it is smoother.

Severity: blocking.

### Gate 10 — Downstream Reconciliation

Metric, dimension, and axis contributions must reconcile within the accepted numerical tolerance.

Required checks include:

* metric-to-dimension reconciliation;
* dimension-to-axis reconciliation;
* axis-change reconstruction.

Severity: blocking.

### Gate 11 — Transition Stability

The candidate must not introduce excessive major, minor, or micro transitions.

The gate must consider:

* transition counts;
* assignment-change rates;
* persistence at supported horizons;
* reversals;
* dwell duration;
* near-boundary transition behavior;
* low-regime-strength transitions.

Severity: blocking or warning according to configured thresholds.

### Gate 12 — Regime Responsiveness

The candidate must preserve meaningful regime responsiveness.

A candidate that eliminates transitions or materially delays genuine transitions without supporting evidence fails this gate even if it improves smoothness or persistence.

Severity: blocking.

### Gate 13 — Geographic Robustness

The candidate must demonstrate acceptable behavior across:

* Washington, DC county;
* high-movement counties;
* low-movement counties;
* high-seasonality counties;
* large shock-divergence counties;
* coverage exceptions.

The gate must report whether candidate performance is geographically consistent, materially heterogeneous, or inconclusive.

Severity: blocking for unexplained failures in mandatory geographies; otherwise warning or blocking according to configured policy.

### Gate 14 — Incumbent Superiority Test

Promotion requires evidence that the candidate is materially preferable to the incumbent.

A candidate should not be promoted merely because it is different or because it passes minimum safeguards.

The gate must identify the specific improvement delivered and the costs introduced.

Severity: blocking.

---

## 14. Candidate Eligibility and Selection

For each candidate, the campaign must calculate:

```text
blocking gates passed
blocking gates failed
blocking gates not evaluated
warnings
eligible flag
```

A candidate is eligible only when:

```text
blocking gates failed = 0
blocking gates not evaluated = 0
```

unless the campaign contract explicitly marks a gate as conditionally inapplicable.

Passing all blocking gates does not automatically make a candidate superior.

Candidate selection must follow this sequence:

1. eliminate ineligible candidates;
2. compare eligible candidates with the incumbent;
3. assess improvement and tradeoffs;
4. assess geographic robustness;
5. identify whether one candidate has a defensible advantage;
6. otherwise retain the incumbent or return `needs_review`.

The first implementation must not use an undocumented weighted composite score.

Ties and conflicting evidence must resolve to:

```text
retain incumbent
```

or:

```text
needs_review
```

rather than an arbitrary winner.

---

## 15. Decision Contract

The campaign must use the existing `DecisionSummary`.

Supported recommendations remain:

```text
promote
reject
needs_review
```

For a multi-candidate campaign:

* `promote` means the evidence supports a named candidate policy;
* `reject` means no challenger is eligible and the incumbent should be retained;
* `needs_review` means the gate record or comparative evidence does not support a deterministic conclusion.

A `DecisionSummary` must identify through its rationale or metadata:

* campaign phase;
* selected or retained policy;
* incumbent policy;
* eligible candidates;
* failed candidates;
* blocking gate outcomes;
* warnings;
* unresolved evidence;
* whether approval has occurred.

Gate evaluation is automated evidence processing.

Final production approval remains a governance action.

A campaign-generated `promote` recommendation must default to:

```text
approved = false
```

unless an authorized reviewer explicitly approves it through the existing decision workflow.

---

## 16. Review Package Contents

The campaign must export through the existing Review Platform.

The package must contain:

```text
manifest.json
tables/...
```

and may contain:

```text
plots/...
decision_summary.json
```

The manifest remains the canonical inventory.

Campaign-specific files must be declared in `manifest.outputs`. They must not become globally required Review Package components.

Recommended campaign tables include:

```text
campaign_definition
candidate_policy_definitions
candidate_alias_resolution
coverage_summary
lineage_summary
isolation_audit
feature_stability_summary
feature_seasonality_summary
feature_redundancy_summary
turning_point_summary
shock_response_summary
normalized_feature_stability
metric_stability_summary
metric_comparison
supply_dimension_comparison
supply_axis_comparison
contribution_summary
cancellation_summary
chronological_monthly_panel
changed_months
transition_summary
transition_persistence
dwell_summary
historical_period_summary
geography_candidates
selected_review_geographies
review_geography_rationale
promotion_gate_results
candidate_eligibility_summary
```

The exact first-version output set should reuse existing evidence-generator outputs. It should not duplicate the same evidence under multiple names merely to satisfy this recommended list.

Plots should use deterministic names and be organized by:

```text
campaign phase
candidate policy
geography
review section
```

The package must be validated as both a directory and deterministic ZIP where ZIP export is requested.

---

## 17. Existing Evidence Reuse

The campaign should initially wrap or compose existing implementations that already provide:

* structural-window diagnostics;
* normalized-feature and metric stability;
* in-memory challenger execution;
* active-inventory comparison;
* finalist comparison;
* chronological review;
* metric contribution;
* axis contribution and cancellation;
* axis volatility;
* transition audit;
* transition sensitivity;
* review geography selection;
* typed review results;
* review bundle orchestration;
* package validation;
* deterministic ZIP export.

Existing implementations may require generalization from:

```text
one baseline
one challenger
two fixed geographies
```

to:

```text
one incumbent
multiple candidates
deterministically selected geographies
```

Generalization should occur by extracting reusable parameters and shared computation. Existing accepted behavior should remain reproducible.

The campaign must not create a second implementation of an existing diagnostic algorithm.

---

## 18. Proposed Runtime Placement

The implementation should remain close to the current Review Platform without moving production computations into the review package.

A suitable initial structure is:

```text
regime/review/calibration/
    __init__.py
    campaign.py
    gates.py
    sections.py
    inventory_campaign.py
```

Responsibilities:

### `campaign.py`

* generic minimal `CalibrationCampaign`;
* campaign validation;
* evidence-generator orchestration;
* review-bundle assembly.

### `gates.py`

* `PromotionGate`;
* `PromotionGateResult`;
* deterministic gate evaluation utilities.

### `sections.py`

* lightweight review-section organization;
* conversion of `ReviewResult` outputs into bundle tables and plots.

### `inventory_campaign.py`

* Inventory candidate definition;
* Phase A and Phase B orchestration;
* Inventory evidence-generator selection;
* geography-candidate construction;
* Inventory gate definitions;
* campaign-level decision assembly.

This package must not contain:

* smoothing algorithms;
* feature normalization;
* metric scoring;
* dimension scoring;
* axis scoring;
* coordinate or geometry calculation;
* regime assignment algorithms.

Those remain in their existing production-safe modules.

If implementation shows that `sections.py` adds no meaningful value, review sections may remain an internal naming convention in `inventory_campaign.py` rather than becoming a public type.

---

## 19. Public API

The first public API should remain small.

Candidate exports are:

```python
CalibrationCampaign
PromotionGate
PromotionGateResult
build_inventory_calibration_campaign
run_inventory_calibration_campaign
```

A high-level runner should conceptually accept:

```text
baseline_run_id
incumbent_policy_id
candidate_policy_ids
campaign_phase
artifact_root
output_dir
manual_geo_ids
gate_thresholds
campaign_metadata
```

and return a structured campaign result containing:

```text
campaign definition
evidence results
selected geographies
gate results
candidate eligibility
review bundle
review manifest
decision summary
package path
ZIP path when requested
```

The implementation should reuse existing `ReviewResult`, `ReviewBundle`, `ReviewManifest`, `ReviewGeographySelection`, and `DecisionSummary` rather than replacing them.

---

## 20. Validation Requirements

At minimum, validation must prove:

### Campaign definition

* candidate IDs are unique;
* required structural windows are represented in Phase A;
* all candidates target active inventory;
* all Phase A candidates are structural;
* Phase A feature weights are identical;
* Phase B uses exactly one fixed structural window.

### Challenger integrity

* no target duplicate keys;
* valid dates;
* finite target feature values;
* required feature keys;
* non-empty lineage;
* exact non-target parity where required;
* matching evaluation calendars.

### Evidence integrity

* required evidence generators return non-empty required tables;
* expected geographies are represented;
* required policy IDs are represented;
* rates remain within valid bounds;
* dwell durations are positive;
* persistence relationships are logically consistent;
* contribution reconciliation remains within tolerance.

### Review integrity

* Washington, DC county is selected;
* automatic targeted geographies are counties;
* geography rationale is complete;
* bundle table names are unique;
* plot paths are unique;
* manifest metadata identifies campaign roles and policies;
* all declared outputs exist;
* all emitted hashes validate;
* ZIP contains no duplicate members;
* generated output remains outside tracked source paths.

---

## 21. Smoke-Test Strategy

The first implementation should add focused smoke tests rather than a full production campaign run.

Recommended progression:

### Contract smoke test

Validate:

* campaign model;
* policy uniqueness;
* phase rules;
* promotion-gate model;
* gate-result status rules.

### Fixture campaign smoke test

Using deterministic lightweight fixtures:

* define one incumbent and multiple candidates;
* return synthetic evidence;
* evaluate gates;
* construct geography candidates;
* select Washington, DC and representative counties;
* assemble a Review Bundle;
* export and validate the package.

### Inventory integration smoke test

Using existing persisted baseline artifacts and the production-safe challenger path:

* resolve the four structural-window candidates;
* build in-memory challenger artifacts;
* run a focused subset of existing evidence generators;
* verify non-target isolation;
* verify expected output tables;
* evaluate at least one pass, warning, and fail fixture or controlled result;
* export a temporary review package;
* validate directory and ZIP output.

### Regression smoke tests

Run the relevant existing smoothing, inventory, review, geography, package, contribution, volatility, chronology, and transition tests affected by the implementation.

The exact test list must be reported at completion.

---

## 22. Generated Artifact Policy

Campaign outputs are generated review artifacts.

They must remain outside version-controlled source paths and must not be committed.

Generated outputs include:

* campaign CSV files;
* plots;
* manifests;
* decision summaries;
* ZIP bundles;
* temporary challenger outputs;
* comparison exports;
* diagnostic logs.

Only source code, tests, configuration, typed contracts, ADRs, and documentation should be committed unless a small hand-authored fixture is explicitly justified.

---

## 23. Non-Goals

The initial Inventory Calibration Campaign does not:

* modify the production Inventory policy;
* modify active-inventory feature weights before Phase B;
* create a recommendation engine;
* create a Decision Scorecard;
* create a dashboard;
* implement all unfinished Section 8b visualizations;
* create an evidence database or repository;
* introduce market-dependent production behavior without a separate deterministic policy contract;
* begin Price calibration;
* begin GDP, ACS, or Macro Structural Metric calibration;
* resolve CBSA labor-source correctness;
* redesign the Review Package schema;
* persist generated campaign artifacts in Git.

---

## Implementation Status

Phase 8c Slice 2 implements the evidence-only Phase A foundation. The
`campaign_definition`, `coverage_and_lineage`, `structural_window_behavior`,
and `baseline_comparison` sections contain exactly these tables:

* `inventory_phase_a_campaign`;
* `inventory_phase_a_candidates`;
* `inventory_phase_a_feature_weights`;
* `inventory_candidate_feature_coverage`;
* `inventory_candidate_lineage_summary`;
* `inventory_candidate_target_replacement`;
* `inventory_candidate_non_target_parity`;
* `inventory_candidate_feature_statistics`;
* `inventory_candidate_feature_correlations`;
* `inventory_candidate_calendar_month_behavior`;
* `inventory_candidate_baseline_feature_comparison`.

The authoritative inputs are loaded without fallback and the four canonical
MA3, MA6, MA9, and MA12 challengers remain in memory only. The result contains
typed evidence and an existing `ReviewBundle`; it contains no decision,
promotion-gate execution, eligibility, ranking, recommendation, or winner
selection.

Normalized and downstream transmission evidence, geography policy, chronology,
shock, transition, persistence, dwell, contribution and cancellation evidence,
review-package and ZIP export, `DecisionSummary`, persisted challenger runs,
production registry changes, and Phase B weight calibration remain deferred.

## 24. Completion Criteria

Section 8c campaign infrastructure is complete when:

1. The four structural window candidates resolve through production-safe policy definitions.
2. The campaign model and promotion-gate contracts are typed and validated.
3. Existing evidence generators can be orchestrated into one Inventory campaign.
4. Evidence covers feature, metric, Supply dimension, Supply axis, coordinates, and regime effects.
5. Washington, DC and deterministically selected representative counties are reviewed.
6. Candidate eligibility is determined through explicit gate results.
7. The campaign produces an existing `DecisionSummary`.
8. The campaign exports a valid Review Bundle and deterministic ZIP.
9. No generated artifacts are committed.
10. Relevant regression smoke tests pass.
11. The completion report identifies:

* files modified;
* evidence generators reused;
* tests executed;
* generated artifacts;
* production-policy impact;
* unresolved thresholds or evidence;
* recommended commit scope.

The production Inventory policy is not considered calibrated until Phase A and Phase B have been executed on authoritative artifacts, reviewed, approved, documented, and reflected in the appropriate production policy or registry through a separate governed change.

## Complete mixed-universe challengers (PR2B)

Inventory challengers use the full authoritative incumbent normalized-feature
universe. The producer removes `redfin_inventory_level`,
`redfin_inventory_short`, and `redfin_inventory_long` only for resolved campaign
geographies, inserts candidate versions at that exact feature-and-geography
grain, and preserves target rows outside the campaign. It invokes the production
metric scorer, keeps only the campaign-declared target metric from that result,
and combines it with exact persisted incumbent sibling metrics. The production
aligner runs only on the recomputed target metric; its result is combined with
exact persisted incumbent non-target aligned metrics before dimension scoring.
It then invokes the production dimension,
Supply-axis, coordinate, geometry, and regime stages. The persisted Demand axis
is a supporting coordinate input and is preserved exactly; it is not
reinterpreted by this campaign. Capital Markets remains an input to the
challenger Supply axis wherever it exists for the incumbent.

After mixed aligned-metric dimension production, the causal challenger retains the
recomputed Supply dimension and replaces every non-Supply dimension with the
exact persisted incumbent rows at campaign geography/date grain. The splice is
schema-strict, null-safe, and duplicate-intolerant. Supply is then scored from
challenger Supply plus incumbent Capital Markets (governed weights 0.85 and
0.15 where both exist); Demand and supporting-only axes remain exact incumbent
artifacts. Coordinates, geometry, and regimes are recomputed from that mixed
axis universe. Current-pipeline recomputation differed from persisted Capital
Markets for 10,758 of 33,578 county-date rows in the historical authoritative
diagnostic; this historical fact is not a production validation threshold.

Publication includes `inventory_challenger_unaffected_parity`,
`inventory_challenger_axis_input_parity`, and
`inventory_challenger_mixed_universe_lineage`. These tables fail closed on a
missing, extra, or changed non-target feature/metric/dimension row, a changed
Capital Markets input, or a changed Demand-axis row. Strict
`dimension_to_axis` chronology coverage is limited to primary decomposition
axes; supporting-axis availability remains explicit in `axis_scope_lineage`
and coordinate reconciliation evidence.

This persisted evidence change advances the Phase A evidence contract to
`inventory_phase_a_evidence_v3`, the review bundle contract to
`calibration_review_bundle_v8`, and the producer code identity to
`inventory_phase_a_authoritative_producer_v7`. The decomposition contract
is `engine_decomposition_v6`; its arithmetic is unchanged, while construction
is campaign-scoped to active Inventory, incumbent permit activity, Supply, and
the Supply axis. The system-evidence contract is
`inventory_system_evidence_v3` because axis chronology now includes incumbent
Demand context and transition centers are target-availability filtered.

The mixed-universe constructor receives the target feature family, target metric,
target dimension, and campaign-declared primary and supporting axis scopes
explicitly. It recomputes
every primary axis, copies supporting-only axes from the incumbent, and rejects
duplicate, unknown, or inconsistent axis scopes. This permits a future
Demand-primary campaign to recompute Demand and preserve Supply without an
Inventory- or Supply-specific branch. Authoritative construction requires the
incumbent normalized-feature, metric, aligned-metric, dimension, and axis
artifacts and has no partial-universe fallback. Candidate target warmup remains
visible at both metric grains; absent leading target rows are never backfilled
from the incumbent.

Unaffected parity is schema-strict. Normalized-feature, metric, aligned-metric,
dimension, and supporting-axis layers each declare required score, count,
weight, freshness, and lineage fields. Baseline and challenger column sets must
also match exactly; missing, renamed, or unexpected columns fail with the
controlled `schema_mismatch` reason before row values are compared.
# Human-review bundle (Phase 8c Slice 4)

## Engine decomposition evidence (PR2A)

Authoritative campaign publication includes an immutable `decomposition/`
package with `feature_to_metric`, `metric_to_dimension`,
`dimension_to_axis`, `chronology_coverage`, `reconciliation_summary`,
`coordinate_reconciliation`, `regime_reconciliation`, and `axis_scope_lineage`
Parquet artifacts. The contract version is `engine_decomposition_v6`; it is recorded in both the
evidence manifest and producer completion marker and is required for current
readiness. Historical packages remain loadable only when their own hashes and
declared contracts validate.

The three additive layers expose the production scorer formula
`contribution = score * configured_weight / available_weight_sum`. Missing
values are excluded exactly where the production scorer excludes them, so the
remaining configured weights are renormalized into effective weights. The
strict absolute reconciliation tolerance is `1e-10`. Supply/Demand axis values
must equal `x_supply`/`y_demand`; regime labels are checked by calling the
production geometry classifier rather than duplicating its boundaries. Any
mismatch fails publication closed. Renderers copy and visualize these supplied
tables and must not recompute an engine stage.

Detailed additive evidence is limited to approved campaign geographies and the
Inventory causal chain: `active_inventory` feature-to-metric for every series,
`permit_activity` feature-to-metric for the incumbent only, Supply
metric-to-dimension, and Supply dimension-to-axis. Unrelated Price,
Affordability, Liquidity, Transaction Activity, Demand, Employment, GDP, and
other preserved families remain subject to exact causal parity validation but
are not expanded into campaign review panels. Coordinate and categorical-regime
reconciliation remain fail-closed validation evidence; the review does not
render a two-dimensional coordinate trajectory.

Implementation reuse classification:

* feature-to-metric: light production-safe exposure of `_03_metric_scorer`
  arithmetic;
* metric-to-dimension: wraps `_build_dimension_weights` and the persisted
  aligned-score/dimension-score artifacts;
* dimension-to-axis: wraps `_build_axis_weights` and the persisted
  dimension/axis artifacts;
* axis-to-coordinate: reuses the `_07_coordinate_engine` identity contract;
* coordinate-to-regime: reuses `assign_geometry` unchanged;
* cancellation: reuses `build_axis_cancellation_from_frames` unchanged.

Production semantics differ by layer and are preserved explicitly:

* `score_metrics` drops rows whose `feature_score` is missing, rejects missing
  registry weights, divides by the sum of the remaining positive configured
  feature weights, permits a one-feature parent, and clips the result to
  `[-1, 1]`. It carries no freshness field. Expected registry features absent
  from the score input are retained in decomposition as unavailable warmup or
  prerequisite rows.
* `score_dimensions` consumes as-of aligned metric scores, filters the registry
  to enabled, non-diagnostic, macro metrics, drops missing scores, divides by
  the remaining configured metric-weight sum, permits a one-metric parent, and
  clips to `[-1, 1]`. Metric age/freshness remains on the aligned input; the
  parent records maximum metric age. Zero total weight produces no parent.
* `score_axes` uses enabled positive axis weights, drops missing dimension
  scores, divides by the available dimension-weight sum, permits a
  one-dimension parent, and clips to `[-1, 1]`. Zero total weight produces no
  parent and maximum upstream age is retained.
* `build_coordinates` requires both Supply and Demand, maps Supply directly to
  `x_supply` and Demand directly to `y_demand`, and drops dates missing either
  axis. `assign_geometry` applies the production major/minor/quadrant boundary
  functions without additive decomposition.

Every table has a versioned required-column and unique-key schema. Additive
rows enumerate the configured child universe at each persisted parent/date,
including unavailable children, their reason, available-child count,
available-weight sum, and nullable effective weight/contribution. Parent rows
are never synthesized. Exact duplicate parent rows may collapse only after
score equality is verified; conflicting duplicates fail closed. Reconciliation
status is one of `reconciled`, `not_applicable`, `not_reconcilable`, or
`failed`, with a controlled reason code and human-readable reason.

The governed internal score range is `[-1, 1]` at all three additive layers.
Configured weights are nonnegative; zero-weight children contribute neither to
the numerator nor materially to the denominator. Whenever a production parent
exists its positive available weight is renormalized so effective weights sum
to one. No transform occurs between weighted aggregation and the defensive
`clip(-1, 1)` call. Consequently the pre-clip result is a convex combination:
if every child is in `[-1, 1]`, the aggregate is also in `[-1, 1]`. The clip is
therefore defensive against numerical drift under current registries, not a
separate scoring behavior requiring a second decomposition field. Evidence
validation rejects out-of-range children, negative weights, and invalid
effective-weight sums.

Coverage uses a layer-specific union of source/child/parent evaluation dates,
not merely persisted parent dates. It separately records source-before-child,
child-before-parent, partial-parent, one-child-only, zero-weight, warmup, and
fully reconciled counts without synthesizing a parent or backfilling a score.
For Metric → Dimension, persisted `metric_scores.date` establishes metric-score
existence while `aligned_metric_scores.evaluation_date` independently
establishes production availability on the dimension chronology; persisted
alignment lineage remains authoritative and no alternate as-of algorithm is
introduced. `zero_available_child_dates` counts all evaluation dates with no
configured child available, whereas `zero_available_weight_parent_rows` counts
only parent-present dates with zero available configured weight. Configuration
is timeless, so no fabricated configured-child date is persisted.

All child rows at one parent/date must carry identical parent score,
contribution sum, residuals, tolerance, status/pass/reason, available-child
count, and available-weight sum. Coordinate and regime reconciliation require
exact key-universe equality before values or labels are compared. Review
timelines use the campaign-governed monthly cadence: a calendar-month gap
greater than one month starts a new segment, even for a two-observation series.

The governed Building Permits identity chain is source metric
`bps_total_units`, feature family `bps_total_units_{level,short,long}`, canonical
metric `permit_activity`, and dimension `supply`. Review headings therefore use
the explicit label **Building Permits (BPS / permit_activity)**.

The advisory human-review stage occurs after Phase A evidence and deterministic
candidate scoring, and before any promotion decision. Its only computational
inputs are a reconciled `CalibrationCampaign`, `PhaseAEvidence`, and
`InventoryCandidateScoringResult`. The renderer must not materialize challengers,
normalize features, orchestrate Phase A, rerun scoring, mutate a registry, or
promote a policy.

Phase A persists the already-materialized target feature series and deterministic
transition review windows. The latter select, for every geography/component, the
largest absolute month-over-month baseline change with up to three observations
of context on either side. Rendering introduces no new analytical metric.

The stable output is
`<output_root>/inventory_calibration/<campaign_id>/<campaign_version>/`, with an
adjacent `<campaign_version>.zip`. It contains `README.md`, a self-contained
`review_summary.html`, ten CSV tables under `tables/`, campaign/scoring/lineage
metadata, and PNG figures for normalized metrics, weighted contributions,
ranking, calendar-month profiles, full time-series overlays, transition windows,
volatility, sign flips, and trend preservation. `manifest.json` records sorted
relative paths, sizes, SHA256 hashes (excluding the self-referential manifest and
ZIP), identity, lineage, and explicit no-recomputation/no-promotion flags. ZIP
entry timestamps are fixed.

The typed campaign axis scope declares `primary_decomposition_axes=("supply",)`
and `supporting_coordinate_axes=("supply", "demand")`. Dimension-to-axis evidence
is restricted to primary axes and the already-resolved campaign geography
universe; coordinate and regime checks still require both supporting axes.
`axis_scope_lineage` records each registry axis, its controlled scope role, strict
and supporting requirements, reason, and registry presence. Demand is a supporting
coordinate axis and is not contribution-decomposed in this campaign. Demand-axis
strict decomposition is deferred until a Demand-focused campaign or until
authoritative Demand-axis provenance is reconciled. The observed historical county
and CBSA mismatch is retained as a likely configuration/provenance mismatch, and a
future Demand-primary campaign fails closed unless its producing configuration
reconciles.

Run authoritative production and export as one fail-fast sequence:

```bash
PYTHONPATH=. python -u scripts/smoke_tests/80_89/84_inventory_candidate_scoring_authoritative.py && \
PYTHONPATH=. python -u scripts/smoke_tests/80_89/86_inventory_review_bundle_authoritative.py
```

The output is advisory: objective recommendation available, visual review bundle
available, human decision pending, promotion not performed.
