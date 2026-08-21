# Section 1 - Project Identity

The Real Estate Intel platform is an artifact-driven market intelligence system designed to produce deterministic, explainable, and reproducible assessments of U.S. residential real estate markets.

Rather than forecasting individual metrics in isolation, the platform integrates housing, economic, demographic, labor, affordability, financing, supply, demand, and capital-market indicators into a unified framework for evaluating market conditions and identifying structural market regimes.

The system emphasizes engineering discipline over model complexity. Every production output is fully reproducible through explicit data lineage, immutable artifacts, versioned engineering contracts, and deterministic execution. Empirical diagnostics and evidence-based evaluation precede production policy changes, ensuring that implementation decisions remain grounded in measured system behavior rather than intuition alone.

The long-term objective is to build a comprehensive decision-support platform capable of monitoring market conditions across multiple geographic scales while providing transparent, auditable, and extensible intelligence for forecasting, investment analysis, and strategic real estate decision-making.

---

# Section 2 - Vision

The long-term vision of the Real Estate Intel platform is to become a comprehensive, data-driven market intelligence system for understanding, monitoring, and anticipating residential real estate market conditions across the United States.

Rather than relying on individual economic indicators or isolated forecasting models, the platform seeks to integrate diverse sources of housing, economic, demographic, labor, financing, supply, demand, and capital-market information into a unified analytical framework.

The platform is intended to support transparent, evidence-based decision-making by producing explainable market regime assessments, reproducible forecasts, and extensible analytical tools that can evolve as new data sources and methodologies become available.

---

# Section 3 - Repository Philosophy

The Real Estate Intel platform is engineered around a small set of core principles that guide both system design and day-to-day development.

- **Deterministic over opaque.** Production outputs should be reproducible and explainable.
- **Artifacts over implicit state.** Engineering decisions, feature selection, and production outputs should be represented through immutable artifacts rather than hidden application state.
- **Evidence before policy.** Empirical diagnostics should inform production decisions; observed behavior should precede changes to scoring or methodology.
- **Explicit contracts over convenience.** Data lineage, feature identity, artifact identity, and execution behavior should be defined through stable engineering contracts.
- **Freeze before extending.** Major components should be stabilized and documented before introducing additional capabilities.
- **Documentation as an engineering artifact.** Significant implementation work is not considered complete until its rationale, decisions, and operational guidance have been documented.

---

# Section 4 - Engineering Principles

The following engineering principles apply throughout the repository unless explicitly superseded by a documented design decision.

- No silent fallbacks.
- Preserve complete data lineage.
- Prefer immutable artifacts over mutable state.
- Ensure deterministic execution whenever possible.
- Validate changes through targeted smoke tests before production adoption.
- Separate diagnostics from production policy.
- Favor explicit configuration over implicit behavior.
- Maintain reproducibility across runs, environments, and execution dates.
- Record significant architectural decisions through version-controlled documentation.

---

# Section 5 - Current System Status

The Real Estate Intel platform has matured into a deterministic, artifact-driven market intelligence system with the core engineering framework substantially complete.

### Completed Foundations

- Indicator taxonomy and engineering
- Feature engineering framework
- Normalization framework
- Forecast evaluation framework
- Artifact identity and lineage contracts
- As-of alignment framework
- Dimension scoring engine
- Regime scoring infrastructure
- Production selector framework
- Smoothing experimentation framework
- Linked price-family recalculation framework
- Dimension contribution and cancellation diagnostics
- Production comparison framework
- Production review visualization framework
- Demand-dimension production engineering freeze covering Labor, GDP, ACS,
  contribution, cancellation, and weighting behavior

Supply calibration remains fully closed and frozen at human-approved S8. Capital Markets is fully calibrated and frozen at human-approved F4: 35% Long-Term Rates, 10% Fed Funds, and 55% Spreads (`automated_winner = false`). Exact metric weights are 0.11666666666666667 each for `mortgage_30y`, `mortgage_15y`, and `treasury_10y`; 0.10 for `fedfunds`; and 0.275 each for `spread_10y_2y` and `spread_10y_fedfunds`. Native policies remain P4/P2/P1/P5/P6/P9, canonical `spread_10y_2y = treasury_10y - treasury_2y` remains implemented by inversion of physical `fred_spread_2y_10y`, and equal intra-family weighting is retained. No Capital Markets calibration remains pending.

Price calibration is complete. The canonical production policy for both
`median_sale_price` and `median_ppsf` is MA12/P6: 35% Level, 20% Short, and 45%
Long. The bounded search converged without a structural MA9 advantage; prior
Price diagnostic policies are superseded by production. Affordability and all
other metric families remain unchanged by this promotion.

### Current Focus

The active workstream is **Macro Regime Visualization MVP**. Capital Markets, Supply, Affordability, Price, and Demand calibration workstreams are closed under their governed freeze contracts. After the visualization MVP, direction proceeds to CBSA/metro expansion, the local-regime layer, and regime-informed forecasting integration, subject to roadmap governance.

The Regime Engine v1.0 historical/current county-level macro baseline has been
prepared for merge to `main`. It freezes the settled production architecture as
an auditable release candidate without changing scoring behavior. After human
merge, validation, and tagging on `main`, the next stage is a lightweight
Visualization MVP.

BPS feature calibration is preserved as superseded history. Human-approved
`BPS-FINAL-80` established:
MA12 level, MA12/lag6 short ratio, and MA12/lag12 long ratio with 80/10/10
feature weights and existing positive expanding-percentile normalization. Its
then-current 0.20 Supply metric weight was subsequently superseded by S8's 0.30
Permit Activity weight. The prior transform, lag, five-policy, and 70-versus-80
finalist reviews remain the historical calibration trail.

Historical Capital Markets source review confirmed that physical `fred_spread_2y_10y`
(`2Y - 10Y`) had been passed through as canonical `spread_10y_2y`. Canonical
resolution now repairs that boundary to the governed `10Y - 2Y` chronology.
That defect-era P7 policy subsequently failed corrected revalidation; P6 replaced it. Defect-era F0-F9 evidence remains invalid historical evidence. The corrected rerun supported the later human-approved F4 closure described above.


Affordability calibration is closed at the human-approved shared MA12/P4 policy:
35% Level, 20% Short, and 45% Long for `price_to_income` and `payment_burden`.
The derive-first contract remains frozen; `AFF-FW-A` is superseded with history
preserved. Native Supply feature calibration and the S0–S9 metric-weight campaign are closed. Human-selected S8 (65/30/5) is production, so Supply is fully calibrated and frozen. The former Capital Markets follow-on review is now also closed by F4, as recorded in current status above.

Current development has shifted from infrastructure implementation toward county-level production calibration.

The Phase 8c Inventory calibration campaign now has a bounded foundation: typed
campaign and promotion-gate contracts, registry-validated MA3/MA6/MA9/MA12
structural candidates, and fixture orchestration through the existing Review
Platform. Analytical evidence generation, thresholds, gate evaluation, and any
production-policy decision remain future Phase 8c work. Phase 8c Slice 3 adds an
artifact-backed advisory scorer over the Slice 2 evidence: explicit configured
weights and directions, hard eligibility gates, warmup as a scored coverage
tradeoff, direction-aware min-max normalization, and deterministic ranking. It
does not rematerialize challengers, mutate registries, or promote the result;
human-reviewed promotion remains deferred.
Phase 8c Slice 4 adds a pure rendering boundary over those immutable results: a
self-contained HTML landing page, CSV evidence copies, deterministic PNG review
figures, lineage/hash manifest, and compressed ZIP. Phase A retains the already-
materialized target series and deterministic transition-window identities needed
for overlays. No challenger construction, normalization, scoring, registry
mutation, or promotion occurs in the renderer; a human decision remains pending.
Phase 8c now enforces an explicit county-only macro campaign boundary before
challenger materialization, independently of any optional manual county subset.
ZIP remains reserved for future local-regime work. Prior authoritative scoring
is provisional pending a corrected county-only authoritative rerun.

Current work is organized into four sequential efforts:

- Review-tooling improvements
- Inventory and Supply-axis calibration
- Price feature-weight calibration
- Deferred CBSA labor-source correctness review

The engineering emphasis remains on improving production methodology through isolated, evidence-driven changes while preserving deterministic engineering contracts.

---

# Section 6 - Current Production Contracts

The following engineering contracts are currently considered stable unless superseded through a documented design decision.

- Month-end anchors define the canonical temporal alignment.
- Production runs are deterministic and artifact-driven.
- Feature identity is frozen through selector artifacts.
- Data lineage must remain explicit throughout the processing pipeline.
- Batch identifiers and data-as-of dates uniquely identify production outputs.
- Engineering contracts should avoid silent fallbacks or implicit behavior.
- Production policy changes require supporting empirical diagnostics.
- Experimental features must remain isolated from production until formally adopted.
- All major implementation changes should be accompanied by validation artifacts and documentation.
- Production and experiment policies should reuse shared production-safe computational implementations.
- The accepted core Demand engineering policy is documented in
  `docs/decisions/demand_dimension_production_policy.md`; future Demand changes
  require an isolated Production Calibration challenger.
- Persisted production artifacts are the authoritative source for downstream comparison and review.
- County-level calibration is the current production target.
- Washington, DC county is a mandatory targeted review geography.

---

# Section 7 - Current Active Roadmap

The project is currently transitioning from foundational engineering into analytical refinement. Future work is expected to proceed in the following general sequence.

## Immediate Priorities

- Improve production-review tooling.
- Complete Inventory and Supply-axis calibration.
- Maintain the closed MA12/P6 Price production policy.
- Audit CBSA labor-source correctness.
- Continue expanding architecture documentation and ADR coverage.

## Near-Term Objectives

- Freeze promoted production policies.
- Update production registries.
- Expand regime explainability.
- Continue strengthening review automation.
- Mature the production acceptance workflow.

## Longer-Term Objectives

- Build the Macro Regime confidence framework.
- Implement historical regime backtesting.
- Develop the Forecast Regime Engine.
- Develop the Local Regime Engine.
- Expand geographic coverage after engineering contracts are validated.

Detailed implementation sequencing, milestones, and historical context are maintained in `regime_engine_roadmap.md`.

---

# Section 8 - Foundational Design Documents

The repository contains a collection of foundational design documents that predate this documentation framework. These documents capture the conceptual development of the Real Estate Intel platform and remain an important part of the project's architectural history.

Current design collections include:

- `B1_Indicator_Taxonomy/`
- `B2_Indicator_Engineering/`
- `C1_Regime_Philosophy/`
- `C2_Regime_States/`
- `C3_Macro_Regime_Scoring/`

These documents describe the design intent and conceptual evolution of the platform. While portions of the implementation have evolved, the design documents remain valuable references for understanding the rationale behind major architectural decisions.

When discrepancies exist, precedence is defined as follows:

1. Current phase decision documents define the authoritative implementation.
2. Architecture documents define the current engineering contracts.
3. Design document folders preserve the historical rationale and conceptual evolution of the system.

The design documents explain how the platform was intended to be built; the newer documentation explains how it has actually been implemented.

---

# Section 9 - Important Architectural Decisions

Several architectural principles have become foundational to the platform.

- Engineering contracts are explicitly defined and version controlled.
- Production behavior is deterministic and reproducible.
- Empirical diagnostics precede production policy changes.
- Experimental capabilities remain isolated until formally adopted.
- Major implementation phases are stabilized before new capabilities are introduced.
- Documentation is maintained as a first-class engineering artifact.
- Significant architectural decisions are preserved through version-controlled decision records rather than conversation history.

---

# Section 10 - Known Open Questions

The following topics remain active areas of investigation and should not be considered settled production policy.

- Optimal smoothing and feature weighting for the Inventory dimension.
- CBSA labor-source treatment and geography contracts.
- Geographic expansion beyond the county-level Macro Regime framework.
- Future Market Balance and Market Profile architecture.
- Opportunities to improve review automation and explainability.
- Appropriate treatment of price and affordability interactions.
- Geographic expansion strategy beyond the current macro framework.
- Evolution of the Axis Engine and Coordinate Engine.
- Future approaches to neighborhood-scale macro intelligence.
- Opportunities to improve diagnostic automation and review workflows.

As investigations are completed and decisions are finalized, these items should migrate into the appropriate architecture or decision documentation.

---

# Section 11 - Session Bootstrap

Before beginning implementation work, contributors (human or AI) should review the following documents in order:

1. `docs/README.md`
2. `docs/project_memory.md`
3. `docs/regime_engine_roadmap.md`
4. `docs/codex/CODEX_WORKFLOW.md`
5. Relevant phase decision document(s)
6. Relevant architecture document(s), including
	- docs/architecture/regime_engine_overview.md
	- docs/architecture/shared_production_experiment_implementation.md
	- relevant ADRs
7. Relevant research document(s)

When starting a new development session, establish the following before making implementation changes:

- Current branch
- Latest completed implementation phase
- Current development objective
- Current production calibration phase (if applicable)
- Active engineering contracts
- Outstanding architectural decisions
- Relevant diagnostic findings

The objective is to ensure that implementation decisions remain consistent with the current state of the project rather than relying on incomplete conversational context.

---

# Document Maintenance

This document is intended to provide a concise snapshot of the current state of the project.

It should be updated whenever:
- a major implementation phase is completed;
- production engineering contracts change;
- architectural priorities materially shift; or
- the active development roadmap is substantially revised.

Detailed implementation history belongs in phase decision documents rather than this project summary.
> **Phase 8c geography boundary:** The generated geography manifest is
> authoritative. Legacy artifact IDs use the reviewed Phase 8c identity
> crosswalk (the old manifest is migration evidence, not runtime authority),
> after which county-only scope is applied before challengers. ZIP remains
> reserved for future local work; city is outside both current macro and
> planned local regimes; all other non-county levels are excluded.

## Capital Markets native feature-policy promotion (2026-08-18)

Capital Markets native feature calibration is now closed at human-approved, metric-specific P4/P2/P1/P5/P7/P9 policies; no automated winner was used. Family metric-weight calibration remains pending at the unchanged 45/10/45 long-rate/Fed-Funds/spread allocation, so Capital Markets is not fully frozen. Supply remains frozen at S8. Demand architecture, Price, Affordability, Labor, normalization, metric weights, and Demand/Supply axis weights remain unchanged.

## Corrected Capital Markets spread feature closure (2026-08-18)

The polarity defect is repaired and validated, and targeted `spread_10y_2y` revalidation is closed at human-approved P6 (60/5/35). Original P7 remains historical defect-era evidence with `revalidation_failed` status. All six native Capital Markets feature policies are valid again. A fresh corrected feature-policy production baseline remains to be materialized, followed by an F0-F9 family-weight calibration rerun. No family-weight production decision has been made; metric and Demand/Supply axis weights remain unchanged.
