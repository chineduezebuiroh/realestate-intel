# Regime Engine Roadmap

# Section 1 - Purpose

This document serves as the canonical development roadmap for the Real Estate Intel Regime Engine.

It records the major implementation phases, architectural milestones, significant design decisions, and future development priorities that have shaped the platform.

Unlike the phase decision documents, which describe individual implementation efforts, this roadmap provides a continuous view of the project's evolution from initial concept through future planned capabilities.

This document should be updated as major phases are completed, priorities shift, or new long-term objectives are established.

---

# Section 2 - Guiding Philosophy

Development of the Regime Engine follows an incremental, evidence-driven methodology.

Major engineering efforts proceed through a consistent lifecycle:

1. Design the engineering contract.
2. Implement deterministic infrastructure.
3. Validate implementation through targeted diagnostics.
4. Freeze stable behavior.
5. Extend the platform from a stable foundation.

The objective is to continually reduce architectural uncertainty while preserving reproducibility, explainability, and engineering discipline.

Large architectural changes should build upon previously stabilized components rather than introducing multiple interacting uncertainties simultaneously.

---

# Section 3 - Development Timeline

## Phase A — Foundation

*Summary to be completed.*

---

## Phase B — Indicator Framework

Established the indicator taxonomy, engineering framework, and supporting documentation that defined the project's initial analytical foundation.

See:
- `docs/B1_Indicator_Taxonomy/`
- `docs/B2_Indicator_Engineering/`

---

## Phase C — Regime Engine Foundation

Established the deterministic engineering framework supporting the Regime Engine, including:

- artifact contracts
- feature identity
- selector framework
- as-of alignment
- normalization
- dimension scoring
- production reproducibility

Supporting design documentation:

- `docs/C1_Regime_Philosophy/`
- `docs/C2_Regime_States/`
- `docs/C3_Macro_Regime_Scoring/`

---

## Phase D — Analytical Refinement and Production Calibration

Shifted development emphasis from infrastructure construction toward empirical evaluation, production-policy integration, and county-level methodology calibration.

Major completed efforts include:

- smoothing experimentation infrastructure;
- linked Price/Affordability recalculation;
- labor and Demand diagnostics;
- contribution and cancellation analysis;
- immutable production-run comparison;
- coordinate and regime-movement review;
- transition-stability diagnostics;
- production-review visualization.
- Demand-dimension production engineering freeze synthesizing Labor, GDP, ACS,
  contribution, cancellation, and weighting diagnostics.

Current Phase D work is organized around four calibration groups:

1. review-tooling improvements;
2. Inventory and Supply-axis calibration;
3. Price feature-weight calibration;
4. deferred CBSA labor-source correctness review.

Detailed implementation history is recorded in the corresponding Phase D decision documents and persisted comparison artifacts.

---

## Phase E — Future Vision

Planned future work includes:

- Macro Regime confidence framework;
- historical backtesting;
- forecast regime generation;
- Local Regime Engine development;
- automated regime-health monitoring;
- explainability and trace tooling;
- geographic expansion after source and mapping contracts are validated;
- advanced market-balance and market-profile intelligence.

---

# Section 4 - Current Status

The deterministic Macro Regime Engine is operational through persisted regime assignment.

Completed production infrastructure includes:

- canonical metric and derived-metric resolution;
- feature engineering and normalization;
- metric, dimension, and axis scoring;
- coordinate and geometry calculation;
- major and minor regime assignment;
- immutable artifact-driven production runs;
- manifests, configuration hashes, and lineage artifacts;
- baseline-versus-candidate production comparison;
- chronological, contribution, cancellation, transition-stability, and visual review tooling.

The linked MA12 Price/Affordability policy has been integrated and evaluated in a production candidate. The candidate improved Demand-axis coherence and transition stability across many geographies while leaving the Supply axis exactly isolated.

The project is now in county-level production calibration. Remaining work is focused on improving the quality and interpretability of the existing engine, not rebuilding its core infrastructure.

---

# Section 5 - Active Development Priorities

Current work is sequenced as follows:

1. Improve production-review tooling.
   - Make Washington, DC county a mandatory review geography.
   - Prioritize county geographies in targeted reviews.
   - Add combined Demand, Supply, major-regime, minor-regime, and micro-transition chronology views.
   - Split full-history and recent-period Demand-delta heatmaps.

2. Calibrate Inventory and the Supply axis.
   - Macro Phase 8c v1 geography scope is county only. A future macro extension
     may add `cbsa_metro`; ZIP geography remains reserved for future local-regime
     campaigns and is not globally invalid.
   - The Phase 8c campaign foundation is implemented for typed contracts,
     structural-candidate validation, and fixture Review Platform packaging;
     the analytical campaign and promotion decision remain incomplete.
   - Evaluate structural smoothing alternatives for active inventory.
   - Phase 8c Slice 2 now supplies descriptive foundation evidence; Slice 3
     converts that evidence into configuration-controlled eligibility, normalized
     scoring, deterministic ranking, and an advisory recommendation. Warmup is a
     scored coverage tradeoff rather than an automatic failure. Authoritative
     scoring reuses the in-memory Phase A evidence and does not repeat challenger
     normalization; human-reviewed promotion remains a subsequent slice.
   - Phase 8c Slice 4 packages the supplied Phase A evidence and Slice 3 scores
     into a hashed, downloadable human-review directory and ZIP. Static score,
     seasonality, overlay, transition, volatility, sign-flip, and correlation
     figures are advisory only; promotion remains blocked pending human review.
   - Reassess level, short, and long feature weighting.
   - Measure seasonality, volatility, sign flips, shock timing, and regime transitions.
   - Preserve genuine structural responsiveness while reducing unnecessary jaggedness.

3. Calibrate Price feature weights.
   - Hold the linked MA12 observation policy constant.
   - Test whether the current short-feature weight contributes to the unusually broad Price-score distribution.
   - Measure downstream Affordability, Demand-axis, cancellation, and regime effects.

4. Audit CBSA labor-source correctness.
   - Verify LAUS and CES availability, source precedence, registry eligibility, and join behavior.
   - Explain the observed CBSA Demand behavior before using CBSA results for model calibration.

5. Continue formalizing architecture and production decisions through ADRs.

The emphasis remains on isolated, evidence-driven changes rather than broad feature expansion.

---

# Section 6 - Future Roadmap

Following completion of the current analytical refinement phase, anticipated development areas include:

## Regime Intelligence

- Implement the Macro Regime confidence framework.
- Complete historical backtesting.
- Develop forecast score, coordinate, regime, and transition generation.
- Continue evidence-driven refinement of regime scoring methodology.

## Geographic Expansion

- Extend macro intelligence to additional geographic resolutions where appropriate.
- Develop deterministic approaches for geographic aggregation and inheritance.

## Market Intelligence

- Expand capital-market integration.
- Continue development of capital flow analytics.
- Strengthen cross-market comparative intelligence.

## Platform Maturity

## Platform Maturity

- Expand automated diagnostics and regime-health monitoring.
- Develop metric, dimension, axis, transition, and assignment trace tools.
- Formalize artifact lifecycle and retention contracts.
- Enforce downstream row-order invariance.
- Continue consolidating shared diagnostic and comparison infrastructure.
- Maintain architecture documentation and ADR coverage.

---

# Section 7 - Major Milestones

Significant milestones in the evolution of the Regime Engine include:

- Established the foundational indicator taxonomy.
- Built the deterministic feature engineering framework.
- Introduced immutable artifact-driven production workflows.
- Implemented selector-based feature identity.
- Standardized engineering contracts across production runs.
- Completed deterministic as-of alignment.
- Established dimension-based regime scoring.
- Introduced smoothing experimentation infrastructure.
- Implemented linked price-family recalculation.
- Completed demand contribution diagnostics.
- Began dimension-specific empirical investigations.
- Transitioned the project from infrastructure development to analytical refinement.
- Promoted shared production-safe linked Price/Affordability implementation.
- Integrated linked MA12 Price/Affordability behavior into a production candidate.
- Implemented generalized persisted-run production comparison.
- Persisted detailed axis-contribution and cancellation comparison records.
- Built aggregate and geography-specific production-review visual diagnostics.
- Verified exact Supply-axis isolation during the linked Price integration.
- Established county-level production calibration as the current engineering phase.
- Formalized Regime Engine architecture and ADR documentation.

---

# Section 8 - Lessons Learned

Several engineering principles have consistently emerged throughout development.

- Deterministic systems are easier to validate, maintain, and extend.
- Stable engineering contracts reduce downstream complexity.
- Explicit artifacts outperform implicit application state.
- Diagnostic tooling frequently reveals incorrect assumptions before production deployment.
- Infrastructure maturity enables faster analytical iteration.
- Documentation significantly improves long-term maintainability and continuity across development sessions.

---

# Section 9 - Document Maintenance

This roadmap is intended to provide a high-level historical record of the Regime Engine's evolution.

It should be updated whenever:

- a major implementation phase is completed;
- long-term development priorities change;
- significant architectural milestones are reached; or
- the project's strategic direction materially evolves.

Detailed implementation decisions should be documented within the corresponding phase decision documents rather than duplicated here.
