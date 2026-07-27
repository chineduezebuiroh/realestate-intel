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

### Current Focus

Current development has shifted from infrastructure implementation toward county-level production calibration.

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
- Complete Price feature-weight calibration.
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
- Appropriate feature weighting within the Price dimension.
- Long-term integration of capital-market indicators into regime scoring.
- CBSA labor-source treatment and geography contracts.
- Geographic expansion beyond the county-level Macro Regime framework.
- Future Market Balance and Market Profile architecture.
- Opportunities to improve review automation and explainability.
- Long-term integration of capital-market indicators into regime scoring.
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
