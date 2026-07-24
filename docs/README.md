# Real Estate Intel Documentation

## Purpose

This directory contains the canonical documentation for the Real Estate Intel platform.

The project has evolved beyond what can reasonably be reconstructed from Git history or conversation threads alone. These documents capture the architectural intent, engineering decisions, empirical findings, and development roadmap that define the system.

The documentation is intended for both human contributors and AI-assisted development sessions.

---

# Documentation Philosophy

The code explains **how** the system works.

These documents explain **why** it works that way.

Documentation should emphasize:

- architectural intent;
- design constraints;
- frozen decisions;
- research findings;
- operational guidance;
- future roadmap.

It should avoid duplicating implementation details already present in source code.

---

# Documentation Structure

## Project

High-level project orientation.

- `project_memory.md`
- `regime_engine_roadmap.md`
- `glossary.md`

---

## Architecture

Stable engineering contracts and system design.

Examples include:

- Regime Engine overview
- artifact contracts
- data flow
- testing strategy
- forecasting philosophy

---

## Decisions

Phase freezes and architecture decisions.

Each completed phase should record:

- objective;
- implementation;
- validation;
- findings;
- decisions made;
- decisions intentionally deferred;
- next phase.

---

## Research

Empirical findings produced by diagnostics.

Research documents describe observed system behavior.

Research documents do **not** automatically establish production policy.

---

## Runbooks

Repeatable operational procedures.

Examples include:

- running the Regime Engine;
- reproducing experiments;
- executing smoke tests;
- publishing review artifacts;
- documentation workflow.

---

# Foundational Design Documents

The repository contains a collection of design documents that predate this
documentation framework. These documents capture the conceptual development of
the Real Estate Intel platform and remain an important part of the project's
architectural history.

Current design collections include:

- `B1_Indicator_Taxonomy/`
- `B2_Indicator_Engineering/`
- `C1_Regime_Philosophy/`
- `C2_Regime_States/`
- `C3_Macro_Regime_Scoring/`

These documents describe the design intent and conceptual evolution of the
system. Some implementation details may have changed as the project matured.

When there is a discrepancy between these design documents and newer project
documentation, the following precedence applies:

1. Current phase decision documents define the authoritative implementation.
2. Architecture documents define the current engineering contracts.
3. Design document folders preserve the historical rationale and conceptual
   development of the system.

Both sets of documentation should be read together. The design documents explain
how the system was intended to be built, while the newer documentation explains
how the system has actually been implemented.

---

# Reading Order

For a new engineer or AI coding session, the recommended reading order is:

1. `project_memory.md`
2. `regime_engine_roadmap.md`
3. Relevant phase decision document
4. Relevant architecture document
5. Relevant research note

---

# Documentation Maintenance

Documentation is part of the production codebase.

A major implementation phase should generally not be considered complete until:

- implementation is stable;
- validation passes;
- canonical artifacts exist;
- documentation has been updated;
- the work has been committed.

---

# Guiding Principle

Conversation history is temporary.

Project documentation is permanent.