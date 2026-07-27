# ADR-003: Artifact-First Deterministic Pipeline

## Status

Accepted

## Context

The Regime Engine is evaluated through immutable historical runs, challenger experiments, and downstream comparisons. Reproducibility requires preservation of the exact inputs, policy identity, outputs, and lineage used to produce each result.

## Decision

Each completed run must:

- have a unique `run_id`;
- record an `experiment_id` or production policy identity;
- persist a manifest;
- persist config and artifact hashes;
- record completion status;
- persist canonical source observations;
- persist downstream score, coordinate, regime, validation, and lineage artifacts required by the run contract;
- fail rather than silently substitute unavailable data or policy behavior.

Persisted artifacts are authoritative for comparison and review. Analytical tools should read them rather than reimplement production logic when the required records already exist.

```text
configuration + canonical observations
                 ↓
       deterministic pipeline
                 ↓
          immutable run
                 ↓
 comparison / diagnostics / visualization
```

## Consequences

- A run is not accepted merely because it completed.
- Manifest status and artifact existence must be verified.
- SHA-256 hashes protect persisted-output identity.
- Comparisons explicitly report overlapping and non-overlapping keys.
- Provenance metadata must reference the source commit that actually generated the artifacts.
