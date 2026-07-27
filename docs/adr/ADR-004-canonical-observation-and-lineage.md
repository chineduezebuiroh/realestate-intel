# ADR-004: Canonical Observation and Lineage Contract

## Status

Accepted

## Context

Derived metrics and linked Price/Affordability recalculation require temporary substitutions without losing source identity, creating double smoothing, or mixing inconsistent structural states.

## Decision

The pipeline persists a canonical pre-feature observation frame at canonical metric grain.

Canonical observations must:

- preserve resolved source identity;
- distinguish direct and derived metrics;
- preserve source observation dates;
- preserve geography and carry-forward status;
- preserve component lineage;
- expose freshness and exceeded-horizon flags where applicable.

Temporary substitutions used for linked recalculation do not automatically replace canonical source observations used by direct feature generation.

For the selected linked Price/Affordability policy:

- raw median sale price remains the canonical direct source observation;
- raw PPSF remains the canonical direct source observation;
- direct MA12 structural features are generated through the selected feature policy;
- MA12 median sale price is substituted only into the temporary derived-metric input panel;
- price-to-income and payment burden are recomputed from that panel;
- income, mortgage-rate, and other component lineage is preserved;
- recomputed derived levels enter the canonical pre-feature frame;
- augmented lineage returned by the approved feature path becomes authoritative downstream lineage.

## Consequences

- Direct metrics and linked derived metrics can share a structural state without double smoothing.
- Derived recalculation remains explicit and auditable.
- Experiments and production must use the same lineage-aware implementation.
