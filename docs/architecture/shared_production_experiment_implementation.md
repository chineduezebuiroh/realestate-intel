# Shared Production and Experiment Implementation

## Architectural principle

Experiments evaluate policies. They should not maintain independent implementations of algorithms that also exist in production.

Whenever practical:

- shared modules own computational algorithms;
- production selects the active policy;
- experiments select baseline and challenger policies;
- production and experiments call the same shared implementation.

## Required dependency direction

```text
                 shared implementation
                    /             \
         production policy     experiment policies
```

Production code must not import computational implementations from `regime/experiments`.

Experiment modules may import shared production-safe implementations.

## Rationale

This contract:

- prevents production and experiment logic from diverging;
- makes experimental parity meaningful;
- ensures production fixes propagate to experiments;
- ensures promoted challengers use the implementation that was tested;
- reduces duplicate code and regression risk;
- gives human and AI contributors one authoritative implementation path.

## Price-family precedent

The linked Price/Affordability architecture is the first formal use of this principle.

The shared implementation owns:

- structural moving-average levels;
- temporary source substitution;
- linked derived-metric recalculation;
- short- and long-horizon feature formulas;
- substitution and derived lineage.

Production selects:

```text
price_family_ma12_structural_linked
```

Experiments may select MA6, MA9, MA12, archived aliases, and future challengers.

## Canonical observation constraint

Temporary source substitutions used to recalculate dependent metrics must not silently replace canonical source observations used by the feature engine unless production policy explicitly requires that replacement.

For the selected Price-family policy:

- raw median sale price remains canonical;
- raw PPSF remains canonical;
- the feature registry calculates their MA12 features;
- MA12 median sale price is substituted only into the derived-metric input panel;
- price-to-income and payment burden are recomputed from that temporary panel;
- recomputed derived levels enter the canonical pre-feature frame.

This prevents double smoothing and preserves explicit registry control over direct source-metric features.

## Contributor rule

Before creating new logic under `regime/experiments`, contributors must check whether the algorithm belongs in a shared production-safe module.

Experimental modules should primarily contain:

- challenger definitions;
- experiment orchestration;
- comparisons;
- diagnostics;
- artifact persistence;
- acceptance criteria.

Future Codex sessions that edit the Regime Engine should read this document, the relevant ADRs, and `docs/architecture/regime_engine_overview.md` first.
