# Derived Input Freshness Policy

**Status:** Selected Production Policy
**Applies To:** Macro Regime Engine v1

## Purpose

This policy governs the treatment of stale component inputs used to calculate derived regime metrics.

The affected derived metrics are:

* `price_to_income`
* `payment_burden`
* `permit_intensity`

## Policy Horizons

Annual income and population inputs use:

```text
warning horizon = 548 days
hard horizon    = 730 days
```

Monthly components use shorter component-specific horizons defined in:

```text
config/derived_input_freshness_registry.csv
```

## Warning-Horizon Behavior

When a component exceeds its warning horizon:

* the derived value is retained;
* `stale_input_flag` is set to `true`;
* `confidence_adjustment_required` is set to `true`;
* the derived metric remains eligible for calculation and persistence.

## Hard-Horizon Behavior

When a component exceeds its hard horizon:

* the derived value is retained;
* `exceeded_horizon_flag` is set to `true`;
* `stale_input_flag` remains `true`;
* `confidence_adjustment_required` is set to `true`;
* the derived metric is not silently suppressed or deleted.

## Suppression Policy

Freshness breaches do not directly suppress derived metrics in Version 1.

Removing stale derived metrics would change metric coverage and trigger downstream weight renormalization. This could move dimension scores, axes, and regimes because data disappeared rather than because economic conditions changed.

Freshness therefore modifies confidence rather than the underlying economic-state calculation.

## Architecture

The Derived Metric Engine:

* calculates derived values;
* retains component lineage;
* does not enforce suppression.

The Freshness Policy Layer:

* evaluates component ages;
* assigns warning and hard-horizon flags;
* identifies the governing component.

The Confidence Framework will:

* consume freshness flags;
* reduce confidence when inputs are stale;
* distinguish economic-state calculation from data-quality assessment.

## Implementation Contract

Machine-readable policy:

```text
config/derived_input_freshness_registry.csv
```

Policy evaluator:

```text
regime/freshness.py
```

Persisted outputs:

```text
derived_input_component_freshness.parquet
derived_input_freshness.parquet
```

## Review Triggers

Revisit this policy if:

* annual source publication timing changes materially;
* modeled or forecasted annual inputs are introduced;
* lineage audits show materially different age distributions;
* the Confidence Framework indicates excessive penalties;
* derived metrics are added or their component definitions change.
