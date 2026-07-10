# Permit Transform Policy

**Status:** Selected Production Policy (Macro Regime Engine v1)

**Decision Date:** 2026-07-10

---

# Purpose

This document records the production feature engineering policy for permit-related metrics within the Macro Regime Engine.

It documents:

- the selected feature definitions,
- the experiments performed,
- the rationale for the selected policy,
- the rejected alternatives, and
- the implementation contract.

The machine-readable implementation lives in the configuration registries and Feature Engine. This document exists to explain *why* the implementation was selected.

---

# Scope

This policy applies only to the following canonical metrics:

- permit_activity
- permit_intensity

No other canonical metrics are affected.

---

# Production Feature Definitions

Permit metrics use moving-average based feature engineering rather than raw monthly observations.

## Level

Trailing 12-month moving average.

```text
Level(t) = MA12(t)
```

---

## Short-Term Change

Percentage difference between the trailing 3-month moving average and the trailing 12-month moving average.

```text
Short(t) = MA3(t) / MA12(t) - 1
```

---

## Long-Term Change

Percentage difference between the current trailing 12-month moving average and the trailing 12-month moving average one year earlier.

```text
Long(t) = MA12(t) / MA12(t-12) - 1
```

---

# Feature Weights

The selected feature weights remain unchanged from the original architecture.

| Feature | Weight |
|---------|-------:|
| Level | 0.25 |
| Short | 0.35 |
| Long | 0.40 |

---

# Supply Dimension Metric Weights

The selected production metric weights remain equal.

| Metric | Weight |
|--------|-------:|
| active_inventory | 0.3334 |
| permit_activity | 0.3333 |
| permit_intensity | 0.3333 |

---

# Background

The original implementation engineered permit metrics using:

- raw monthly level
- month-over-month percentage change
- year-over-year percentage change

Historical validation demonstrated that this approach produced excessive volatility.

Permit issuance is naturally lumpy because projects are permitted in discrete batches rather than continuously. As a result, month-over-month percentage changes frequently generated very large score movements despite relatively modest structural changes in housing supply.

These swings propagated through the Supply dimension, causing large Supply-axis movements and repeated oscillation between major market regimes.

---

# Experiments

Three persisted runs were compared.

## Baseline

Run ID:

```text
baseline_raw_pct_equal_supply_weights
```

Policy:

- raw monthly permit features
- equal Supply metric weights

---

## Experiment A (Selected)

Run ID:

```text
permit_ma_pct_equal_supply_weights
```

Policy:

- moving-average percentage features
- equal Supply metric weights

---

## Experiment B

Run ID:

```text
permit_ma_pct_reweighted_supply
```

Policy:

- moving-average percentage features
- Supply metric weights

```text
active_inventory = 0.50
permit_activity  = 0.25
permit_intensity = 0.25
```

---

# Validation Results

## Alameda County

| Metric | Baseline | Experiment A | Improvement |
|-------|---------:|-------------:|------------:|
| Major transitions | 122 | 62 | -49% |
| Minor transitions | 178 | 126 | -29% |
| Recovery ↔ Hyper Supply flips | 41 | 4 | -90% |
| Mean absolute Supply-axis change | 0.373 | 0.132 | -65% |
| Mean absolute permit-score change | 0.549 | 0.168 | -69% |

---

## District of Columbia

| Metric | Baseline | Experiment A | Improvement |
|-------|---------:|-------------:|------------:|
| Major transitions | 126 | 68 | -46% |
| Minor transitions | 169 | 131 | -22% |
| Recovery ↔ Hyper Supply flips | 46 | 9 | -80% |
| Mean absolute Supply-axis change | 0.376 | 0.138 | -63% |
| Mean absolute permit-score change | 0.574 | 0.173 | -70% |

---

# Decision

Experiment A was selected as the production policy.

The moving-average feature engineering substantially reduced artificial Supply-axis volatility while preserving the economic interpretation of percentage-based changes.

No feature-weight adjustments were required.

---

# Rejected Alternative

Experiment B reduced the influence of permit metrics by changing Supply metric weights to:

```text
active_inventory = 0.50
permit_activity  = 0.25
permit_intensity = 0.25
```

Although this remained materially better than the original baseline, it consistently produced:

- more major regime transitions,
- more minor regime transitions,
- larger Supply-axis movement, and
- more Recovery ↔ Hyper Supply oscillations

than Experiment A.

The weighting adjustment was therefore rejected for Version 1.

---

# Implementation Contract

The production implementation is defined by:

```text
config/feature_registry.csv
config/metric_dimension_registry.csv
regime/_01_feature_engine.py
```

This document explains the selected policy but does not override the machine-readable implementation.

---

# Persisted Evidence

The supporting experiment artifacts are stored under:

```text
artifacts/regime/runs/
```

Specifically:

```text
baseline_raw_pct_equal_supply_weights/

permit_ma_pct_equal_supply_weights/

permit_ma_pct_reweighted_supply/
```

Each run includes:

- manifest
- configuration hashes
- pipeline artifacts
- validation artifacts

These runs are immutable and provide the evidence supporting this policy decision.

---

# Future Review Triggers

This policy should be reconsidered only if one or more of the following occur:

- additional permit metrics are introduced,
- seasonal adjustment becomes available,
- Supply dimension architecture materially changes,
- historical validation across a broader market sample contradicts the current findings, or
- future volatility diagnostics identify renewed instability in permit-driven regime behavior.

Until one of these conditions is met, this document defines the production permit feature engineering policy for the Macro Regime Engine.
