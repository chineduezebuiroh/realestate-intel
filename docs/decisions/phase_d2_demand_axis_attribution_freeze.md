# Phase D2 — Demand Axis Attribution Freeze

## Status

Frozen.

## Purpose

Phase D2 converts the integrated Demand-axis chronology produced by Phase D1
into monthly attribution diagnostics.

## Input

artifacts/regime/review_exports/integrated_demand_chronology/

Canonical input:

monthly_integrated_demand_axis.csv

## Contract

For every geography-month, Phase D2 computes:

- monthly axis change
- dimension score changes
- weighted contribution changes
- contribution shares
- gross component activity
- aligned activity
- opposing activity
- component cancellation
- component cancellation rate
- effective component count
- dominant contributing dimension
- reconstruction diagnostics

## Validation

Required invariants:

- monthly axis change equals the sum of weighted contribution changes
- contribution shares sum to 100%
- effective component count ∈ [1, number_of_dimensions]
- reconstruction residuals remain within floating-point tolerance

## Canonical outputs

artifacts/regime/review_exports/demand_axis_attribution_d2/

- monthly_axis_attribution.csv
- monthly_dimension_contributions.csv
- monthly_dimension_volatility.csv
- dimension_share_of_axis_change.csv
- dimension_share_of_absolute_change.csv
- monthly_extreme_moves.csv
- geography_summary.csv
- geography_dimension_summary.csv
- candidate_summary.csv
- latest_attribution_state.csv
- axis_reconstruction_diagnostics.csv
- attribution_manifest.json

## Downstream consumer

Phase D3 — Multi-Candidate Evaluation

No downstream phase should recompute attribution.
All challenger evaluations must consume these outputs.

## Revisit triggers

Only reopen D2 if:

- attribution mathematics changes
- axis definitions change
- dimensions are added or removed
- weighting methodology changes
