# Phase D1 — Integrated Demand Chronology Freeze

## Status

Provisionally frozen.

## Decision

Phase D1 establishes the canonical evaluation chronology for Demand-axis
attribution using:

- incumbent Demand dimension scores;
- incumbent Capital Markets dimension scores;
- MA12 structural-linked Price dimension scores;
- MA12 structural-linked Affordability dimension scores;
- fixed Demand-axis weights from `config/axis_registry.csv`;
- no availability-based weight renormalization.

## Production and challenger sources

Incumbent production run:

`artifacts/regime/runs/macro_regime_v1_bps120_sources`

Incumbent inputs:

- `dimension_scores.parquet`
- `axis_scores.parquet`

Selected Price/Affordability challenger:

`price_family_ma12_structural_linked`

Challenger chronology:

`artifacts/regime/comparisons/price_family_structural_windows/price_family_ma12_structural_linked/phase2_chronology/chronology_monthly.csv`

## Evaluation universe

The Phase D1 chronology is intentionally restricted to the geography-month
universe supported by the selected MA12 challenger.

Current frozen evaluation universe:

- Alameda County, California
- District of Columbia

Coverage:

- 258 total geography-month rows
- 258 complete geography-month rows
- 100% complete four-dimension coverage
- 129 months per geography
- 2015-11-30 through 2026-07-31

Phase D1 is not a full 227-geography production reconstruction. It is a
controlled challenger-evaluation chronology.

## Axis formula

The Demand axis is reconstructed as:

- Demand: 65%
- Price: 15%
- Affordability: 10%
- Capital Markets: 10%

No availability renormalization is allowed.

## Validation requirements

The frozen contract requires:

1. all four dimensions on every output row;
2. exact equality between the stored integrated axis and the sum of weighted
   dimension contributions;
3. exact challenger-universe matching;
4. Price and Affordability sourced from the MA12 challenger;
5. Demand and Capital Markets sourced from the incumbent production run;
6. integrated-versus-incumbent axis change reconstructed entirely from the
   Price and Affordability replacements.

## Canonical outputs

`artifacts/regime/review_exports/integrated_demand_chronology/`

- `integrated_dimension_history_long.csv`
- `monthly_integrated_demand_axis.csv`
- `monthly_integrated_demand_axis_long.csv`
- `coverage_summary.csv`
- `missing_dimension_rows.csv`
- `latest_integrated_state.csv`
- `axis_impact_summary.csv`
- `integration_manifest.json`

## Downstream contract

Phase D2 Demand-axis attribution must consume:

`monthly_integrated_demand_axis.csv`

It must not consume the earlier Price-family-only chronology directly.

## Revisit triggers

Revisit the D1 freeze only if:

- the selected Price/Affordability candidate changes;
- the Demand-axis weights change;
- the evaluation geography universe expands;
- incumbent Demand or Capital Markets candidates change;
- Phase D2 exposes an unreconciled reconstruction or lineage defect.
