"""Build the focused Phase 4A Affordability derivation-order evidence bundle."""
from __future__ import annotations

import argparse
import html
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from regime.experiments.affordability_derivation_order import build_affordability_derivation_evidence
from regime.affordability_derivation import build_affordability_promotion_evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-metrics", required=True, type=Path,
                        help="Canonical source-metrics CSV or Parquet from the intended frozen Price/Affordability run")
    parser.add_argument("--source-run-id", required=True,
                        help="Immutable identity of the authoritative source run")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    source_path = args.source_metrics

    if source_path.suffix.lower() == ".parquet":
        source = pd.read_parquet(source_path)
    elif source_path.suffix.lower() == ".csv":
        source = pd.read_csv(source_path)
    else:
        raise ValueError(
            f"Unsupported source-metrics format: {source_path.suffix}. "
            "Expected .csv or .parquet"
        )

    # Phase 4A calibrates the county-level macro contract, but the canonical
    # derivation path may also require shared/national source observations.
    # Therefore determine eligibility from county median-sale-price histories
    # without discarding non-county inputs needed by the canonical builder.
    required_focus_geos = {
        "district_of_columbia_dc__county",
        "alameda_county_ca__county",
    }

    county_price = source.loc[
        source["canonical_metric_key"].eq("median_sale_price")
        & source["geo_id"].astype(str).str.endswith("__county"),
        ["geo_id", "date", "value"],
    ].copy()

    if county_price.empty:
        raise ValueError(
            "No county-level median_sale_price observations found for Phase 4A"
        )

    county_price["date"] = pd.to_datetime(
        county_price["date"],
        errors="coerce",
    )

    eligibility_rows = []
    eligible_geos = set()

    for geo_id, group in county_price.groupby("geo_id", sort=True):
        dates = pd.DatetimeIndex(
            group["date"].dropna().drop_duplicates().sort_values()
        ).astype("datetime64[ns]")

        duplicate_count = int(
            group.duplicated(["date"], keep=False).sum()
        )

        if len(dates):
            expected = pd.DatetimeIndex(
                pd.date_range(
                    dates.min(),
                    dates.max(),
                    freq="M",
                )
            ).astype("datetime64[ns]")
            missing = expected.difference(dates)
            unexpected = dates.difference(expected)
        else:
            expected = pd.DatetimeIndex([])
            missing = pd.DatetimeIndex([])
            unexpected = pd.DatetimeIndex([])

        observation_count = len(dates)
        expected_month_count = len(expected)
        missing_month_count = len(missing)

        # MA12 level plus lag12 long feature requires at least 24 contiguous
        # monthly observations for a valid long structural feature.
        sufficient_history = observation_count >= 24
        contiguous_monthly = (
            observation_count == expected_month_count
            and missing_month_count == 0
            and len(unexpected) == 0
        )
        finite_values = bool(
            pd.to_numeric(
                group["value"],
                errors="coerce",
            ).notna().all()
        )

        eligible = (
            duplicate_count == 0
            and finite_values
            and sufficient_history
            and contiguous_monthly
        )

        reasons = []
        if duplicate_count:
            reasons.append("duplicate_price_dates")
        if not finite_values:
            reasons.append("non_finite_price_values")
        if not sufficient_history:
            reasons.append("insufficient_history_for_ma12_lag12")
        if not contiguous_monthly:
            reasons.append("interior_monthly_price_gaps")

        if eligible:
            eligible_geos.add(str(geo_id))

        eligibility_rows.append(
            {
                "geo_id": str(geo_id),
                "eligible_flag": bool(eligible),
                "required_focus_geo_flag":
                    str(geo_id) in required_focus_geos,
                "exclusion_reason":
                    "eligible" if eligible else "|".join(reasons),
                "first_date":
                    dates.min() if observation_count else pd.NaT,
                "last_date":
                    dates.max() if observation_count else pd.NaT,
                "observation_count": observation_count,
                "expected_month_count": expected_month_count,
                "missing_month_count": missing_month_count,
                "duplicate_date_count": duplicate_count,
                "coverage_pct":
                    (
                        100.0 * observation_count / expected_month_count
                        if expected_month_count
                        else float("nan")
                    ),
                "minimum_required_observations": 24,
            }
        )

    eligibility_audit = (
        pd.DataFrame(eligibility_rows)
        .sort_values(
            ["eligible_flag", "geo_id"],
            ascending=[False, True],
        )
        .reset_index(drop=True)
    )

    missing_focus = required_focus_geos - eligible_geos
    if missing_focus:
        raise ValueError(
            "Required Phase 4A focus geographies are not eligible: "
            f"{sorted(missing_focus)}"
        )

    # Phase 4A requires exactly the canonical inputs used by the two
    # Affordability derivations:
    #
    #   median_sale_price
    #   median_household_income
    #   mortgage_30y
    #
    # Price and income are geography-local inputs and are restricted to the
    # eligible county panel. Mortgage 30Y is a shared/national input and keeps
    # its canonical source geography. Do not retain ZIP/local observations
    # merely because they are "non-county".
    local_metric_mask = source["canonical_metric_key"].isin(
        {
            "median_sale_price",
            "median_household_income",
        }
    )
    mortgage_mask = source["canonical_metric_key"].eq(
        "mortgage_30y"
    )
    eligible_county_mask = (
        source["geo_id"].astype(str).isin(eligible_geos)
    )

    source = source.loc[
        mortgage_mask
        | (
            local_metric_mask
            & eligible_county_mask
        )
    ].copy()

    remaining_metrics = set(
        source["canonical_metric_key"].dropna().astype(str)
    )
    expected_metrics = {
        "median_sale_price",
        "median_household_income",
        "mortgage_30y",
    }

    if remaining_metrics != expected_metrics:
        raise ValueError(
            "Phase 4A source scope does not contain exactly the required "
            "canonical inputs; "
            f"expected={sorted(expected_metrics)}, "
            f"actual={sorted(remaining_metrics)}"
        )

    local_rows = source.loc[
        source["canonical_metric_key"].isin(
            {
                "median_sale_price",
                "median_household_income",
            }
        )
    ]

    unexpected_local_geos = set(
        local_rows["geo_id"].astype(str)
    ) - eligible_geos

    if unexpected_local_geos:
        raise ValueError(
            "Phase 4A retained non-eligible local geographies: "
            f"{sorted(unexpected_local_geos)[:25]}"
        )

    evidence = build_affordability_derivation_evidence(source)

    evidence.tables[
        "affordability_derivation_geo_eligibility_audit"
    ] = eligibility_audit

    # Promotion parity must consume the exact same scoped canonical source
    # used by the authoritative Phase 4A diagnostic. This keeps the
    # production-vs-selected comparison on an identical eligible geography
    # and source-input population.
    evidence.tables.update(
        build_affordability_promotion_evidence(
            source,
            evidence.tables[
                "affordability_derivation_raw_chronology"
            ],
            evidence.tables[
                "affordability_derivation_feature_chronology"
            ],
        )
    )

    args.output_dir.mkdir(parents=True, exist_ok=False)
    for name, frame in evidence.tables.items():
        frame.to_csv(args.output_dir / f"{name}.csv", index=False)
    registry = evidence.tables["affordability_derivation_policy_registry"]
    matrix = evidence.tables["affordability_derivation_decision_matrix"]
    review = f"""<!doctype html><meta charset='utf-8'><title>Affordability Derivation-Order Review</title>
<h1>Affordability Derivation-Order Review</h1>
<p><strong>Phase 4A is closed with human-selected AFF-DERIVATION-B promoted.
Feature weights remain fixed at 50/20/30 pending Phase 4B.</strong></p>
<p>Source run: <code>{html.escape(args.source_run_id)}</code></p>
<h2>Exact formulas, lineage, policies, and MA12 location</h2>{registry.to_html(index=False)}
<p>The architectures are not generally equivalent. Payment is nonlinear in mortgage rate;
income forward-fill boundaries also make MA12(price)/income differ from MA12(price/income).</p>
<h2>Chronology, stability, turning points, divergence, and event review</h2>
<p>See the namespaced CSV evidence tables. Empty downstream tables mean authoritative
production scoring context was not supplied; they are not silently reconstructed.</p>
<h2>Affordability dimension and Demand-axis/regime context</h2>
<p>Context remains secondary and requires the authoritative frozen run.</p>
<h2>Decision matrix</h2>{matrix.to_html(index=False)}
"""
    (args.output_dir / "affordability_derivation_order_review.html").write_text(review, encoding="utf-8")


if __name__ == "__main__":
    main()
