"""Build the focused Phase 4B Affordability feature-weight review bundle."""
from __future__ import annotations

import argparse
import html
from pathlib import Path
import sys

import pandas as pd

from regime.pandas_compat import MONTH_END

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from regime.experiments.affordability_feature_weights import (  # noqa: E402
    FOCUS_GEOS, build_affordability_feature_weight_evidence,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-metrics", required=True, type=Path)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    source = pd.read_parquet(args.source_metrics) if args.source_metrics.suffix.lower() == ".parquet" else pd.read_csv(args.source_metrics)
    prices = source[source.canonical_metric_key.eq("median_sale_price") & source.geo_id.astype(str).str.endswith("__county")].copy()
    prices["date"] = pd.to_datetime(prices.date, errors="coerce")
    audit, eligible = [], set()
    for geo, frame in prices.groupby("geo_id", sort=True):
        dates = pd.DatetimeIndex(frame.date.dropna().drop_duplicates().sort_values())
        expected = pd.date_range(dates.min(), dates.max(), freq=MONTH_END) if len(dates) else pd.DatetimeIndex([])
        ok = len(dates) >= 24 and len(dates) == len(expected) and not frame.duplicated("date").any() and pd.to_numeric(frame.value, errors="coerce").notna().all()
        if ok: eligible.add(str(geo))
        audit.append({"geo_id":geo, "eligible_flag":ok, "required_focus_geo_flag":geo in FOCUS_GEOS,
                      "exclusion_reason":"eligible" if ok else "incomplete_or_invalid_monthly_price_history",
                      "observation_count":len(dates), "expected_month_count":len(expected)})
    if set(FOCUS_GEOS) - eligible:
        raise ValueError(f"Required focus geographies are not eligible: {sorted(set(FOCUS_GEOS)-eligible)}")
    local = source.canonical_metric_key.isin(["median_sale_price","median_household_income"])
    source = source[source.canonical_metric_key.eq("mortgage_30y") | (local & source.geo_id.astype(str).isin(eligible))].copy()
    if set(source.canonical_metric_key) != {"median_sale_price","median_household_income","mortgage_30y"}:
        raise ValueError("Source scope does not contain exactly the three frozen canonical inputs")
    evidence = build_affordability_feature_weight_evidence(source)
    evidence.tables["affordability_feature_weight_geo_eligibility_audit"] = pd.DataFrame(audit)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    for name, frame in evidence.tables.items(): frame.to_csv(args.output_dir / f"{name}.csv", index=False)
    matrix = evidence.tables["affordability_feature_weight_decision_matrix"]
    registry = evidence.tables["affordability_feature_weight_policy_registry"]
    review = f"""<!doctype html><meta charset='utf-8'><title>Affordability Feature-Weight Review</title>
<h1>Affordability Feature-Weight Review</h1><p><strong>Phase 4A derivation order is frozen.<br>Phase 4B is closed by human decision.<br>AFF-FW-A production weights are retained; no challenger is promoted.</strong></p>
<p>Source run: <code>{html.escape(args.source_run_id)}</code></p><h2>Frozen derive-first contract and exact A/B weights</h2>{registry.to_html(index=False)}
<h2>Metric stability and turning points</h2><p>See metric stability and geography-safe persistent turning-point CSVs.</p>
<h2>Feature contributions and cancellation</h2><p>Contributions reconstruct scores under availability renormalization; unavailable features remain null.</p>
<h2>Affordability dimension stability</h2><p>See dimension chronology, stability, and turning-point summary.</p>
<h2>DC / Alameda and recent-36m chronology</h2><p>See focus-geo and recent chronology CSVs.</p>
<h2>Downstream Demand/regime context</h2><p>Explicitly unavailable from canonical source metrics; no context is invented.</p>
<h2>Decision matrix</h2>{matrix.to_html(index=False)}"""
    (args.output_dir / "affordability_feature_weight_review.html").write_text(review, encoding="utf-8")


if __name__ == "__main__": main()
