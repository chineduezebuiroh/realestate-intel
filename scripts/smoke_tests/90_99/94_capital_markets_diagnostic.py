"""Focused governed smoke coverage for the Capital Markets diagnostic."""
from pathlib import Path
import tempfile
import hashlib

import pandas as pd

from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime.diagnostics.capital_markets import (
    DIMENSION, TABLE_NAMES, build_capital_markets_evidence, build_registry_audit,
    write_review_bundle,
)


def main() -> None:
    policy_paths = tuple(Path("config") / name for name in (
        "feature_registry.csv", "metric_dimension_registry.csv", "axis_registry.csv",
        "normalization_registry.csv", "source_metric_registry.csv"))
    policy_before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in policy_paths}
    audit = build_registry_audit()
    active = audit[audit.enabled & ~audit.diagnostic_only & audit.macro_enabled]
    assert set(active.canonical_metric_key) == {
        "mortgage_30y", "mortgage_15y", "fedfunds", "treasury_10y",
        "spread_2y10y", "spread_10y_fedfunds",
    }
    assert set(audit.dimension) == {DIMENSION}
    geos = ("united_states__nation", "district_of_columbia_dc__county")
    dates = pd.to_datetime(["2020-01-31", "2020-02-29", "2020-03-31", "2020-04-30"])
    normalized_rows = []
    features = active[["feature_key", "canonical_metric_key", "feature_type"]].drop_duplicates()
    for geo in geos:
        for di, date in enumerate(dates):
            for fi, row in features.reset_index(drop=True).iterrows():
                # Both signs, sign flips, and an intentionally missing metric child.
                value = ((-1) ** (di + fi)) * min(.1 + .02 * fi + .08 * di, .9)
                if date == dates[1] and row.canonical_metric_key != "mortgage_30y":
                    value = None
                if date == dates[2]:
                    value = None
                normalized_rows.append({"geo_id": geo, "date": date,
                    "canonical_metric_key": row.canonical_metric_key,
                    "feature_key": row.feature_key, "feature_type": row.feature_type,
                    "feature_score": value, "source_date": date})
    normalized = pd.DataFrame(normalized_rows)
    metrics = score_metrics(normalized)
    aligned = metrics.rename(columns={"date": "source_date"}).copy()
    aligned["evaluation_date"] = aligned["source_date"]
    aligned["metric_age_days"] = 0
    dimensions = score_dimensions(aligned)
    # Persist supporting dimensions only as axis context; no unrelated decomposition.
    axis_dims = {"demand": ("demand", "price", DIMENSION),
                 "supply": ("supply", "capital_markets", "permit_activity", "permit_intensity")}
    extra = []
    capital_dates = pd.MultiIndex.from_product([geos, dates], names=["geo_id", "date"]).to_frame(index=False)
    for row in capital_dates.itertuples(index=False):
        for dim in sorted(set(axis_dims["demand"] + axis_dims["supply"]) - {DIMENSION}):
            extra.append({"geo_id": row.geo_id, "date": row.date, "dimension": dim,
                "dimension_score": .2, "max_metric_age_days": 0})
    dimensions = pd.concat([dimensions, pd.DataFrame(extra)], ignore_index=True)
    axes = score_axes(dimensions)
    kwargs = dict(normalized_features=normalized, metric_scores=metrics,
        aligned_metric_scores=aligned, dimension_scores=dimensions, axis_scores=axes,
        native_geo_ids=(geos[0],), review_geographies=(geos[1],))
    evidence = build_capital_markets_evidence(**kwargs)
    assert tuple(evidence.tables) == TABLE_NAMES
    assert {"native_source", "county_aligned"} == set(evidence.tables["metric_to_dimension_decomposition"].grain)
    f2m = evidence.tables["feature_to_metric_decomposition"]
    assert f2m.reconciliation_status.eq("reconciled").all()
    assert f2m.weighted_contribution.gt(0).any() and f2m.weighted_contribution.lt(0).any()
    m2d = evidence.tables["metric_to_dimension_decomposition"]
    assert m2d.reconciliation_status.eq("reconciled").all()
    effective = evidence.tables["effective_weight_history"]
    assert effective.effective_weight_one.any() and effective.zero_child_period.any()
    assert evidence.tables["cancellation"].cancellation_ratio.between(0, 1).all()
    assert not evidence.tables["volatility_attribution"].empty
    assert not evidence.tables["sign_flip_attribution"].empty
    assert set(evidence.tables["provenance_reconstruction"].reconciliation_status) == {"reconciled"}
    assert evidence.tables["supply_axis_propagation"].axis.eq("supply").all()
    assert evidence.tables["demand_axis_propagation"].axis.eq("demand").all()
    with tempfile.TemporaryDirectory() as tmp:
        first = Path(tmp) / "first"; second = Path(tmp) / "second"
        review1, zip1, count = write_review_bundle(evidence, first)
        review2, zip2, _ = write_review_bundle(evidence, second)
        assert review1.read_bytes() == review2.read_bytes()
        assert zip1.read_bytes() == zip2.read_bytes()
        assert count < 300 and "promotion_state\": \"none" in (first / "manifest.json").read_text()
    mismatched_dimensions = dimensions.copy()
    target = mismatched_dimensions.dimension.eq(DIMENSION)
    mismatched_dimensions.loc[target, "dimension_score"] += .01
    mismatch = build_capital_markets_evidence(**{**kwargs, "dimension_scores": mismatched_dimensions})
    assert mismatch.tables["provenance_reconstruction"].reconciliation_status.eq("failed").all()
    assert mismatch.tables["provenance_reconstruction"].reason_code.eq("residual_exceeds_tolerance").all()
    assert policy_before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in policy_paths}
    print("Capital Markets diagnostic smoke test passed")


if __name__ == "__main__":
    main()
