"""Smoke 143: Capital Markets Phase-1 contract, reconstruction, axes, and review."""
from __future__ import annotations
import hashlib
import re
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_feature_anatomy import (
    EXPECTED_AXIS_WEIGHTS, EXPECTED_WEIGHTS, OUTPUTS, POLICY, PROMOTION_IDENTITY,
    REVIEW_GEOS, build, load_run, write_review,
)
from scripts.build_capital_markets_feature_anatomy_diagnostic import DEFAULT_RUN


def fixture(reverse: bool = False) -> dict[str, pd.DataFrame]:
    dates = pd.date_range("2022-08-31", periods=48, freq="ME")
    registry = pd.read_csv("config/feature_registry.csv")
    metric_registry = pd.read_csv("config/metric_dimension_registry.csv")
    governed = metric_registry[metric_registry.canonical_metric_key.isin(EXPECTED_WEIGHTS)]
    key_for = governed.set_index("canonical_metric_key").metric_key.to_dict()
    source, features, normalized, native, aligned, dimensions = [], [], [], [], [], []
    for geo_index, geo in enumerate(REVIEW_GEOS):
        scores_by_date = {}
        for metric_index, metric in enumerate(EXPECTED_WEIGHTS):
            rows = registry[registry.metric_key.eq(key_for[metric])]
            for i, evaluation_date in enumerate(dates):
                native_date = evaluation_date.to_period("M").to_timestamp()  # preserve native vs aligned
                raw = 3 + metric_index * .4 + .02 * i + np.sin(i / 5 + metric_index) + geo_index * .01
                source.append({"geo_id": geo, "date": native_date, "canonical_metric_key": metric, "value": raw})
                channels = {"level": np.tanh((i - 20) / 16),
                            "short_term_change": .65 * np.sin(i / 3 + metric_index),
                            "long_term_change": .75 * np.sin(i / 8 + metric_index / 2)}
                score = .6 * channels["level"] + .2 * channels["short_term_change"] + .2 * channels["long_term_change"]
                scores_by_date.setdefault(evaluation_date, {})[metric] = score
                for feature in rows.itertuples(index=False):
                    raw_feature = raw if feature.feature_type == "level" else channels[feature.feature_type]
                    base = {"geo_id": geo, "date": native_date, "feature_key": feature.feature_key,
                            "canonical_metric_key": metric, "raw_feature_value": raw_feature}
                    features.append(base); normalized.append({**base, "feature_score": channels[feature.feature_type]})
                native.append({"geo_id": geo, "date": native_date, "canonical_metric_key": metric, "metric_score": score})
                aligned.append({"geo_id": geo, "evaluation_date": evaluation_date, "metric_date": native_date,
                                "canonical_metric_key": metric, "metric_score": score})
        for date, values in scores_by_date.items():
            cm = sum(values[m] * EXPECTED_WEIGHTS[m] for m in EXPECTED_WEIGHTS)
            other = {"demand": .25 * np.sin(date.month / 3 + geo_index),
                     "price": .15 * np.cos(date.month / 4), "affordability": -.10,
                     "supply": .20 * np.cos(date.month / 5 + geo_index)}
            dimensions.append({"geo_id": geo, "date": date, "dimension": "capital_markets", "dimension_score": cm})
            dimensions.extend({"geo_id": geo, "date": date, "dimension": dim, "dimension_score": value}
                              for dim, value in other.items())
    dimension_frame = pd.DataFrame(dimensions)
    axes = []
    axis_registry = pd.read_csv("config/axis_registry.csv")
    for (geo, date), group in dimension_frame.groupby(["geo_id", "date"]):
        values = group.set_index("dimension").dimension_score
        for axis in ("demand", "supply"):
            weights = axis_registry[axis_registry.axis.eq(axis)].set_index("dimension").dimension_weight
            score = values.reindex(weights.index).mul(weights).sum()
            axes.append({"geo_id": geo, "date": date, "axis": axis, "axis_score": score})
    frames = {"source_metrics": pd.DataFrame(source), "features": pd.DataFrame(features),
              "normalized_features": pd.DataFrame(normalized), "metric_scores": pd.DataFrame(native),
              "aligned_metric_scores": pd.DataFrame(aligned), "dimension_scores": dimension_frame,
              "axis_scores": pd.DataFrame(axes)}
    if reverse:
        frames = {key: value.iloc[::-1].reset_index(drop=True) for key, value in frames.items()}
    return frames


def main() -> None:
    protected = [Path("config/feature_registry.csv"), Path("config/metric_dimension_registry.csv"),
                 Path("config/normalization_registry.csv"), Path("config/axis_registry.csv")]
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in protected}
    assert DEFAULT_RUN == Path("artifacts/regime/runs/supply_s8_production_20260817")
    tables = build(fixture(), Path("."))
    assert set(OUTPUTS).issubset(tables)
    contract = tables["production_contract"]
    assert set(contract.metric) == set(EXPECTED_WEIGHTS)
    assert contract.groupby("metric").metric_weight.first().to_dict() == EXPECTED_WEIGHTS
    assert set(contract.configured_feature_weight) == {.6, .2}
    assert set(contract.prior_policy_identity) == {POLICY} and set(contract.prior_promotion_provenance) == {PROMOTION_IDENTITY}
    assert contract.groupby("metric").size().eq(3).all() and contract.governed_by_mw_tempered_c.all()
    assert set(contract.query("metric == 'fedfunds'").ma_window) == {"3m"}
    assert set(contract.query("metric.str.startswith('spread_')", engine="python").ma_window) == {"9m"}
    assert set(contract.query("metric.str.startswith('mortgage_')", engine="python").ma_window) == {"12m"}
    assert set(contract.query("metric.str.startswith('spread_')", engine="python").score_direction) == {"positive"}
    assert set(contract.query("not metric.str.startswith('spread_')", engine="python").score_direction) == {"negative"}
    feature = tables["feature_contributions"]
    replay = feature.groupby(["geo_id", "date", "metric"]).weighted_feature_contribution.sum()
    observed = feature.drop_duplicates(["geo_id", "date", "metric"]).set_index(["geo_id", "date", "metric"]).production_metric_score.reindex(replay.index)
    assert np.allclose(replay, observed)
    contribution = tables["dimension_contributions"]
    replay = contribution.groupby(["geo_id", "evaluation_date"]).weighted_metric_contribution.sum()
    observed = contribution.drop_duplicates(["geo_id", "evaluation_date"]).set_index(["geo_id", "evaluation_date"]).capital_markets_dimension_score.reindex(replay.index)
    assert np.allclose(replay, observed)
    assert set(tables["axis_propagation"].axis) == set(EXPECTED_AXIS_WEIGHTS)
    for axis, weight in EXPECTED_AXIS_WEIGHTS.items():
        q = tables["axis_propagation"].query("axis == @axis")
        assert np.allclose(q.configured_capital_markets_axis_weight, weight)
        assert np.allclose(q.capital_markets_weighted_contribution,
                           q.merge(tables["_dimension"][["geo_id", "date", "capital_markets_dimension_score"]],
                                   on=["geo_id", "date"]).capital_markets_dimension_score * weight)
    raw = tables["raw_chronology"]
    assert raw.native_date.dt.is_month_start.all() and tables["aligned_metric_scores"].evaluation_date.dt.is_month_end.all()
    assert tables["aligned_metric_scores"].native_metric_date.dt.is_month_start.all()
    correlations = tables["raw_feature_relationship"].correlation_to_raw.replace([np.inf, -np.inf], np.nan)
    assert correlations.notna().any() and np.isfinite(correlations.dropna()).all()
    assert set(tables["turning_point_health"].health).issubset({"pass", "indeterminate"})
    assert len(tables["historical_policy_audit"]) == 7
    governance = tables["governance_status"].iloc[0]
    assert governance.recommendation_state == "none" and governance.promotion_state == "current_production_unchanged"
    assert not governance.automated_winner and not governance.production_policy_changed
    assert not governance.demand_changed and not governance.supply_changed and not governance.capital_markets_changed
    reverse = build(fixture(True), Path("."))
    pd.testing.assert_frame_equal(tables["axis_propagation"], reverse["axis_propagation"])
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp); write_review(tables, out)
        assert all((out / f"capital_markets_phase1_{name}.csv").is_file() for name in OUTPUTS)
        svgs = list(out.glob("*.svg")); assert len(svgs) >= 43
        assert all("<path" in path.read_text() and not re.search(r"(?:NaN|Inf)", path.read_text(), re.I) for path in svgs)
        assert all(any(re.search(r"[ML][0-9]", value) for value in re.findall(r'<path d="([^"]*)"', path.read_text())) for path in svgs)
        index = (out / "capital_markets_phase1_review_index.html").read_text()
        assert "Demand and Supply remain separate" in index and "No challenger" in index
    assert before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in protected}
    try:
        load_run(Path("/absent/supply_s8_production_20260817"))
    except FileNotFoundError as exc:
        assert "no substitution" in str(exc)
    else:
        raise AssertionError("absent authoritative input did not fail closed")
    print("Smoke 143 passed: governed Capital Markets anatomy, exact reconstruction, both axes, SVGs, immutability, fail closed")


if __name__ == "__main__":
    main()
