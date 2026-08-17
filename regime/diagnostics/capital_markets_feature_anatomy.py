"""Capital Markets Phase-1 incumbent-anatomy and shared-axis audit.

All score arithmetic is delegated to the canonical anatomy implementation.  This
adapter adds the governed MW-TEMPERED-C contract, metric-to-dimension contribution
evidence, and separate Demand/Supply propagation.  It never constructs a
challenger or changes production configuration.
"""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd

from regime.diagnostics import price_feature_anatomy as canonical
from regime.diagnostics.capital_markets_ma import detect_turning_points

DIMENSION = "capital_markets"
POLICY = "MW-TEMPERED-C"
PROMOTION_IDENTITY = "capital_markets_mw_tempered_c_2026_08_07"
EXPECTED_WEIGHTS = {
    "mortgage_30y": .15, "mortgage_15y": .15, "treasury_10y": .15,
    "fedfunds": .10, "spread_10y_2y": .225, "spread_10y_fedfunds": .225,
}
EXPECTED_WINDOWS = {
    "mortgage_30y": "12m", "mortgage_15y": "12m", "treasury_10y": "12m",
    "fedfunds": "3m", "spread_10y_2y": "9m", "spread_10y_fedfunds": "9m",
}
EXPECTED_AXIS_WEIGHTS = {"demand": .10, "supply": .15}
REVIEW_GEOS = canonical.REVIEW_GEOS
DC = canonical.DC
OUTPUTS = (
    "production_contract", "raw_chronology", "feature_anatomy",
    "normalized_features", "feature_contributions", "feature_statistics",
    "metric_statistics", "dimension_statistics", "dimension_contributions",
    "axis_propagation", "cross_axis_comparison", "monthly_coverage",
    "seasonality_noise", "turning_point_health", "historical_policy_audit",
    "evaluation_matrix", "governance_status",
)


def resolve_contract(root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    contract, registry = canonical.resolve_contract(root, DIMENSION)
    weights = contract.groupby("metric").metric_weight.first().to_dict()
    if set(weights) != set(EXPECTED_WEIGHTS) or any(
            not np.isclose(weights[key], value) for key, value in EXPECTED_WEIGHTS.items()):
        raise ValueError(f"MW-TEMPERED-C membership/weights disagree with governance: {weights}")
    if not np.allclose(contract.configured_feature_weight, contract.feature_type.map(
            {"level": .60, "short": .20, "long": .20})):
        raise ValueError("MW-TEMPERED-C feature weights are not governed 60/20/20")
    for metric, window in EXPECTED_WINDOWS.items():
        rows = contract[contract.metric.eq(metric)]
        if not rows.window_lag_definition.fillna("").str.startswith(window).all():
            raise ValueError(f"governed window mismatch for {metric}")
    spreads = contract.metric.str.startswith("spread_")
    if not contract.loc[spreads & ~contract.feature_type.eq("level"), "transform"].eq("ma_difference").all():
        raise ValueError("governed spread change transforms are not arithmetic differences")
    if not contract.loc[~spreads & ~contract.feature_type.eq("level"), "transform"].eq("ma_pct_change").all():
        raise ValueError("governed rate change transforms are not proportional changes")
    if not contract.loc[spreads, "score_direction"].eq("positive").all() or not contract.loc[~spreads, "score_direction"].eq("negative").all():
        raise ValueError("Capital Markets polarity differs from governed policy")
    contract = contract.copy()
    contract["prior_policy_identity"] = POLICY
    contract["prior_promotion_provenance"] = PROMOTION_IDENTITY
    contract["governed_by_mw_tempered_c"] = True
    contract["ma_window"] = contract.window_lag_definition.str.split("/").str[0]
    contract["lag"] = contract.window_lag_definition.str.extract(r"lag([^/]+)$", expand=False).fillna("")
    return contract, registry


def load_run(run: Path) -> dict[str, pd.DataFrame]:
    artifacts = canonical.load_run(run)
    artifacts["axis_scores"] = canonical._read(run, "axis_scores")
    return artifacts


def _native_raw(artifacts: dict[str, pd.DataFrame], contract: pd.DataFrame) -> pd.DataFrame:
    q = canonical._dates(artifacts["source_metrics"])
    metric_col = canonical._metric_col(q)
    value_col = canonical._value_col(q, ("value", "metric_value", "raw_value"))
    identities = pd.concat([
        contract[["registry_metric_key", "metric"]].drop_duplicates(),
        contract[["metric"]].drop_duplicates().assign(
            registry_metric_key=lambda x: x.metric)[["registry_metric_key", "metric"]],
    ]).drop_duplicates()
    q = q.rename(columns={metric_col: "registry_metric_key", value_col: "raw_value"}).merge(
        identities, on="registry_metric_key", validate="many_to_one")
    q = q[q.geo_id.isin(REVIEW_GEOS)][["geo_id", "date", "metric", "raw_value"]]
    missing = set(EXPECTED_WEIGHTS) - set(q.metric)
    if missing:
        raise ValueError(f"governed raw metrics missing after identity resolution: {sorted(missing)}")
    if q.duplicated(["geo_id", "date", "metric"]).any():
        raise ValueError("duplicate native Capital Markets chronology")
    q = q.rename(columns={"date": "native_date"}).sort_values(["metric", "geo_id", "native_date"])
    q["frequency"] = "monthly"
    q["calendar_month"] = q.native_date.dt.to_period("M").astype(str)
    q["missing"] = q.raw_value.isna()
    q["source_age_days"] = pd.NA
    grouped = q.groupby(["geo_id", "metric"], sort=False)
    q["month_gap"] = grouped.native_date.diff().dt.days.gt(35)
    q["monthly_movement"] = grouped.raw_value.diff()
    q["structural_trend_12m"] = grouped.raw_value.transform(lambda values: values.rolling(12, min_periods=6).mean())
    q["cyclical_movement"] = q.raw_value - q.structural_trend_12m
    scale = grouped.raw_value.transform(lambda values: (values - values.median()).abs().median())
    q["outlier_flag"] = q.cyclical_movement.abs().gt(6 * scale.replace(0, np.nan))
    q["date"] = q.native_date                 # compatibility, explicitly native
    return q.reset_index(drop=True)


def _dimension_contributions(aligned: pd.DataFrame, dimensions: pd.DataFrame) -> pd.DataFrame:
    q = aligned.rename(columns={"date": "evaluation_date", "metric_date": "native_metric_date"}).copy()
    q["configured_metric_weight"] = q.metric.map(EXPECTED_WEIGHTS)
    q["metric_available"] = q.aligned_metric_score.notna()
    keys = [q.geo_id, q.evaluation_date]
    q["available_configured_weight_sum"] = q.configured_metric_weight.where(q.metric_available, 0).groupby(keys).transform("sum")
    q["effective_metric_weight"] = q.configured_metric_weight.div(q.available_configured_weight_sum).where(q.metric_available)
    q["weighted_metric_contribution"] = q.aligned_metric_score * q.effective_metric_weight
    actual = dimensions[["geo_id", "date", "capital_markets_dimension_score"]].rename(columns={"date": "evaluation_date"})
    q = q.merge(actual, on=["geo_id", "evaluation_date"], validate="many_to_one")
    replay = q.groupby(["geo_id", "evaluation_date"]).weighted_metric_contribution.sum(min_count=1)
    observed = q.drop_duplicates(["geo_id", "evaluation_date"]).set_index(
        ["geo_id", "evaluation_date"]).capital_markets_dimension_score.reindex(replay.index)
    if not np.allclose(replay, observed, equal_nan=True, atol=1e-12):
        raise ValueError("metric contributions do not reconstruct Capital Markets dimension")
    return q.sort_values(["geo_id", "evaluation_date", "metric"]).reset_index(drop=True)


def _axis_propagation(artifacts: dict[str, pd.DataFrame], root: Path) -> pd.DataFrame:
    registry = pd.read_csv(root / "config/axis_registry.csv")
    enabled = registry.enabled.astype(str).str.lower().isin(("true", "1", "yes"))
    governed = registry[enabled & registry.dimension.eq(DIMENSION)]
    weights = governed.set_index("axis").dimension_weight.astype(float).to_dict()
    if weights != EXPECTED_AXIS_WEIGHTS:
        raise ValueError(f"Capital Markets shared-axis contract mismatch: {weights}")
    dims = canonical._dates(artifacts["dimension_scores"])
    dims = dims[dims.geo_id.isin(REVIEW_GEOS)]
    axes = canonical._dates(artifacts["axis_scores"])
    axis_col = next((c for c in ("axis", "axis_name") if c in axes), None)
    score_col = canonical._value_col(axes, ("axis_score", "score"))
    rows = []
    for axis, cm_weight in weights.items():
        ar = registry[enabled & registry.axis.eq(axis)][["dimension", "dimension_weight"]].copy()
        wide = dims[dims.dimension.isin(ar.dimension)].pivot(index=["geo_id", "date"], columns="dimension", values="dimension_score")
        configured = ar.set_index("dimension").dimension_weight.astype(float)
        valid = wide.notna(); denominator = valid.mul(configured).sum(axis=1)
        effective = valid.mul(configured).div(denominator, axis=0)
        contributions = wide.mul(effective)
        replay = contributions.sum(axis=1, min_count=1)
        observed = axes[axes[axis_col].astype(str).str.lower().eq(axis)].set_index(["geo_id", "date"])[score_col].reindex(replay.index)
        if not np.allclose(replay, observed, equal_nan=True, atol=1e-12):
            raise ValueError(f"Capital Markets propagation does not reconstruct {axis} axis")
        cm = contributions[DIMENSION]
        other = contributions.drop(columns=DIMENSION).sum(axis=1, min_count=1)
        frame = pd.DataFrame({"capital_markets_weighted_contribution": cm,
            "other_dimension_contribution": other, "axis_score": observed}).reset_index()
        frame.insert(0, "axis", axis); frame["configured_capital_markets_axis_weight"] = cm_weight
        frame["capital_markets_contribution_share"] = cm.abs().div(contributions.abs().sum(axis=1)).values
        frame["opposes_other_dimensions"] = (np.sign(cm) != np.sign(other)).values
        frame["cancellation_attributable_to_disagreement"] = (1 - replay.abs().div(contributions.abs().sum(axis=1).replace(0, np.nan))).values
        frame["capital_markets_changes_axis_direction"] = (np.sign(replay) != np.sign(other)).values
        frame["capital_markets_dominates_axis"] = (cm.abs() > other.abs()).values
        rows.append(frame)
    return pd.concat(rows, ignore_index=True).sort_values(["axis", "geo_id", "date"]).reset_index(drop=True)


def _cross_axis(axis: pd.DataFrame) -> pd.DataFrame:
    values = ["capital_markets_weighted_contribution", "other_dimension_contribution", "axis_score",
              "capital_markets_contribution_share", "opposes_other_dimensions",
              "capital_markets_changes_axis_direction", "capital_markets_dominates_axis"]
    wide = axis.pivot(index=["geo_id", "date"], columns="axis", values=values)
    wide.columns = [f"{axis_name}_{value}" for value, axis_name in wide.columns]
    out = wide.reset_index()
    out["amplifies_both_axes"] = (np.sign(out.demand_capital_markets_weighted_contribution) == np.sign(out.demand_other_dimension_contribution)) & (np.sign(out.supply_capital_markets_weighted_contribution) == np.sign(out.supply_other_dimension_contribution))
    out["opposes_one_reinforces_other"] = out.demand_opposes_other_dimensions != out.supply_opposes_other_dimensions
    out["dominates_one_axis_only"] = out.demand_capital_markets_dominates_axis != out.supply_capital_markets_dominates_axis
    return out.sort_values(["geo_id", "date"]).reset_index(drop=True)


def _turning_health(metric_scores: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (metric, geo), group in metric_scores.groupby(["metric", "geo_id"]):
        turns = detect_turning_points(group[["date", "production_metric_score"]], "production_metric_score", include_rejected=True)
        qualified = int(turns.qualified.sum()) if len(turns) else 0
        rejected = turns.loc[~turns.qualified, "rejection_reason"].value_counts().to_dict() if len(turns) else {}
        rows.append({"metric": metric, "geo_id": geo, "qualified_turn_count": qualified,
            "rejected_turn_count": int(len(turns) - qualified),
            "rejection_reasons": "|".join(f"{k}:{v}" for k, v in sorted(rejected.items())),
            "obvious_failure_mode": "no qualified turns" if qualified == 0 else "",
            "health": "pass" if qualified else "indeterminate"})
    return pd.DataFrame(rows)


def _native_feature_anatomy(artifacts: dict[str, pd.DataFrame], contract: pd.DataFrame,
                            raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Join native observations by explicit month identity without changing dates."""
    features = canonical._dates(artifacts["features"])
    fmap = contract.set_index("feature_key")[["metric", "feature_type"]]
    features = features[features.feature_key.isin(fmap.index) & features.geo_id.isin(REVIEW_GEOS)].merge(
        fmap, left_on="feature_key", right_index=True, validate="many_to_one")
    features = features.rename(columns={"date": "native_feature_date"})
    features["calendar_month"] = features.native_feature_date.dt.to_period("M").astype(str)
    joined = features[["geo_id", "native_feature_date", "calendar_month", "metric", "feature_type",
                       "feature_key", "raw_feature_value"]].merge(
        raw[["geo_id", "native_date", "calendar_month", "metric", "raw_value"]],
        on=["geo_id", "calendar_month", "metric"], validate="many_to_one")
    joined["date"] = joined.native_feature_date
    relationships = []
    for (metric, feature_type, geo), group in joined.groupby(["metric", "feature_type", "geo_id"]):
        group = group.sort_values("calendar_month")
        relationships.append({"metric": metric, "feature_type": feature_type, "geo_id": geo,
            "correlation_to_raw": group.raw_feature_value.corr(group.raw_value),
            "correlation_to_raw_monthly_change": group.raw_feature_value.corr(group.raw_value.diff()),
            "correlation_to_raw_12_month_change": group.raw_feature_value.corr(group.raw_value.pct_change(12, fill_method=None)),
            "direction_agreement": (np.sign(group.raw_feature_value) == np.sign(group.raw_value.diff())).mean(),
            "feature_turning_points": canonical._series_stats(group.raw_feature_value, group.native_feature_date)["turning_point_count"],
            "raw_turning_points": canonical._series_stats(group.raw_value, group.native_date)["turning_point_count"],
            "matched_turning_points": pd.NA, "median_turning_point_lag_months": np.nan})
    return joined.sort_values(["metric", "geo_id", "native_feature_date", "feature_type"]), pd.DataFrame(relationships)


def _policy_audit() -> pd.DataFrame:
    evidence = "authoritative empirical review required; fixtures cannot establish a finding"
    return pd.DataFrame([
        {"audit_question": "problem_MW_TEMPERED_C_was_designed_to_fix", "governed_record": "reduce contribution concentration, movement, volatility, turning points, and Fed Funds dominance", "empirical_status": evidence},
        {"audit_question": "current_anatomy_still_reflects_historical_concerns", "governed_record": "evaluate incumbent anatomy only", "empirical_status": evidence},
        {"audit_question": "policy_durability", "governed_record": "closed unless substantive failure evidence exists", "empirical_status": evidence},
        {"audit_question": "structurally_miscalibrated_metric_or_feature_family", "governed_record": "none presumed", "empirical_status": evidence},
        {"audit_question": "detector_behavior_usable", "governed_record": "shared detector; no tuning", "empirical_status": evidence},
        {"audit_question": "demand_and_supply_propagation_proportionate", "governed_record": "review separately at 0.10 and 0.15", "empirical_status": evidence},
        {"audit_question": "evidence_strong_enough_to_reopen_calibration", "governed_record": "no challenger and no automated recommendation", "empirical_status": evidence},
    ])


def build(artifacts: dict[str, pd.DataFrame], root: Path) -> dict[str, pd.DataFrame]:
    contract, _ = resolve_contract(root)
    tables = canonical.build(artifacts, root, DIMENSION)
    tables["production_contract"] = contract
    tables["raw_chronology"] = _native_raw(artifacts, contract)
    tables["feature_anatomy"], tables["raw_feature_relationship"] = _native_feature_anatomy(
        artifacts, contract, tables["raw_chronology"])
    contributions = _dimension_contributions(tables["_aligned_metrics"], tables["_dimension"])
    tables["dimension_contributions"] = contributions
    tables["dimension_statistics"] = tables.pop("capital_markets_dimension_statistics")
    axis = _axis_propagation(artifacts, root)
    tables["axis_propagation"] = axis
    tables["cross_axis_comparison"] = _cross_axis(axis)
    tables["turning_point_health"] = _turning_health(
        tables["feature_contributions"][["geo_id", "date", "metric", "production_metric_score"]].drop_duplicates())
    tables["historical_policy_audit"] = _policy_audit()
    health = "pass" if tables["turning_point_health"].health.eq("pass").all() else "indeterminate"
    tables["evaluation_matrix"] = pd.DataFrame([
        {"question": question, "status": "human_review_required", "evidence": "authoritative run exports",
         "bounded_candidate_region": "none; Phase 1 does not open a candidate grid"}
        for question in ("incumbent_feature_anatomy", "metric_and_dimension_anatomy", "shared_axis_propagation", "county_and_recent_period_robustness", "historical_policy_revalidation")
    ])
    tables["governance_status"] = pd.DataFrame([{
        "recommendation_state": "none", "promotion_state": "current_production_unchanged",
        "human_decision": "capital_markets_phase1_review_pending", "automated_winner": False,
        "production_policy_changed": False, "demand_changed": False, "supply_changed": False,
        "capital_markets_changed": False, "turning_point_detector_health": health,
    }])
    tables["_metadata"] = {"dimension": DIMENSION, "target_metrics": tuple(EXPECTED_WEIGHTS),
                           "dimension_score": "capital_markets_dimension_score"}
    return tables


def write_review(tables: dict[str, pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    prefix = "capital_markets_phase1"
    for name in OUTPUTS:
        tables[name].to_csv(out / f"{prefix}_{name}.csv", index=False)
    plots = []
    # Reuse the canonical renderer for required metric and dimension anatomy.
    canonical.write_review({**tables, "capital_markets_dimension_statistics": tables["dimension_statistics"]}, out, DIMENSION)
    for metric in EXPECTED_WEIGHTS:
        scores = tables["feature_contributions"].query("metric == @metric")[["geo_id", "date", "production_metric_score"]].drop_duplicates()
        for scope in ("dc", "seven_county_standardized"):
            q = scores[scores.geo_id.eq(DC)].rename(columns={"production_metric_score": "value"}) if scope == "dc" else canonical._pool(scores, "production_metric_score", ["geo_id"]).rename(columns={"production_metric_score": "value"})
            filename = f"{prefix}_{metric}_{scope}_metric_chronology.svg"
            canonical._plot(out / filename, [("Metric score", q)], f"{metric} — {scope} — metric chronology", (-1, 1)); plots.append(filename)
    contributions = tables["dimension_contributions"]
    for scope in ("dc", "seven_county_standardized"):
        panels = []
        for metric in EXPECTED_WEIGHTS:
            q = contributions.query("metric == @metric")[["geo_id", "evaluation_date", "weighted_metric_contribution"]].rename(columns={"evaluation_date": "date"})
            q = q[q.geo_id.eq(DC)].rename(columns={"weighted_metric_contribution": "value"}) if scope == "dc" else canonical._pool(q, "weighted_metric_contribution", ["geo_id"]).rename(columns={"weighted_metric_contribution": "value"})
            panels.append((metric, q))
        filename = f"{prefix}_dimension_contribution_decomposition_{scope}.svg"
        canonical._plot(out / filename, panels, f"Capital Markets contribution decomposition — {scope}", (-1, 1)); plots.append(filename)
    axis = tables["axis_propagation"]
    for name, group in axis.groupby("axis"):
        for scope in ("dc", "seven_county_standardized"):
            panels = []
            for label, column in (("Capital Markets contribution", "capital_markets_weighted_contribution"),
                                  ("Other dimensions", "other_dimension_contribution"), ("Final axis", "axis_score")):
                q = group[["geo_id", "date", column]].dropna()
                q = q[q.geo_id.eq(DC)].rename(columns={column: "value"}) if scope == "dc" else canonical._pool(q, column, ["geo_id"]).rename(columns={column: "value"})
                panels.append((label, q))
            filename = f"{prefix}_{name}_axis_propagation_{scope}.svg"
            canonical._plot(out / filename, panels, f"{name.title()} axis propagation — {scope}", (-1, 1)); plots.append(filename)
    cross = tables["cross_axis_comparison"]
    panels = []
    for axis_name in ("demand", "supply"):
        col = f"{axis_name}_capital_markets_weighted_contribution"
        panels.append((axis_name.title(), canonical._pool(cross[["geo_id", "date", col]], col, ["geo_id"]).rename(columns={col: "value"})))
    filename = f"{prefix}_cross_axis_comparison.svg"
    canonical._plot(out / filename, panels, "Same Capital Markets chronology across axes", (-1, 1)); plots.append(filename)
    index = out / f"{prefix}_review_index.html"
    links = [f'<li><a href="{html.escape(prefix)}_{html.escape(name)}.csv">{html.escape(name)}</a></li>' for name in OUTPUTS]
    links += [f'<li><a href="{html.escape(name)}">{html.escape(name)}</a></li>' for name in plots]
    index.write_text("<!doctype html><meta charset=utf-8><title>Capital Markets Phase 1</title>"
        "<h1>Capital Markets Phase 1 — MW-TEMPERED-C anatomy</h1>"
        "<p>Diagnostic only. Demand and Supply remain separate frozen downstream contexts. No challenger or recommendation.</p><ul>"
        + "".join(links) + "</ul>", encoding="utf-8")
