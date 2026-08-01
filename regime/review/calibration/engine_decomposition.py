"""Typed, immutable decomposition of already-produced regime-engine stages."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._05_dimension_scorer import _build_dimension_weights
from regime._06_axis_engine import _build_axis_weights
from regime._08_geometry_engine import assign_geometry

DECOMPOSITION_CONTRACT_VERSION = "engine_decomposition_v4"
RECONCILIATION_TOLERANCE = 1e-10
DECOMPOSITION_SECTIONS = (
    "feature_to_metric", "metric_to_dimension", "dimension_to_axis",
    "chronology_coverage", "reconciliation_summary",
    "coordinate_reconciliation", "regime_reconciliation",
)
IDENTITY = ["campaign_id", "campaign_version", "series_id", "geo_id"]
RECONCILIATION_COLUMNS = {
    "reconciliation_status", "reconciliation_pass", "reason_code", "reason",
    "parent_score", "summed_contributions", "absolute_residual",
    "relative_residual", "tolerance",
}
SCHEMAS = {
    "feature_to_metric": ({*IDENTITY, "date", "canonical_metric_key", "feature_key", "feature_type",
        "feature_score", "configured_weight", "available", "availability_reason_code",
        "availability_reason", "available_child_count", "available_weight_sum", "effective_weight",
        "weighted_contribution", *RECONCILIATION_COLUMNS},
        [*IDENTITY, "date", "canonical_metric_key", "feature_key"]),
    "metric_to_dimension": ({*IDENTITY, "date", "dimension", "canonical_metric_key", "metric_score",
        "configured_weight", "available", "availability_reason_code", "availability_reason",
        "available_child_count", "available_weight_sum", "effective_weight", "weighted_contribution",
        *RECONCILIATION_COLUMNS}, [*IDENTITY, "date", "dimension", "canonical_metric_key"]),
    "dimension_to_axis": ({*IDENTITY, "date", "axis", "dimension", "dimension_score",
        "configured_weight", "available", "availability_reason_code", "availability_reason",
        "available_child_count", "available_weight_sum", "effective_weight", "weighted_contribution",
        *RECONCILIATION_COLUMNS}, [*IDENTITY, "date", "axis", "dimension"]),
    "chronology_coverage": ({*IDENTITY, "layer", "parent_key", "expected_child_count",
        "evaluation_universe_start", "evaluation_universe_end", "first_source_observation_date",
        "first_valid_child_date", "first_valid_parent_date",
        "first_fully_reconciled_date", "source_present_child_unavailable_rows",
        "child_available_parent_absent_rows", "partially_available_parent_date_count",
        "one_child_only_parent_date_count", "zero_available_child_dates",
        "zero_available_weight_parent_rows", "warmup_rows",
        "fully_reconciled_row_count"}, [*IDENTITY, "layer", "parent_key"]),
    "reconciliation_summary": ({*IDENTITY, "date", "layer", "parent_key", *RECONCILIATION_COLUMNS},
        [*IDENTITY, "date", "layer", "parent_key"]),
    "coordinate_reconciliation": ({*IDENTITY, "date", "supply_axis_score", "x_supply",
        "demand_axis_score", "y_demand", "supply_residual", "demand_residual",
        *RECONCILIATION_COLUMNS}, [*IDENTITY, "date"]),
    "regime_reconciliation": ({*IDENTITY, "date", "major_regime_expected", "major_regime_actual",
        "minor_regime_expected", "minor_regime_actual", "quadrant_expected", "quadrant_actual",
        *RECONCILIATION_COLUMNS}, [*IDENTITY, "date"]),
}

@dataclass(frozen=True, slots=True)
class EngineDecompositionEvidence:
    campaign_id: str
    campaign_version: str
    candidate_policy_ids: tuple[str, ...]
    tables: Mapping[str, pd.DataFrame]
    def copied_tables(self) -> dict[str, pd.DataFrame]:
        return {key: value.copy(deep=True) for key, value in self.tables.items()}


def _dates(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if out["date"].isna().any(): raise ValueError(f"{name} contains invalid dates")
    return out


def _unique_parent(frame: pd.DataFrame, keys: list[str], score: str, name: str) -> pd.DataFrame:
    required = {*keys, score}
    if required.difference(frame): raise ValueError(f"{name} missing columns: {sorted(required.difference(frame))}")
    work = frame[keys + [score]].copy()
    duplicates = work[work.duplicated(keys, keep=False)]
    if not duplicates.empty:
        conflicts = duplicates.groupby(keys, dropna=False)[score].nunique(dropna=False)
        if (conflicts > 1).any(): raise ValueError(f"{name} contains conflicting parent rows")
        work = work.drop_duplicates(keys)
    values = pd.to_numeric(work[score], errors="coerce")
    if values.isna().any() or not np.isfinite(values).all(): raise ValueError(f"{name} contains non-finite {score}")
    if values.abs().gt(1.0 + RECONCILIATION_TOLERANCE).any(): raise ValueError(f"{name} contains out-of-range {score}")
    work[score] = values
    return work


def _finish(parent: pd.DataFrame, children: pd.DataFrame, *, keys: list[str], score: str,
            layer: str, parent_key_column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    children = children.copy()
    denominator = children["configured_weight"].where(children["available"], 0.0).groupby(
        [children[key] for key in keys]).transform("sum")
    children["effective_weight"] = np.where(
        children["available"] & denominator.gt(0), children["configured_weight"] / denominator, np.nan)
    children["weighted_contribution"] = np.where(
        children["available"], children["child_score"] * children["effective_weight"], np.nan)
    available = children[children["available"]]
    sums = available.groupby(keys, as_index=False)["weighted_contribution"].sum().rename(
        columns={"weighted_contribution": "summed_contributions"})
    counts = available.groupby(keys, as_index=False).agg(
        available_child_count=("available", "size"), available_weight_sum=("configured_weight", "sum"))
    rec = parent.merge(sums, on=keys, how="left", validate="one_to_one").merge(counts, on=keys, how="left", validate="one_to_one")
    rec = rec.rename(columns={score: "parent_score"})
    rec["available_child_count"] = rec["available_child_count"].fillna(0).astype(int)
    rec["available_weight_sum"] = rec["available_weight_sum"].fillna(0.0)
    reconcilable = rec["available_weight_sum"] > 0
    rec["summed_contributions"] = rec["summed_contributions"].where(reconcilable)
    rec["absolute_residual"] = (rec["parent_score"] - rec["summed_contributions"]).abs()
    rec["relative_residual"] = rec["absolute_residual"] / rec["parent_score"].abs().replace(0, np.nan)
    rec["tolerance"] = RECONCILIATION_TOLERANCE
    passed = reconcilable & rec["absolute_residual"].le(RECONCILIATION_TOLERANCE)
    rec["reconciliation_status"] = np.select([~reconcilable, passed], ["not_reconcilable", "reconciled"], default="failed")
    rec["reconciliation_pass"] = pd.array(np.where(reconcilable, passed, pd.NA), dtype="boolean")
    rec["reason_code"] = np.select([~reconcilable, ~passed], ["no_available_weight", "residual_exceeds_tolerance"], default="none")
    rec["reason"] = np.select([~reconcilable, ~passed], ["No production-eligible child weight is available.", "Contribution sum differs from the persisted parent score."], default="Reconciled to the persisted parent score.")
    rec["layer"] = layer; rec["parent_key"] = rec[parent_key_column].astype(str)
    attach = keys + ["parent_score", "summed_contributions", "absolute_residual", "relative_residual", "tolerance",
                     "reconciliation_status", "reconciliation_pass", "reason_code", "reason",
                     "available_child_count", "available_weight_sum"]
    out = children.merge(rec[attach], on=keys, how="left", validate="many_to_one")
    return out, rec


def _feature_registry() -> pd.DataFrame:
    config = load_regime_config(validate=True)
    features = config.features[["feature_key", "metric_key", "feature_type", "feature_weight"]].copy()
    mapping = config.metric_dimensions[["metric_key", "canonical_metric_key"]].drop_duplicates()
    features = features.merge(mapping, on="metric_key", how="left", validate="many_to_one")
    features["configured_weight"] = pd.to_numeric(features.pop("feature_weight"), errors="coerce")
    if features[["canonical_metric_key", "configured_weight"]].isna().any().any():
        raise ValueError("Feature registry ownership/weight mapping is incomplete")
    return features[["canonical_metric_key", "feature_key", "feature_type", "configured_weight"]].drop_duplicates()


def build_feature_to_metric(normalized: pd.DataFrame, metrics: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    observed = _dates(normalized, "normalized_features")
    child_keys = ["geo_id", "date", "canonical_metric_key", "feature_key"]
    if set(child_keys + ["feature_score"]).difference(observed): raise ValueError("normalized_features missing required columns")
    if observed.duplicated(child_keys).any(): raise ValueError("normalized_features contains duplicate child rows")
    registry = _feature_registry()
    ownership = registry[["feature_key", "canonical_metric_key"]].drop_duplicates()
    checked = observed.merge(ownership, on="feature_key", how="left", suffixes=("", "_registry"), validate="many_to_one")
    if checked["canonical_metric_key_registry"].isna().any() or not checked["canonical_metric_key"].eq(checked["canonical_metric_key_registry"]).all():
        raise ValueError("normalized feature metric ownership mismatch")
    parent_keys = ["geo_id", "date", "canonical_metric_key"]
    parent = _unique_parent(_dates(metrics, "metric_scores"), parent_keys, "metric_score", "metric_scores")
    expected = parent[parent_keys].merge(registry, on="canonical_metric_key", how="left", validate="many_to_many")
    if expected["feature_key"].isna().any(): raise ValueError("Metric has no configured features")
    expected = expected.merge(observed[child_keys + ["feature_score"]], on=child_keys, how="left", validate="one_to_one")
    scores = pd.to_numeric(expected["feature_score"], errors="coerce")
    invalid = expected["feature_score"].notna() & ((~np.isfinite(scores)) | scores.abs().gt(1.0 + RECONCILIATION_TOLERANCE))
    if invalid.any(): raise ValueError("normalized_features contains non-finite feature_score")
    expected["feature_score"] = scores; expected["child_score"] = scores
    expected["available"] = scores.notna()
    expected["availability_reason_code"] = np.where(expected["available"], "available", "feature_score_missing")
    expected["availability_reason"] = np.where(expected["available"], "Normalized feature score is available.", "Normalized feature score is unavailable because production feature warmup/prerequisites are incomplete.")
    out, rec = _finish(parent, expected, keys=parent_keys, score="metric_score", layer="feature_to_metric", parent_key_column="canonical_metric_key")
    return out.drop(columns="child_score").sort_values(child_keys, kind="mergesort").reset_index(drop=True), rec


def build_metric_to_dimension(aligned: pd.DataFrame, dimensions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    observed = aligned.copy(); date_col = "evaluation_date" if "evaluation_date" in observed else "date"; observed["date"] = observed[date_col]
    observed = _dates(observed, "aligned_metric_scores")
    child_keys = ["geo_id", "date", "dimension", "canonical_metric_key"]
    weights = _build_dimension_weights().rename(columns={"metric_weight": "configured_weight"})
    parent_keys = ["geo_id", "date", "dimension"]
    parent = _unique_parent(_dates(dimensions, "dimension_scores"), parent_keys, "dimension_score", "dimension_scores")
    expected = parent[parent_keys].merge(weights, on="dimension", how="left", validate="many_to_many")
    observed_keys = ["geo_id", "date", "canonical_metric_key"]
    if observed.duplicated(observed_keys).any(): raise ValueError("aligned_metric_scores contains duplicate child rows")
    expected = expected.merge(observed[observed_keys + ["metric_score"]], on=observed_keys, how="left", validate="many_to_one")
    scores = pd.to_numeric(expected["metric_score"], errors="coerce"); invalid = expected["metric_score"].notna() & ((~np.isfinite(scores)) | scores.abs().gt(1.0 + RECONCILIATION_TOLERANCE))
    if invalid.any(): raise ValueError("aligned_metric_scores contains non-finite metric_score")
    expected["metric_score"] = scores; expected["child_score"] = scores; expected["available"] = scores.notna()
    expected["availability_reason_code"] = np.where(expected["available"], "available", "metric_score_missing")
    expected["availability_reason"] = np.where(expected["available"], "Aligned metric score is production-eligible.", "Configured metric has no eligible aligned score on this evaluation date.")
    out, rec = _finish(parent, expected, keys=parent_keys, score="dimension_score", layer="metric_to_dimension", parent_key_column="dimension")
    return out.drop(columns="child_score").sort_values(child_keys, kind="mergesort").reset_index(drop=True), rec


def build_dimension_to_axis(dimensions: pd.DataFrame, axes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    observed = _dates(dimensions, "dimension_scores")
    weights = _build_axis_weights().rename(columns={"dimension_weight": "configured_weight"})
    parent_keys = ["geo_id", "date", "axis"]
    parent = _unique_parent(_dates(axes, "axis_scores"), parent_keys, "axis_score", "axis_scores")
    expected = parent[parent_keys].merge(weights, on="axis", how="left", validate="many_to_many")
    observed_keys = ["geo_id", "date", "dimension"]
    if observed.duplicated(observed_keys).any(): raise ValueError("dimension_scores contains duplicate child rows")
    expected = expected.merge(observed[observed_keys + ["dimension_score"]], on=observed_keys, how="left", validate="many_to_one")
    scores = pd.to_numeric(expected["dimension_score"], errors="coerce"); invalid = expected["dimension_score"].notna() & ((~np.isfinite(scores)) | scores.abs().gt(1.0 + RECONCILIATION_TOLERANCE))
    if invalid.any(): raise ValueError("dimension_scores contains non-finite dimension_score")
    expected["dimension_score"] = scores; expected["child_score"] = scores; expected["available"] = scores.notna()
    expected["availability_reason_code"] = np.where(expected["available"], "available", "dimension_score_missing")
    expected["availability_reason"] = np.where(expected["available"], "Dimension score is production-eligible.", "Configured dimension has no eligible score on this axis date.")
    out, rec = _finish(parent, expected, keys=parent_keys, score="axis_score", layer="dimension_to_axis", parent_key_column="axis")
    child_keys = ["geo_id", "date", "axis", "dimension"]
    return out.drop(columns="child_score").sort_values(child_keys, kind="mergesort").reset_index(drop=True), rec

def _add_identity(frame: pd.DataFrame, campaign, series_id: str) -> pd.DataFrame:
    out = frame.copy()
    out.insert(0, "series_id", series_id); out.insert(0, "campaign_version", campaign.campaign_version); out.insert(0, "campaign_id", campaign.campaign_id)
    return out


def _coverage_universe(*, layer: str, parent_key: str, geo_id: str,
                       expected_children: tuple[str, ...], source: pd.DataFrame,
                       source_child: str, available: pd.DataFrame, available_child: str,
                       parents: pd.DataFrame, reconciliation: pd.DataFrame) -> dict[str, object]:
    source = _dates(source, f"{layer} coverage source") if not source.empty else source.assign(date=pd.Series(dtype="datetime64[ns]"))
    available = _dates(available, f"{layer} coverage available") if not available.empty else available.assign(date=pd.Series(dtype="datetime64[ns]"))
    parents = _dates(parents, f"{layer} coverage parents") if not parents.empty else parents.assign(date=pd.Series(dtype="datetime64[ns]"))
    dates = sorted(set(source.get("date", [])) | set(available.get("date", [])) | set(parents.get("date", [])))
    if not dates: raise ValueError(f"No governed coverage universe for {layer}/{geo_id}/{parent_key}")
    grid = pd.MultiIndex.from_product([dates, expected_children], names=["date", "child"]).to_frame(index=False)
    source_pairs = set(zip(source.get("date", []), source.get(source_child, [])))
    available_pairs = set(zip(available.get("date", []), available.get(available_child, [])))
    parent_dates = set(parents.get("date", []))
    grid["source_present"] = [(d, c) in source_pairs for d, c in zip(grid.date, grid.child)]
    grid["child_available"] = [(d, c) in available_pairs for d, c in zip(grid.date, grid.child)]
    grid["parent_present"] = grid.date.isin(parent_dates)
    by_date = grid.groupby("date").agg(available_count=("child_available", "sum"), parent_present=("parent_present", "first"))
    fully = reconciliation[reconciliation["reconciliation_status"].eq("reconciled")]
    return {"geo_id": geo_id, "layer": layer, "parent_key": str(parent_key),
        "expected_child_count": len(expected_children), "evaluation_universe_start": min(dates),
        "evaluation_universe_end": max(dates), "first_source_observation_date": source["date"].min() if not source.empty else pd.NaT,
        "first_valid_child_date": available["date"].min() if not available.empty else pd.NaT,
        "first_valid_parent_date": parents["date"].min() if not parents.empty else pd.NaT,
        "first_fully_reconciled_date": fully["date"].min() if not fully.empty else pd.NaT,
        "source_present_child_unavailable_rows": int((grid.source_present & ~grid.child_available).sum()),
        "child_available_parent_absent_rows": int((grid.child_available & ~grid.parent_present).sum()),
        "partially_available_parent_date_count": int((by_date.parent_present & by_date.available_count.between(1, len(expected_children)-1)).sum()) if len(expected_children)>1 else 0,
        "one_child_only_parent_date_count": int((by_date.parent_present & by_date.available_count.eq(1)).sum()),
        "zero_available_child_dates": int(by_date.available_count.eq(0).sum()),
        "zero_available_weight_parent_rows": int((by_date.parent_present & by_date.available_count.eq(0)).sum()),
        "warmup_rows": int((grid.source_present & ~grid.child_available).sum()) if layer == "feature_to_metric" else 0,
        "fully_reconciled_row_count": len(fully)}


def _coverage_tables(artifacts: Mapping[str, pd.DataFrame], f2m: pd.DataFrame, r1: pd.DataFrame,
                     m2d: pd.DataFrame, r2: pd.DataFrame, d2a: pd.DataFrame, r3: pd.DataFrame) -> pd.DataFrame:
    rows = []
    features = _dates(artifacts.get("features", artifacts["normalized_features"]), "features")
    normalized = _dates(artifacts["normalized_features"], "normalized_features")
    metrics = _dates(artifacts["metric_scores"], "metric_scores")
    aligned = artifacts["aligned_metric_scores"].copy(); aligned["date"] = aligned["evaluation_date"] if "evaluation_date" in aligned else aligned["date"]; aligned = _dates(aligned, "aligned_metric_scores")
    dimensions = _dates(artifacts["dimension_scores"], "dimension_scores")
    axes = _dates(artifacts["axis_scores"], "axis_scores")
    feature_registry = _feature_registry(); metric_registry = _build_dimension_weights(); axis_registry = _build_axis_weights()
    geos = sorted(set(features.geo_id) | set(normalized.geo_id) | set(metrics.geo_id) | set(dimensions.geo_id) | set(axes.geo_id))
    for geo in geos:
        metric_keys = sorted(set(features.loc[features.geo_id.eq(geo), "canonical_metric_key"]) | set(normalized.loc[normalized.geo_id.eq(geo), "canonical_metric_key"]) | set(metrics.loc[metrics.geo_id.eq(geo), "canonical_metric_key"]))
        for parent_key in metric_keys:
            children = tuple(feature_registry.loc[feature_registry.canonical_metric_key.eq(parent_key), "feature_key"])
            if not children: continue
            rows.append(_coverage_universe(layer="feature_to_metric", parent_key=parent_key, geo_id=geo, expected_children=children,
                source=features.query("geo_id == @geo and canonical_metric_key == @parent_key"), source_child="feature_key",
                available=normalized.query("geo_id == @geo and canonical_metric_key == @parent_key and feature_score == feature_score"), available_child="feature_key",
                parents=metrics.query("geo_id == @geo and canonical_metric_key == @parent_key"),
                reconciliation=r1.query("geo_id == @geo and canonical_metric_key == @parent_key")))
        for parent_key in sorted(metric_registry.dimension.unique()):
            children = tuple(metric_registry.loc[metric_registry.dimension.eq(parent_key), "canonical_metric_key"])
            metric_source = metrics.query("geo_id == @geo and canonical_metric_key in @children")
            relevant = aligned.query("geo_id == @geo and canonical_metric_key in @children")
            parent = dimensions.query("geo_id == @geo and dimension == @parent_key")
            if metric_source.empty and relevant.empty and parent.empty: continue
            rows.append(_coverage_universe(layer="metric_to_dimension", parent_key=parent_key, geo_id=geo, expected_children=children,
                source=metric_source, source_child="canonical_metric_key", available=relevant.query("metric_score == metric_score"), available_child="canonical_metric_key",
                parents=parent, reconciliation=r2.query("geo_id == @geo and dimension == @parent_key")))
        for parent_key in sorted(axis_registry.axis.unique()):
            children = tuple(axis_registry.loc[axis_registry.axis.eq(parent_key), "dimension"])
            relevant = dimensions.query("geo_id == @geo and dimension in @children")
            parent = axes.query("geo_id == @geo and axis == @parent_key")
            if relevant.empty and parent.empty: continue
            rows.append(_coverage_universe(layer="dimension_to_axis", parent_key=parent_key, geo_id=geo, expected_children=children,
                source=relevant, source_child="dimension", available=relevant.query("dimension_score == dimension_score"), available_child="dimension",
                parents=parent, reconciliation=r3.query("geo_id == @geo and axis == @parent_key")))
    return pd.DataFrame(rows)

def _coordinate_reconciliation(axes: pd.DataFrame, coordinates: pd.DataFrame) -> pd.DataFrame:
    axis = _dates(axes, "axis_scores")
    if axis.duplicated(["geo_id", "date", "axis"]).any(): raise ValueError("axis_scores contains duplicate rows")
    pivot = axis.pivot(index=["geo_id", "date"], columns="axis", values="axis_score").reset_index()
    expected = pivot.dropna(subset=["supply", "demand"])[["geo_id", "date"]]
    coords = _dates(coordinates, "coordinates")
    _unique_parent(coords, ["geo_id", "date"], "x_supply", "coordinates")
    _unique_parent(coords, ["geo_id", "date"], "y_demand", "coordinates")
    expected_keys = set(map(tuple, expected.itertuples(index=False, name=None)))
    actual_keys = set(map(tuple, coords[["geo_id", "date"]].itertuples(index=False, name=None)))
    if expected_keys != actual_keys:
        raise ValueError(f"coordinate key-universe mismatch: missing={len(expected_keys-actual_keys)}, extra={len(actual_keys-expected_keys)}")
    out = coords[["geo_id", "date", "x_supply", "y_demand"]].merge(pivot, on=["geo_id", "date"], validate="one_to_one")
    out = out.rename(columns={"supply": "supply_axis_score", "demand": "demand_axis_score"})
    out["supply_residual"] = (out["x_supply"] - out["supply_axis_score"]).abs(); out["demand_residual"] = (out["y_demand"] - out["demand_axis_score"]).abs()
    out["parent_score"] = out["x_supply"]; out["summed_contributions"] = out["supply_axis_score"]
    out["absolute_residual"] = out[["supply_residual", "demand_residual"]].max(axis=1)
    out["relative_residual"] = out["absolute_residual"] / out[["x_supply", "y_demand"]].abs().max(axis=1).replace(0, np.nan)
    out["tolerance"] = RECONCILIATION_TOLERANCE
    passed = out["absolute_residual"].le(RECONCILIATION_TOLERANCE)
    out["reconciliation_status"] = np.where(passed, "reconciled", "failed"); out["reconciliation_pass"] = passed
    out["reason_code"] = np.where(passed, "none", "axis_coordinate_mismatch")
    out["reason"] = np.where(passed, "Supply and Demand axes equal their coordinate fields.", "Axis score differs from its coordinate field.")
    return out


def _regime_reconciliation(coordinates: pd.DataFrame, regimes: pd.DataFrame) -> pd.DataFrame:
    coords = _dates(coordinates, "coordinates"); expected = assign_geometry(coords)
    actual = _dates(regimes, "regime_assignments")
    cols = ["major_regime", "minor_regime", "quadrant"]
    if actual.duplicated(["geo_id", "date"]).any(): raise ValueError("regime_assignments contains duplicate rows")
    coordinate_keys = set(map(tuple, coords[["geo_id", "date"]].itertuples(index=False, name=None)))
    regime_keys = set(map(tuple, actual[["geo_id", "date"]].itertuples(index=False, name=None)))
    if coordinate_keys != regime_keys:
        raise ValueError(f"regime key-universe mismatch: missing={len(coordinate_keys-regime_keys)}, extra={len(regime_keys-coordinate_keys)}")
    out = expected[["geo_id", "date", *cols]].merge(actual[["geo_id", "date", *cols]], on=["geo_id", "date"], suffixes=("_expected", "_actual"), validate="one_to_one")
    passed = np.logical_and.reduce([out[f"{c}_expected"].astype(str).eq(out[f"{c}_actual"].astype(str)) for c in cols])
    out["parent_score"] = np.nan; out["summed_contributions"] = np.nan; out["absolute_residual"] = np.nan; out["relative_residual"] = np.nan; out["tolerance"] = RECONCILIATION_TOLERANCE
    out["reconciliation_status"] = np.where(passed, "reconciled", "failed"); out["reconciliation_pass"] = passed
    out["reason_code"] = np.where(passed, "none", "regime_label_mismatch")
    out["reason"] = np.where(passed, "Persisted labels match the production geometry classifier.", "Persisted major/minor/quadrant labels differ from production classification.")
    return out


def build_engine_decomposition(*, campaign, series_artifacts: Mapping[str, Mapping[str, pd.DataFrame]], selected_geographies: list[str]) -> EngineDecompositionEvidence:
    expected_series = ("baseline", *campaign.candidate_policy_ids)
    if tuple(series_artifacts) != expected_series: raise ValueError("Decomposition series identity/order mismatch")
    collected = {name: [] for name in DECOMPOSITION_SECTIONS}
    for series_id, artifacts in series_artifacts.items():
        required = {"normalized_features", "metric_scores", "aligned_metric_scores", "dimension_scores", "axis_scores", "coordinates", "regime_assignments"}
        if required.difference(artifacts): raise ValueError(f"{series_id} decomposition artifacts missing: {sorted(required.difference(artifacts))}")
        f2m, r1 = build_feature_to_metric(artifacts["normalized_features"], artifacts["metric_scores"])
        m2d, r2 = build_metric_to_dimension(artifacts["aligned_metric_scores"], artifacts["dimension_scores"])
        d2a, r3 = build_dimension_to_axis(artifacts["dimension_scores"], artifacts["axis_scores"])
        cr = _coordinate_reconciliation(artifacts["axis_scores"], artifacts["coordinates"])
        rr = _regime_reconciliation(artifacts["coordinates"], artifacts["regime_assignments"])
        for name, frame in (("feature_to_metric", f2m), ("metric_to_dimension", m2d), ("dimension_to_axis", d2a), ("coordinate_reconciliation", cr), ("regime_reconciliation", rr)):
            collected[name].append(_add_identity(frame[frame["geo_id"].isin(selected_geographies)], campaign, series_id))
        summaries = []
        for rec, parent_col in ((r1, "canonical_metric_key"), (r2, "dimension"), (r3, "axis")):
            summary = rec.copy(); summary["parent_key"] = summary[parent_col].astype(str)
            summaries.append(summary[["geo_id", "date", "layer", "parent_key", *sorted(RECONCILIATION_COLUMNS)]])
        collected["reconciliation_summary"].append(_add_identity(pd.concat(summaries).query("geo_id in @selected_geographies"), campaign, series_id))
        coverage = _coverage_tables(artifacts, f2m, r1, m2d, r2, d2a, r3)
        collected["chronology_coverage"].append(_add_identity(coverage[coverage["geo_id"].isin(selected_geographies)], campaign, series_id))
    tables = {name: pd.concat(parts, ignore_index=True).sort_values(SCHEMAS[name][1], kind="mergesort").reset_index(drop=True) for name, parts in collected.items()}
    evidence = EngineDecompositionEvidence(campaign.campaign_id, campaign.campaign_version, campaign.candidate_policy_ids, tables)
    validate_engine_decomposition(evidence)
    return evidence


def validate_engine_decomposition(evidence: EngineDecompositionEvidence) -> None:
    unknown = set(evidence.tables).difference(DECOMPOSITION_SECTIONS); missing = set(DECOMPOSITION_SECTIONS).difference(evidence.tables)
    if unknown: raise ValueError(f"Unknown decomposition artifacts: {sorted(unknown)}")
    if missing: raise ValueError(f"Missing decomposition evidence: {sorted(missing)}")
    series = {"baseline", *evidence.candidate_policy_ids}
    valid_status = {"reconciled", "not_applicable", "not_reconcilable", "failed"}
    approved_reasons = {
        "reconciled": {"none"},
        "not_reconcilable": {"no_available_weight"},
        "not_applicable": {"categorical_no_numeric_residual"},
        "failed": {"residual_exceeds_tolerance", "axis_coordinate_mismatch", "regime_label_mismatch"},
    }
    for name in DECOMPOSITION_SECTIONS:
        frame = evidence.tables[name]; required, keys = SCHEMAS[name]
        absent = required.difference(frame)
        if absent: raise ValueError(f"{name} missing required columns: {sorted(absent)}")
        if frame.empty: raise ValueError(f"Required decomposition evidence is empty: {name}")
        if frame.duplicated(keys).any(): raise ValueError(f"{name} contains duplicate keys")
        for date_column in required.intersection({"date", "first_source_observation_date", "first_valid_child_date",
                                                  "first_valid_parent_date", "first_fully_reconciled_date"}):
            values = frame[date_column]
            parsed = pd.to_datetime(values, errors="coerce")
            if values.notna().any() and parsed[values.notna()].isna().any(): raise ValueError(f"{name} contains invalid {date_column}")
        if set(frame["campaign_id"].astype(str)) != {evidence.campaign_id} or set(frame["campaign_version"].astype(str)) != {evidence.campaign_version}: raise ValueError(f"{name} campaign identity mismatch")
        if set(frame["series_id"].astype(str)) != series: raise ValueError(f"{name} candidate identity mismatch")
        if name in {"feature_to_metric", "metric_to_dimension", "dimension_to_axis"}:
            parent_keys = SCHEMAS[name][1][:-1]
            parent_fields = ["parent_score", "summed_contributions", "absolute_residual", "relative_residual",
                             "tolerance", "reconciliation_status", "reconciliation_pass", "reason_code",
                             "reason", "available_child_count", "available_weight_sum"]
            for _, part in frame.groupby(parent_keys, dropna=False):
                for field in parent_fields:
                    governed = part[field].astype(object).where(part[field].notna(), "__NULL__")
                    if governed.nunique(dropna=False) != 1:
                        raise ValueError(f"{name} contains inconsistent parent-level {field}")
        if "reconciliation_status" in frame and not set(frame["reconciliation_status"]).issubset(valid_status): raise ValueError(f"{name} has invalid reconciliation status")
        if "reconciliation_status" in frame:
            for status, reasons in frame.groupby("reconciliation_status")["reason_code"]:
                if not set(reasons).issubset(approved_reasons[status]): raise ValueError(f"{name} has unapproved reconciliation reason")
            if frame["reconciliation_status"].eq("failed").any(): raise ValueError(f"{name} contains failed reconciliation")
        for column in required.intersection({"configured_weight", "available_weight_sum", "tolerance"}):
            values = pd.to_numeric(frame[column], errors="coerce")
            if values.isna().any() or not np.isfinite(values).all() or (values < 0).any(): raise ValueError(f"{name} contains invalid {column}")
        if "effective_weight" in frame:
            values = pd.to_numeric(frame.loc[frame["available"], "effective_weight"], errors="coerce")
            if values.isna().any() or not np.isfinite(values).all() or (values < 0).any() or (values > 1 + RECONCILIATION_TOLERANCE).any(): raise ValueError(f"{name} contains invalid effective_weight")
        for score_column in required.intersection({"feature_score", "metric_score", "dimension_score", "parent_score"}):
            values = pd.to_numeric(frame.loc[frame[score_column].notna(), score_column], errors="coerce")
            if not np.isfinite(values).all() or values.abs().gt(1 + RECONCILIATION_TOLERANCE).any(): raise ValueError(f"{name} contains out-of-range {score_column}")
        if "reconciliation_status" in frame:
            reconciled = frame["reconciliation_status"].eq("reconciled")
            if not frame.loc[reconciled, "reconciliation_pass"].astype(bool).all(): raise ValueError(f"{name} has inconsistent reconciliation rows")
            if frame.loc[reconciled, "absolute_residual"].gt(frame.loc[reconciled, "tolerance"]).any(): raise ValueError(f"{name} reconciled row exceeds tolerance")
            non_reconcilable = frame["reconciliation_status"].eq("not_reconcilable")
            if "available_weight_sum" in frame and frame.loc[non_reconcilable, "available_weight_sum"].gt(0).any(): raise ValueError(f"{name} marks sufficient inputs non-reconcilable")
            failed = frame["reconciliation_status"].eq("failed")
            if frame.loc[failed, "reconciliation_pass"].astype(bool).any(): raise ValueError(f"{name} has inconsistent failed rows")
    feature = evidence.tables["feature_to_metric"]
    ownership = _feature_registry()[["feature_key", "canonical_metric_key", "configured_weight"]]
    owned = feature.merge(ownership, on=["feature_key", "canonical_metric_key"], how="left", indicator=True)
    if owned["_merge"].ne("both").any(): raise ValueError("feature_to_metric contains unexpected feature ownership")
    if not np.allclose(owned["configured_weight_x"], owned["configured_weight_y"]): raise ValueError("feature_to_metric configured weight mismatch")
    metric = evidence.tables["metric_to_dimension"]
    metric_pairs = _build_dimension_weights().rename(columns={"metric_weight": "registry_weight"})
    metric_owned = metric.merge(metric_pairs, on=["dimension", "canonical_metric_key"], how="left", indicator=True)
    if metric_owned["_merge"].ne("both").any():
        raise ValueError("metric_to_dimension contains unexpected metric/dimension identifiers")
    if not np.allclose(metric_owned["configured_weight"], metric_owned["registry_weight"]): raise ValueError("metric_to_dimension configured weight mismatch")
    axis = evidence.tables["dimension_to_axis"]
    axis_pairs = _build_axis_weights().rename(columns={"dimension_weight": "registry_weight"})
    axis_owned = axis.merge(axis_pairs, on=["axis", "dimension"], how="left", indicator=True)
    if axis_owned["_merge"].ne("both").any():
        raise ValueError("dimension_to_axis contains unexpected axis/dimension identifiers")
    if not np.allclose(axis_owned["configured_weight"], axis_owned["registry_weight"]): raise ValueError("dimension_to_axis configured weight mismatch")
    for name in ("feature_to_metric", "metric_to_dimension", "dimension_to_axis"):
        frame = evidence.tables[name]
        parent_keys = SCHEMAS[name][1][:-1]
        parent_fields = [
            "parent_score", "summed_contributions", "absolute_residual", "relative_residual",
            "tolerance", "reconciliation_status", "reconciliation_pass", "reason_code",
            "reason", "available_child_count", "available_weight_sum",
        ]
        for _, part in frame.groupby(parent_keys, dropna=False):
            for field in parent_fields:
                governed = part[field].astype(object).where(part[field].notna(), "__NULL__")
                if governed.nunique(dropna=False) != 1:
                    raise ValueError(f"{name} contains inconsistent parent-level {field}")
            available = part[part["available"]]
            if not available.empty and not np.isclose(available["effective_weight"].sum(), 1.0, atol=RECONCILIATION_TOLERANCE):
                raise ValueError(f"{name} effective weights do not sum to one")
            expected_contribution = available[{"feature_to_metric": "feature_score", "metric_to_dimension": "metric_score", "dimension_to_axis": "dimension_score"}[name]] * available["effective_weight"]
            if not np.allclose(available["weighted_contribution"], expected_contribution, atol=RECONCILIATION_TOLERANCE):
                raise ValueError(f"{name} weighted contribution mismatch")
            summed = available["weighted_contribution"].sum()
            recorded = part["summed_contributions"].drop_duplicates()
            status = str(part["reconciliation_status"].iloc[0])
            valid_sum = (status == "not_reconcilable" and recorded.isna().all()) or (
                len(recorded) == 1 and pd.notna(recorded.iloc[0]) and
                np.isclose(float(recorded.iloc[0]), summed, atol=RECONCILIATION_TOLERANCE))
            if not valid_sum:
                raise ValueError(f"{name} contains inconsistent contribution sum")
