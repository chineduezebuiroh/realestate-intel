from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore
from regime.experiments.core_demand_dimension_diagnostic import (
    BASELINE_RUN_ID,
    build_core_demand_dimension_diagnostic,
)

TARGET_METRICS = ("gdp_annual", "median_household_income", "population")
DEFAULT_OUTPUT_ROOT = Path("artifacts/regime/review_exports/gdp_acs_demand_diagnostic")


def _geo_level(values: pd.Series) -> pd.Series:
    return values.astype(str).str.rsplit("__", n=1).str[-1]


def build_gdp_acs_demand_diagnostic(
    *, run_id: str = BASELINE_RUN_ID, artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT
) -> dict[str, pd.DataFrame | dict[str, object]]:
    """Extend the core-Demand diagnostic with GDP/ACS source and feature evidence."""
    store = RegimeArtifactStore(artifact_root)
    manifest = store.read_manifest(run_id)
    if manifest.get("status") != "complete":
        raise ValueError(f"Run {run_id!r} is not complete")

    source = store.read_dataframe(run_id, "source_metrics")
    source = source[source["canonical_metric_key"].isin(TARGET_METRICS)].copy()
    if source.empty:
        raise ValueError("Persisted run contains no GDP/ACS canonical observations")
    source["date"] = pd.to_datetime(source["date"])
    source["value"] = pd.to_numeric(source["value"], errors="coerce")
    source["geo_level"] = _geo_level(source["geo_id"])
    source = source.sort_values(["canonical_metric_key", "geo_id", "date"])
    source["gap_days"] = source.groupby(["canonical_metric_key", "geo_id"])["date"].diff().dt.days
    as_of = max(pd.to_datetime(source["date"]).max(), pd.Timestamp(manifest["metadata"]["stage_summaries"]["source_metrics"]["date_max"]))

    coverage = source.groupby(["canonical_metric_key", "geo_level"], dropna=False).agg(
        rows=("date", "size"), geographies=("geo_id", "nunique"), first_valid_date=("date", "min"),
        last_valid_date=("date", "max"), missing_values=("value", lambda x: int(x.isna().sum())),
        median_cadence_days=("gap_days", "median"), maximum_gap_days=("gap_days", "max"),
    ).reset_index()
    coverage["age_at_run_asof_days"] = (as_of - coverage["last_valid_date"]).dt.days

    source["yoy_change"] = source.groupby(["canonical_metric_key", "geo_id"])["value"].pct_change()
    structural = source.groupby(["canonical_metric_key", "geo_level"]).agg(
        observations=("value", "count"), level_cv=("value", lambda x: float(x.std() / abs(x.mean())) if x.mean() else np.nan),
        median_yoy_change=("yoy_change", "median"), yoy_change_std=("yoy_change", "std"),
        maximum_absolute_yoy_change=("yoy_change", lambda x: x.abs().max()),
    ).reset_index()

    normalized = store.read_dataframe(run_id, "normalized_features")
    normalized = normalized[normalized["canonical_metric_key"].isin(TARGET_METRICS)].copy()
    normalized["date"] = pd.to_datetime(normalized["date"])
    normalized["geo_level"] = _geo_level(normalized["geo_id"])
    config = load_regime_config(validate=True)
    feature_policy = config.features[config.features["metric_key"].isin({"acs1_population", "acs1_median_household_income", "bea_annual_gdp"})][
        ["feature_key", "feature_type", "transform", "feature_window", "feature_weight"]
    ].copy()
    normalized = normalized.merge(feature_policy, on="feature_key", how="left", validate="many_to_one")
    feature_behavior = normalized.groupby(["canonical_metric_key", "feature_key", "feature_type", "geo_level"], dropna=False).agg(
        rows=("date", "size"), geographies=("geo_id", "nunique"), first_valid_date=("date", "min"), last_valid_date=("date", "max"),
        raw_std=("raw_feature_value", "std"), score_std=("feature_score", "std"),
        near_zero_score_rate=("feature_score", lambda x: float(pd.to_numeric(x).abs().le(.1).mean())),
        lower_clip_rate=("percentile", lambda x: float(pd.to_numeric(x).le(.01).mean())),
        upper_clip_rate=("percentile", lambda x: float(pd.to_numeric(x).ge(.99).mean())),
    ).reset_index().merge(feature_policy, on=["feature_key", "feature_type"], how="left", validate="many_to_one")

    core = build_core_demand_dimension_diagnostic(run_id=run_id, artifact_root=artifact_root)
    contribution = core["contribution_summary"]
    contribution = contribution[contribution["canonical_metric_key"].isin(TARGET_METRICS)].copy()
    contribution["mean_absolute_demand_axis_contribution"] = contribution["mean_absolute_weighted_contribution"] * .65
    correlations = core["pairwise_metric_correlations"]
    interactions = correlations[
        correlations["left_metric"].isin(TARGET_METRICS) | correlations["right_metric"].isin(TARGET_METRICS)
    ].copy()

    metadata = {"run_id": run_id, "run_as_of": as_of.isoformat(), "target_metrics": list(TARGET_METRICS),
                "limitations": ["Persisted canonical source_metrics do not identify ACS1 versus ACS5 physical-source rows.",
                                "Revision-vintage histories are not represented by the current artifact contract.",
                                "Coordinate/regime sensitivity requires an immutable counterfactual run and is not inferred silently."]}
    return {"source_coverage": coverage, "time_series_structure": structural, "feature_policy": feature_policy,
            "feature_behavior": feature_behavior, "contribution_summary": contribution,
            "interaction_correlations": interactions, "metadata": metadata}


def write_gdp_acs_demand_diagnostic(result: dict[str, object], output_root: str | Path = DEFAULT_OUTPUT_ROOT) -> Path:
    output = Path(output_root)
    output.mkdir(parents=True, exist_ok=True)
    for name, value in result.items():
        if isinstance(value, pd.DataFrame):
            value.to_csv(output / f"{name}.csv", index=False)
    (output / "diagnostic_manifest.json").write_text(json.dumps(result["metadata"], indent=2) + "\n", encoding="utf-8")
    return output
