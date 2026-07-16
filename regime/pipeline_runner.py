from __future__ import annotations
# regime/pipeline_runner.py

import hashlib
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore
from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import (
    build_canonical_source_metrics_with_lineage,
    build_feature_matrix_with_lineage,
)
from regime._02_feature_normalizer import normalize_features
from regime._03_metric_scorer import score_metrics
from regime._04_asof_aligner import align_metric_scores_asof
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes
from regime.validation import (
    DEFAULT_VALIDATION_GEOS,
    build_historical_trajectory,
    build_metric_contribution_audit,
    build_seasonality_audit,
    build_transition_audit,
    build_transition_events,
)
from regime.freshness import evaluate_derived_input_freshness
from regime.experiments.smoothing_run import apply_smoothing_experiment


DEFAULT_CONFIG_PATHS = [
    Path("config/source_metric_registry.csv"),
    Path("config/feature_registry.csv"),
    Path("config/metric_dimension_registry.csv"),
    Path("config/axis_registry.csv"),
    Path("config/normalization_registry.csv"),
    Path("config/derived_input_freshness_registry.csv"),
    Path("config/metric_smoothing_experiments.csv"),
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def _config_hashes(
    config_paths: list[Path] | None = None,
) -> dict[str, str]:
    paths = config_paths or DEFAULT_CONFIG_PATHS
    hashes: dict[str, str] = {}

    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(f"Required regime config not found: {path}")

        hashes[str(path)] = _sha256_file(path)

    return hashes


def _ma_transform_policy_snapshot() -> dict[str, Any]:
    config = load_regime_config(validate=True)
    policy_rows = config.features[
        config.features["transform"].str.startswith("ma")
    ][
        [
            "feature_key",
            "metric_key",
            "feature_type",
            "transform",
            "feature_window",
        ]
    ].copy()

    policy_rows = policy_rows.sort_values(
        [
            "metric_key",
            "feature_key",
            "feature_type",
            "transform",
            "feature_window",
        ]
    ).reset_index(drop=True)

    return {
        "transform_schema": {
            "ma_level": "rolling mean over feature_window observations",
            "ma_pct_change": (
                "rolling mean over feature_window MA component divided by "
                "lagged rolling mean from lag component minus one"
            ),
        },
        "active_ma_features": policy_rows.to_dict(orient="records"),
    }

def _frame_summary(df: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
    }

    if "geo_id" in df.columns:
        summary["geos"] = int(df["geo_id"].nunique())

    for date_col in ("date", "evaluation_date", "metric_date"):
        if date_col not in df.columns:
            continue

        values = pd.to_datetime(df[date_col], errors="coerce").dropna()

        summary[f"{date_col}_min"] = (
            values.min().isoformat() if not values.empty else None
        )
        summary[f"{date_col}_max"] = (
            values.max().isoformat() if not values.empty else None
        )

    return summary


def _write_pipeline_artifact(
    store: RegimeArtifactStore,
    run_id: str,
    artifact_name: str,
    dataframe: pd.DataFrame,
) -> None:
    store.write_dataframe(
        run_id,
        artifact_name,
        dataframe,
        validation=False,
        extra_metadata=_frame_summary(dataframe),
    )


def _write_validation_artifact(
    store: RegimeArtifactStore,
    run_id: str,
    artifact_name: str,
    dataframe: pd.DataFrame,
) -> None:
    store.write_dataframe(
        run_id,
        artifact_name,
        dataframe,
        validation=True,
        extra_metadata=_frame_summary(dataframe),
    )


def run_regime_pipeline(
    *,
    run_id: str,
    experiment_id: str,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    validation_geo_ids: list[str] | None = None,
    serving_db_path: str | Path = "data/market_serving.duckdb",
    run_metadata: dict[str, Any] | None = None,
    smoothing_experiment_id: (
        str | None
    ) = None,
) -> dict[str, Any]:
    """
    Run the regime pipeline exactly once, persist each stage, and return
    the completed manifest.

    Existing run IDs are rejected. Runs should be treated as immutable.
    """
    validation_geo_ids = (
        validation_geo_ids
        if validation_geo_ids is not None
        else DEFAULT_VALIDATION_GEOS
    )

    serving_db_path = Path(serving_db_path)
    if not serving_db_path.is_file():
        raise FileNotFoundError(
            f"Serving database not found: {serving_db_path}"
        )

    store = RegimeArtifactStore(artifact_root)

    metadata: dict[str, Any] = {
        "pipeline_version": "C4.3b_v1",
        "started_at_utc": _utc_now_iso(),
        "serving_db_path": str(serving_db_path),
        "validation_geo_ids": validation_geo_ids,
        "config_hashes": _config_hashes(),
        "ma_transform_policy_snapshot": _ma_transform_policy_snapshot(),
        "smoothing_experiment_id": smoothing_experiment_id,
    }

    if run_metadata:
        metadata.update(run_metadata)

    store.initialize_run(
        run_id,
        experiment_id=experiment_id,
        metadata=metadata,
        overwrite=False,
    )

    stage_summaries: dict[str, dict[str, Any]] = {}

    try:
        print("[regime_pipeline] loading config")
        config = load_regime_config(validate=True)

        print("[regime_pipeline] building canonical source metrics")

        (
            source_metrics,
            derived_metric_lineage,
        ) = (
            build_canonical_source_metrics_with_lineage(
                config=config,
                db_path=serving_db_path,
            )
        )

        stage_summaries["source_metrics"] = _frame_summary(source_metrics)
        _write_pipeline_artifact(store, run_id, "source_metrics", source_metrics)

        print("[regime_pipeline] 1/9 building features")

        features, feature_lineage = (
            build_feature_matrix_with_lineage(
                config=config,
                db_path=serving_db_path,
                canonical_observations=(
                    source_metrics
                ),
                derived_metric_lineage=(
                    derived_metric_lineage
                ),
            )
        )

        (
            features,
            smoothing_lineage,
        ) = apply_smoothing_experiment(
            features=features,
            source_metrics=source_metrics,
            experiment_id=(
                smoothing_experiment_id
            ),
        )

        if not feature_lineage.equals(
            derived_metric_lineage
        ):
            raise AssertionError(
                "Feature generation changed the supplied "
                "derived-metric lineage"
            )

        stage_summaries["features"] = _frame_summary(features)
        _write_pipeline_artifact(store, run_id, "features", features)

        stage_summaries["derived_metric_lineage"] = _frame_summary(derived_metric_lineage)
        _write_pipeline_artifact(store, run_id, "derived_metric_lineage", derived_metric_lineage)

        print("[regime_pipeline] evaluating derived input freshness")
        freshness_outputs = evaluate_derived_input_freshness(derived_metric_lineage)

        derived_input_component_freshness = (freshness_outputs["component_status"])

        derived_input_freshness = (freshness_outputs["derived_status"])

        stage_summaries["derived_input_component_freshness"] = _frame_summary(derived_input_component_freshness)
        _write_pipeline_artifact(store, run_id, "derived_input_component_freshness", derived_input_component_freshness)

        stage_summaries["derived_input_freshness"] = _frame_summary(derived_input_freshness)
        _write_pipeline_artifact(store, run_id, "derived_input_freshness", derived_input_freshness)

        stage_summaries["smoothing_lineage"] = _frame_summary(smoothing_lineage)
        _write_pipeline_artifact(store, run_id, "smoothing_lineage", smoothing_lineage)
        
        print("[regime_pipeline] 2/9 normalizing features")
        normalized_features = normalize_features(features)
        stage_summaries["normalized_features"] = _frame_summary(
            normalized_features
        )
        _write_pipeline_artifact(store, run_id, "normalized_features", normalized_features)

        print("[regime_pipeline] 3/9 scoring metrics")
        metric_scores = score_metrics(normalized_features)
        stage_summaries["metric_scores"] = _frame_summary(metric_scores)
        _write_pipeline_artifact(store, run_id, "metric_scores", metric_scores)

        print("[regime_pipeline] 4/9 aligning metric scores")
        aligned_metric_scores = align_metric_scores_asof(metric_scores)
        stage_summaries["aligned_metric_scores"] = _frame_summary(aligned_metric_scores)
        _write_pipeline_artifact(store, run_id, "aligned_metric_scores", aligned_metric_scores)

        print("[regime_pipeline] 5/9 scoring dimensions")
        dimension_scores = score_dimensions(aligned_metric_scores)
        stage_summaries["dimension_scores"] = _frame_summary(dimension_scores)
        _write_pipeline_artifact(store, run_id, "dimension_scores", dimension_scores)

        print("[regime_pipeline] 6/9 scoring axes")
        axis_scores = score_axes(dimension_scores)
        stage_summaries["axis_scores"] = _frame_summary(axis_scores)
        _write_pipeline_artifact(store, run_id, "axis_scores", axis_scores)

        print("[regime_pipeline] 7/9 building coordinates")
        coordinates = build_coordinates(axis_scores)
        stage_summaries["coordinates"] = _frame_summary(coordinates)
        _write_pipeline_artifact(store, run_id, "coordinates", coordinates)

        print("[regime_pipeline] 8/9 assigning geometry")
        geometry = assign_geometry(coordinates)
        stage_summaries["geometry"] = _frame_summary(geometry)
        _write_pipeline_artifact(store, run_id, "geometry", geometry)

        print("[regime_pipeline] 9/9 assigning regimes")
        regime_assignments = assign_regimes(geometry)
        stage_summaries["regime_assignments"] = _frame_summary(regime_assignments)
        _write_pipeline_artifact(store, run_id, "regime_assignments", regime_assignments)

        print("[regime_pipeline] building validation artifacts")

        trajectory = build_historical_trajectory(
            regimes=regime_assignments,
            geo_ids=validation_geo_ids,
        )
        _write_validation_artifact(store, run_id, "historical_trajectory", trajectory)

        transition_events = build_transition_events(
            trajectory=trajectory,
            geo_ids=validation_geo_ids,
        )
        _write_validation_artifact(store, run_id, "transition_events", transition_events)

        transition_audit = build_transition_audit(
            trajectory=trajectory,
            geo_ids=validation_geo_ids,
        )
        _write_validation_artifact(store, run_id, "transition_audit", transition_audit)

        seasonality = build_seasonality_audit(
            trajectory=trajectory,
            geo_ids=validation_geo_ids,
        )

        for artifact_name, dataframe in seasonality.items():
            _write_validation_artifact(
                store,
                run_id,
                f"seasonality_{artifact_name}",
                dataframe,
            )

        contribution = build_metric_contribution_audit(
            trajectory=trajectory,
            aligned_metric_scores=aligned_metric_scores,
            geo_ids=validation_geo_ids,
            axis="supply",
        )

        for artifact_name, dataframe in contribution.items():
            _write_validation_artifact(
                store,
                run_id,
                f"supply_contribution_{artifact_name}",
                dataframe,
            )

        completed_at = _utc_now_iso()

        store.update_manifest(
            run_id,
            status="complete",
            metadata_updates={
                "completed_at_utc": completed_at,
                "stage_summaries": stage_summaries,
            },
        )

        verification = store.verify_run(run_id)

        if verification.empty:
            raise RuntimeError("Completed run contains no recorded artifacts")

        if not verification["exists"].all():
            raise RuntimeError("One or more recorded artifacts are missing")

        if not verification["hash_matches"].all():
            raise RuntimeError("One or more artifact hashes failed verification")

        return store.read_manifest(run_id)

    except Exception as exc:
        error_metadata = {
            "failed_at_utc": _utc_now_iso(),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
            "stage_summaries": stage_summaries,
        }

        try:
            store.update_manifest(
                run_id,
                status="failed",
                metadata_updates=error_metadata,
            )
        except Exception:
            pass

        raise
