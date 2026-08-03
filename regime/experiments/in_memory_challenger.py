from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Collection, Mapping

import pandas as pd

from regime._02_feature_normalizer import (
    normalize_features,
)
from regime._03_metric_scorer import (
    score_metrics,
)
from regime._04_asof_aligner import (
    align_metric_scores_asof,
)
from regime._05_dimension_scorer import (
    score_dimensions,
)
from regime._06_axis_engine import (
    _build_axis_weights,
    score_axes,
)
from regime._07_coordinate_engine import (
    build_coordinates,
)
from regime._08_geometry_engine import (
    assign_geometry,
)
from regime._09_regime_assignment import (
    assign_regimes,
)
from regime.experiments.smoothing_run import (
    apply_smoothing_experiment,
)


@dataclass(slots=True)
class InMemoryChallengerArtifacts:
    """
    Full downstream challenger artifact set built without
    creating or mutating a persisted regime run.
    """

    features: pd.DataFrame
    smoothing_lineage: pd.DataFrame
    normalized_features: pd.DataFrame
    metric_scores: pd.DataFrame
    aligned_metric_scores: pd.DataFrame
    dimension_scores: pd.DataFrame
    axis_scores: pd.DataFrame
    coordinates: pd.DataFrame
    geometry: pd.DataFrame
    regime_assignments: pd.DataFrame

    def as_mapping(
        self,
    ) -> dict[str, pd.DataFrame]:
        return {
            "features": self.features,
            "smoothing_lineage": (
                self.smoothing_lineage
            ),
            "normalized_features": (
                self.normalized_features
            ),
            "metric_scores": (
                self.metric_scores
            ),
            "aligned_metric_scores": (
                self.aligned_metric_scores
            ),
            "dimension_scores": (
                self.dimension_scores
            ),
            "axis_scores": self.axis_scores,
            "coordinates": self.coordinates,
            "geometry": self.geometry,
            "regime_assignments": (
                self.regime_assignments
            ),
        }


def build_in_memory_smoothing_challenger(
    *,
    baseline_features: pd.DataFrame,
    source_metrics: pd.DataFrame,
    experiment_id: str,
    incumbent_artifacts: Mapping[str, pd.DataFrame] | None = None,
    target_feature_keys: Collection[str] | None = None,
    primary_axes: Collection[str] | None = None,
    supporting_axes: Collection[str] | None = None,
    require_complete_universe: bool = False,
) -> InMemoryChallengerArtifacts:
    """
    Apply an approved smoothing experiment and execute all
    downstream regime stages without persisting a challenger run.

    The supplied baseline feature and source frames are not
    modified.
    """

    target = tuple(target_feature_keys or ())
    primary = tuple(primary_axes or ())
    supporting = tuple(supporting_axes or ())
    complete_mode = incumbent_artifacts is not None or require_complete_universe
    if require_complete_universe and incumbent_artifacts is None:
        raise ValueError("Complete mixed-universe construction requires incumbent_artifacts")
    if complete_mode:
        required_artifacts = {"normalized_features", "axis_scores"}
        missing_artifacts = required_artifacts.difference(incumbent_artifacts or {})
        if missing_artifacts:
            raise ValueError(f"Incumbent artifacts missing required tables: {sorted(missing_artifacts)}")
        if not target:
            raise ValueError("Complete mixed-universe construction requires target_feature_keys")
        if not primary or not supporting:
            raise ValueError("Complete mixed-universe construction requires primary_axes and supporting_axes")
        if len(primary) != len(set(primary)) or len(supporting) != len(set(supporting)):
            raise ValueError("Axis scope must not contain duplicates")
        if not set(primary).issubset(supporting):
            raise ValueError("primary_axes must be a subset of supporting_axes")
        governed_axes = set(_build_axis_weights()["axis"].astype(str))
        unknown = (set(primary) | set(supporting)).difference(governed_axes)
        if unknown:
            raise ValueError(f"Axis scope contains unknown axes: {sorted(unknown)}")

    started = perf_counter()
    (
        challenger_features,
        smoothing_lineage,
    ) = apply_smoothing_experiment(
        features=baseline_features.copy(),
        source_metrics=source_metrics.copy(),
        experiment_id=experiment_id,
    )
    print(f"[inventory-challenger] mixed-universe assembly/raw replacement {perf_counter() - started:,.1f}s", flush=True)

    started = perf_counter()
    candidate_normalized = normalize_features(challenger_features)
    if incumbent_artifacts is None:
        normalized_features = candidate_normalized
    else:
        incumbent_normalized = incumbent_artifacts["normalized_features"].copy(deep=True)
        incumbent_targets = set(incumbent_normalized.loc[
            incumbent_normalized["feature_key"].isin(target), "feature_key"
        ].astype(str))
        candidate_targets = set(candidate_normalized.loc[
            candidate_normalized["feature_key"].isin(target), "feature_key"
        ].astype(str))
        if incumbent_targets != set(target) or candidate_targets != set(target):
            raise ValueError(
                "Target feature family is incomplete; "
                f"incumbent={sorted(incumbent_targets)}, candidate={sorted(candidate_targets)}, expected={sorted(target)}"
            )
        incumbent_non_target = incumbent_normalized[~incumbent_normalized["feature_key"].isin(target)]
        normalized_features = pd.concat(
            [incumbent_non_target,
             candidate_normalized[candidate_normalized["feature_key"].isin(target)]],
            ignore_index=True,
        )
        keys = ["geo_id", "date", "canonical_metric_key", "feature_key"]
        if normalized_features.duplicated(keys).any():
            raise ValueError("Mixed normalized-feature universe contains duplicate keys")
        normalized_features = normalized_features.sort_values(keys, kind="mergesort").reset_index(drop=True)
        mixed_non_target = normalized_features[~normalized_features["feature_key"].isin(target)]
        if set(map(tuple, incumbent_non_target[keys].itertuples(index=False, name=None))) != set(
            map(tuple, mixed_non_target[keys].itertuples(index=False, name=None))
        ):
            raise ValueError("Mixed normalized-feature universe changed the non-target key set")
    print(f"[inventory-challenger] mixed-universe assembly/normalization {perf_counter() - started:,.1f}s", flush=True)

    started = perf_counter()
    metric_scores = score_metrics(
        normalized_features
    )
    print(f"[inventory-challenger] metric scoring {perf_counter() - started:,.1f}s", flush=True)

    started = perf_counter()
    aligned_metric_scores = (
        align_metric_scores_asof(
            metric_scores
        )
    )
    print(f"[inventory-challenger] alignment {perf_counter() - started:,.1f}s", flush=True)

    started = perf_counter()
    dimension_scores = score_dimensions(
        aligned_metric_scores
    )
    print(f"[inventory-challenger] dimension scoring {perf_counter() - started:,.1f}s", flush=True)

    started = perf_counter()
    axis_scores = score_axes(
        dimension_scores
    )
    if complete_mode:
        # Primary axes are recomputed. Supporting-only axes are immutable
        # incumbent coordinate inputs, independent of campaign identity.
        incumbent_axes = incumbent_artifacts["axis_scores"]
        supporting_only = set(supporting).difference(primary)
        axis_scores = pd.concat(
            [axis_scores[axis_scores["axis"].isin(primary)],
             incumbent_axes[incumbent_axes["axis"].isin(supporting_only)]],
            ignore_index=True,
        ).sort_values(["geo_id", "date", "axis"], kind="mergesort").reset_index(drop=True)
        actual_axes = set(axis_scores["axis"].astype(str))
        if actual_axes != set(supporting):
            raise ValueError(
                "Final challenger axis identity mismatch; "
                f"expected={sorted(supporting)}, actual={sorted(actual_axes)}"
            )
        for axis in primary:
            if axis not in actual_axes:
                raise ValueError(f"Primary axis was not recomputed: {axis}")
    print(f"[inventory-challenger] axis scoring {perf_counter() - started:,.1f}s", flush=True)

    started = perf_counter()
    coordinates = build_coordinates(
        axis_scores
    )

    geometry = assign_geometry(
        coordinates
    )

    regime_assignments = assign_regimes(
        geometry
    )
    print(f"[inventory-challenger] coordinate/regime generation {perf_counter() - started:,.1f}s", flush=True)

    return InMemoryChallengerArtifacts(
        features=challenger_features,
        smoothing_lineage=smoothing_lineage,
        normalized_features=(
            normalized_features
        ),
        metric_scores=metric_scores,
        aligned_metric_scores=(
            aligned_metric_scores
        ),
        dimension_scores=dimension_scores,
        axis_scores=axis_scores,
        coordinates=coordinates,
        geometry=geometry,
        regime_assignments=(
            regime_assignments
        ),
    )
