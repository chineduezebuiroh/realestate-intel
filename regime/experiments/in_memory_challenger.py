from __future__ import annotations

from dataclasses import dataclass

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
) -> InMemoryChallengerArtifacts:
    """
    Apply an approved smoothing experiment and execute all
    downstream regime stages without persisting a challenger run.

    The supplied baseline feature and source frames are not
    modified.
    """

    (
        challenger_features,
        smoothing_lineage,
    ) = apply_smoothing_experiment(
        features=baseline_features.copy(),
        source_metrics=source_metrics.copy(),
        experiment_id=experiment_id,
    )

    normalized_features = (
        normalize_features(
            challenger_features
        )
    )

    metric_scores = score_metrics(
        normalized_features
    )

    aligned_metric_scores = (
        align_metric_scores_asof(
            metric_scores
        )
    )

    dimension_scores = score_dimensions(
        aligned_metric_scores
    )

    axis_scores = score_axes(
        dimension_scores
    )

    coordinates = build_coordinates(
        axis_scores
    )

    geometry = assign_geometry(
        coordinates
    )

    regime_assignments = assign_regimes(
        geometry
    )

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