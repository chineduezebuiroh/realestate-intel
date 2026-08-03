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
    target_replacement_reconciliation: pd.DataFrame

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


_TARGET_KEY_COLUMNS = ["geo_id", "date", "canonical_metric_key", "feature_key"]


def _target_replacement_reconciliation(
    incumbent: pd.DataFrame,
    candidate: pd.DataFrame,
    *,
    experiment_id: str,
    target_feature_keys: Collection[str],
    campaign_geo_ids: Collection[str],
) -> pd.DataFrame:
    """Validate and describe governed target replacement chronology."""
    target = frozenset(str(value) for value in target_feature_keys)
    geos = frozenset(str(value) for value in campaign_geo_ids)
    incumbent_mask = incumbent["feature_key"].isin(target) & incumbent["geo_id"].astype(str).isin(geos)
    candidate_family = candidate["feature_key"].isin(target)
    outside = candidate_family & ~candidate["geo_id"].astype(str).isin(geos)

    def fail(reason: str, count: int, sample: list[object]) -> None:
        raise ValueError(
            f"Target replacement reconciliation failed: candidate={experiment_id}; "
            f"reason={reason}; count={count}; sample={sample[:5]}"
        )

    if outside.any():
        sample = list(candidate.loc[outside, _TARGET_KEY_COLUMNS].itertuples(index=False, name=None))
        fail("out_of_scope_target_rows", int(outside.sum()), sample)

    base = incumbent.loc[incumbent_mask, _TARGET_KEY_COLUMNS].copy()
    challenger = candidate.loc[candidate_family, _TARGET_KEY_COLUMNS].copy()
    for label, frame in (("incumbent", base), ("candidate", challenger)):
        duplicate = frame.duplicated(_TARGET_KEY_COLUMNS, keep=False)
        if duplicate.any():
            sample = list(frame.loc[duplicate, _TARGET_KEY_COLUMNS].itertuples(index=False, name=None))
            fail("duplicate_target_keys", int(duplicate.sum()), [(label, *row) for row in sample])
        duplicate_date = frame.duplicated(["geo_id", "feature_key", "date"], keep=False)
        if duplicate_date.any():
            sample = list(frame.loc[duplicate_date, _TARGET_KEY_COLUMNS].itertuples(index=False, name=None))
            fail("duplicate_target_keys", int(duplicate_date.sum()), [(label, *row) for row in sample])

    base_keys = set(base.itertuples(index=False, name=None))
    candidate_keys = set(challenger.itertuples(index=False, name=None))
    candidate_only = candidate_keys - base_keys
    if candidate_only:
        fail("candidate_only_target_keys", len(candidate_only), sorted(candidate_only, key=str))

    rows: list[dict[str, object]] = []
    expected_series = [(geo, feature) for geo in sorted(geos) for feature in sorted(target)]
    for geo_id, feature_key in expected_series:
        base_part = base[base["geo_id"].astype(str).eq(geo_id) & base["feature_key"].astype(str).eq(feature_key)]
        candidate_part = challenger[
            challenger["geo_id"].astype(str).eq(geo_id)
            & challenger["feature_key"].astype(str).eq(feature_key)
        ]
        base_dates = sorted(pd.to_datetime(base_part["date"]).unique())
        candidate_dates = sorted(pd.to_datetime(candidate_part["date"]).unique())
        if not base_dates or not candidate_dates:
            fail("missing_target_series", 1, [(geo_id, feature_key)])
        first = candidate_dates[0]
        last = candidate_dates[-1]
        missing = sorted(set(base_dates) - set(candidate_dates))
        leading = [date for date in missing if date < first]
        interior = [date for date in missing if first < date < last]
        trailing = [date for date in missing if date > last]
        # A missing key at the first candidate date (for example because its
        # canonical metric differs) is already caught as candidate-only.
        if interior:
            fail("interior_target_gap", len(interior), [(geo_id, feature_key, date) for date in interior])
        if trailing:
            fail("trailing_target_gap", len(trailing), [(geo_id, feature_key, date) for date in trailing])
        expected_leading = [date for date in base_dates if date < first]
        if leading != expected_leading or missing != leading:
            fail("interior_target_gap", len(set(missing) ^ set(expected_leading)) or 1,
                 [(geo_id, feature_key, date) for date in missing])
        rows.append({
            "candidate_policy_id": experiment_id,
            "geo_id": geo_id,
            "feature_key": feature_key,
            "incumbent_first_date": base_dates[0],
            "candidate_first_date": first,
            "incumbent_last_date": base_dates[-1],
            "candidate_last_date": last,
            "incumbent_row_count": len(base_part),
            "candidate_row_count": len(candidate_part),
            "leading_warmup_rows": len(leading),
            "interior_missing_rows": 0,
            "trailing_missing_rows": 0,
            "candidate_only_rows": 0,
            "warmup_reconciliation_pass": True,
            "failure_reason": "leading_warmup_valid",
        })
    return pd.DataFrame(rows)


def build_in_memory_smoothing_challenger(
    *,
    baseline_features: pd.DataFrame,
    source_metrics: pd.DataFrame,
    experiment_id: str,
    incumbent_artifacts: Mapping[str, pd.DataFrame] | None = None,
    target_feature_keys: Collection[str] | None = None,
    primary_axes: Collection[str] | None = None,
    supporting_axes: Collection[str] | None = None,
    campaign_output_geo_ids: Collection[str] | None = None,
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
    campaign_geos = frozenset(str(item) for item in (campaign_output_geo_ids or ()))
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
        if not campaign_geos:
            raise ValueError("Complete mixed-universe construction requires campaign_output_geo_ids")
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
    target_reconciliation = pd.DataFrame()
    if incumbent_artifacts is None:
        normalized_features = candidate_normalized
    else:
        incumbent_normalized = incumbent_artifacts["normalized_features"].copy(deep=True)
        incumbent_target_mask = (
            incumbent_normalized["feature_key"].isin(target)
            & incumbent_normalized["geo_id"].astype(str).isin(campaign_geos)
        )
        candidate_target_mask = candidate_normalized["feature_key"].isin(target)
        target_reconciliation = _target_replacement_reconciliation(
            incumbent_normalized, candidate_normalized, experiment_id=experiment_id,
            target_feature_keys=target, campaign_geo_ids=campaign_geos,
        )
        # The upstream dependency universe remains complete.  Replacement is
        # bounded by both target identity and campaign geography; target rows
        # outside the campaign are immutable dependencies too.
        incumbent_preserved = incumbent_normalized[~incumbent_target_mask]
        normalized_features = pd.concat(
            [incumbent_preserved, candidate_normalized[candidate_target_mask]],
            ignore_index=True,
        )
        keys = ["geo_id", "date", "canonical_metric_key", "feature_key"]
        if normalized_features.duplicated(keys).any():
            raise ValueError("Mixed normalized-feature universe contains duplicate keys")
        normalized_features = normalized_features.sort_values(keys, kind="mergesort").reset_index(drop=True)
        mixed_preserved = normalized_features[~(
            normalized_features["feature_key"].isin(target)
            & normalized_features["geo_id"].astype(str).isin(campaign_geos)
        )]
        try:
            pd.testing.assert_frame_equal(
                incumbent_preserved.sort_values(keys, kind="mergesort").reset_index(drop=True),
                mixed_preserved.sort_values(keys, kind="mergesort").reset_index(drop=True),
            )
        except AssertionError as exc:
            raise ValueError("Mixed normalized-feature universe changed preserved dependency rows") from exc
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

    if complete_mode:
        started = perf_counter()
        # This is the sole downstream campaign-output filtering point. Scoring
        # above consumes the complete upstream dependency universe.
        def campaign_only(frame: pd.DataFrame) -> pd.DataFrame:
            return frame[frame["geo_id"].astype(str).isin(campaign_geos)].copy().reset_index(drop=True)

        dimension_scores = campaign_only(dimension_scores)
        axis_scores = campaign_only(axis_scores)
        coordinates = campaign_only(coordinates)
        geometry = campaign_only(geometry)
        regime_assignments = campaign_only(regime_assignments)
        print(f"[inventory-challenger] downstream campaign-output filtering {perf_counter() - started:,.1f}s", flush=True)

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
        target_replacement_reconciliation=target_reconciliation,
    )
