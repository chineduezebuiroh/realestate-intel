from __future__ import annotations
# regime/diagnostics/transition_sensitivity.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.diagnostics.axis_contribution import (
    DEFAULT_AUDIT_GEOS,
    DEFAULT_RUN_ID,
    build_axis_contribution_audit,
)
from regime.diagnostics.chronological_axis_review import build_chronological_axis_review
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes


TARGET_DIMENSIONS = (
    "price",
    "affordability",
    "supply",
)

TARGET_METRICS = (
    "median_sale_price",
    "median_ppsf",
    "price_to_income",
    "payment_burden",
    "active_inventory",
    "permit_activity",
    "permit_intensity",
)

DEFAULT_WEIGHT_MULTIPLIER = 0.50

AXES = (
    "demand",
    "supply",
)

MATURE_HISTORY_START = pd.Timestamp(
    "2015-12-31"
)

PERSISTENCE_HORIZONS = (
    1,
    3,
    6,
)


def _assign_counterfactual_regimes(
    counterfactual_axes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Pass counterfactual axis scores through the canonical downstream
    regime pipeline:

        axes -> coordinates -> geometry -> regime assignments

    The supplied geo_id values must already be unique per
    counterfactual scenario and date.
    """
    required = {
        "geo_id",
        "date",
        "demand_axis_score",
        "supply_axis_score",
    }

    missing = (
        required
        - set(counterfactual_axes.columns)
    )

    if missing:
        raise ValueError(
            "Counterfactual axis frame is missing "
            f"required columns: {sorted(missing)}"
        )

    work = counterfactual_axes.copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["demand_axis_score"] = pd.to_numeric(
        work["demand_axis_score"],
        errors="coerce",
    )

    work["supply_axis_score"] = pd.to_numeric(
        work["supply_axis_score"],
        errors="coerce",
    )

    invalid = work[
        work["date"].isna()
        | work["demand_axis_score"].isna()
        | work["supply_axis_score"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "Counterfactual axes contain invalid "
            "dates or scores:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicate_keys = work.duplicated(
        subset=[
            "geo_id",
            "date",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Counterfactual geo/date keys must be "
            "unique before canonical assignment:\n"
            + work.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    axis_long = work.melt(
        id_vars=[
            "geo_id",
            "date",
        ],
        value_vars=[
            "demand_axis_score",
            "supply_axis_score",
        ],
        var_name="axis",
        value_name="axis_score",
    )

    axis_long["axis"] = (
        axis_long["axis"]
        .str.replace(
            "_axis_score",
            "",
            regex=False,
        )
    )

    # build_coordinates() uses this field when creating
    # max_axis_age_days. Counterfactual sensitivity is testing
    # classification geometry, not changing source freshness.
    axis_long["max_dimension_age_days"] = 0

    expected_axes = {
        "demand",
        "supply",
    }

    actual_axes = set(
        axis_long["axis"]
    )

    if actual_axes != expected_axes:
        raise AssertionError(
            "Counterfactual axis contract mismatch. "
            f"Expected {sorted(expected_axes)}, "
            f"found {sorted(actual_axes)}"
        )

    coordinates = build_coordinates(
        axes=axis_long
    )

    required_coordinate_columns = {
        "geo_id",
        "date",
        "x_supply",
        "y_demand",
        "radius",
        "angle_degrees",
        "axis_count",
        "min_axis_score",
        "max_axis_score",
        "max_axis_age_days",
    }

    missing_coordinates = (
        required_coordinate_columns
        - set(coordinates.columns)
    )

    if missing_coordinates:
        raise AssertionError(
            "Canonical coordinate engine did not "
            "return required columns: "
            f"{sorted(missing_coordinates)}"
        )

    geometry = assign_geometry(
        coordinates=coordinates
    )

    required_geometry_columns = {
        "geo_id",
        "date",
        "major_regime",
        "minor_regime",
        "quadrant",
        "radius",
        "angle_degrees",
        "distance_to_boundary_degrees",
        "x_supply",
        "y_demand",
    }

    missing_geometry = (
        required_geometry_columns
        - set(geometry.columns)
    )

    if missing_geometry:
        raise AssertionError(
            "Canonical geometry engine did not "
            "return required columns: "
            f"{sorted(missing_geometry)}"
        )

    assignments = assign_regimes(
        geometry=geometry
    )

    required_assignment_columns = {
        "geo_id",
        "date",
        "major_regime",
        "minor_regime",
        "regime_strength",
        "angle_degrees",
        "distance_to_boundary_degrees",
    }

    missing_assignments = (
        required_assignment_columns
        - set(assignments.columns)
    )

    if missing_assignments:
        raise AssertionError(
            "Canonical regime assignment did not "
            "return required columns: "
            f"{sorted(missing_assignments)}"
        )

    if len(assignments) != len(work):
        raise AssertionError(
            "Counterfactual assignment row count changed. "
            f"Input rows: {len(work)}, "
            f"assignment rows: {len(assignments)}"
        )

    return assignments


def _transition_events(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    work = timeline.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).copy()

    work["transition_id"] = (
        work["geo_id"].astype(str)
        + "__"
        + work["date"].dt.strftime(
            "%Y%m%d"
        )
    )

    transitions = work[
        work["major_changed"]
        | work["minor_changed"]
    ].copy()

    return transitions.reset_index(
        drop=True
    )


def _axis_dimension_policy(
    axis_contributions: pd.DataFrame,
) -> pd.DataFrame:
    policy = (
        axis_contributions[
            [
                "axis",
                "dimension",
                "dimension_weight",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "axis",
                "dimension",
            ]
        )
        .reset_index(drop=True)
    )

    duplicates = policy.duplicated(
        [
            "axis",
            "dimension",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Conflicting axis-dimension policies:\n"
            + policy.loc[
                duplicates
            ].to_string(index=False)
        )

    return policy


def _metric_dimension_policy(
    metric_contributions: pd.DataFrame,
) -> pd.DataFrame:
    policy = (
        metric_contributions[
            [
                "dimension",
                "canonical_metric_key",
                "metric_weight",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "dimension",
                "canonical_metric_key",
            ]
        )
        .reset_index(drop=True)
    )

    duplicates = policy.duplicated(
        [
            "dimension",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Conflicting metric-dimension policies:\n"
            + policy.loc[
                duplicates
            ].to_string(index=False)
        )

    return policy


def _dimension_panel(
    axis_contributions: pd.DataFrame,
) -> pd.DataFrame:
    return (
        axis_contributions[
            [
                "geo_id",
                "date",
                "axis",
                "dimension",
                "dimension_score",
                "dimension_weight",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "geo_id",
                "date",
                "axis",
                "dimension",
            ]
        )
        .reset_index(drop=True)
    )


def _metric_panel(
    metric_contributions: pd.DataFrame,
) -> pd.DataFrame:
    return (
        metric_contributions[
            [
                "geo_id",
                "date",
                "dimension",
                "canonical_metric_key",
                "metric_score",
                "metric_weight",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "geo_id",
                "date",
                "dimension",
                "canonical_metric_key",
            ]
        )
        .reset_index(drop=True)
    )


def _rebuild_weighted_score(
    frame: pd.DataFrame,
    *,
    score_column: str,
    weight_column: str,
) -> float:
    available = frame[
        frame[score_column].notna()
        & frame[weight_column].notna()
        & frame[weight_column].gt(0)
    ]

    if available.empty:
        return np.nan

    weight_sum = available[
        weight_column
    ].sum()

    if weight_sum <= 0:
        return np.nan

    return float(
        (
            available[score_column]
            * available[weight_column]
        ).sum()
        / weight_sum
    )


def _apply_dimension_counterfactual(
    current: pd.DataFrame,
    previous: pd.DataFrame,
    *,
    target_dimension: str,
    scenario: str,
    weight_multiplier: float,
) -> pd.DataFrame:
    work = current.copy()

    if scenario == "freeze":
        previous_score = previous.loc[
            previous["dimension"].eq(
                target_dimension
            ),
            "dimension_score",
        ]

        if previous_score.empty:
            return work

        work.loc[
            work["dimension"].eq(
                target_dimension
            ),
            "dimension_score",
        ] = previous_score.iloc[0]

    elif scenario == "remove":
        work = work[
            ~work["dimension"].eq(
                target_dimension
            )
        ].copy()

    elif scenario == "reweight":
        work.loc[
            work["dimension"].eq(
                target_dimension
            ),
            "dimension_weight",
        ] *= weight_multiplier

    else:
        raise ValueError(
            f"Unsupported scenario: {scenario}"
        )

    return work


def _rebuild_axes_from_dimensions(
    dimension_rows: pd.DataFrame,
) -> dict[str, float]:
    output: dict[str, float] = {}

    for axis in AXES:
        axis_rows = dimension_rows[
            dimension_rows["axis"].eq(
                axis
            )
        ]

        output[
            f"{axis}_axis_score"
        ] = _rebuild_weighted_score(
            axis_rows,
            score_column="dimension_score",
            weight_column="dimension_weight",
        )

    return output


def _rebuild_dimension_from_metrics(
    metric_rows: pd.DataFrame,
) -> float:
    return _rebuild_weighted_score(
        metric_rows,
        score_column="metric_score",
        weight_column="metric_weight",
    )


def _apply_metric_counterfactual(
    current_metric_rows: pd.DataFrame,
    previous_metric_rows: pd.DataFrame,
    *,
    target_metric: str,
    scenario: str,
    weight_multiplier: float,
) -> tuple[str | None, float]:
    target = current_metric_rows[
        current_metric_rows[
            "canonical_metric_key"
        ].eq(target_metric)
    ]

    if target.empty:
        return None, np.nan

    dimension = str(
        target["dimension"].iloc[0]
    )

    dimension_rows = current_metric_rows[
        current_metric_rows[
            "dimension"
        ].eq(dimension)
    ].copy()

    if scenario == "freeze":
        previous_score = (
            previous_metric_rows[
                previous_metric_rows[
                    "canonical_metric_key"
                ].eq(target_metric)
            ]["metric_score"]
        )

        if previous_score.empty:
            return dimension, np.nan

        dimension_rows.loc[
            dimension_rows[
                "canonical_metric_key"
            ].eq(target_metric),
            "metric_score",
        ] = previous_score.iloc[0]

    elif scenario == "remove":
        dimension_rows = dimension_rows[
            ~dimension_rows[
                "canonical_metric_key"
            ].eq(target_metric)
        ].copy()

    elif scenario == "reweight":
        dimension_rows.loc[
            dimension_rows[
                "canonical_metric_key"
            ].eq(target_metric),
            "metric_weight",
        ] *= weight_multiplier

    else:
        raise ValueError(
            f"Unsupported scenario: {scenario}"
        )

    return (
        dimension,
        _rebuild_dimension_from_metrics(
            dimension_rows
        ),
    )


def _event_month_pairs(
    transitions: pd.DataFrame,
) -> pd.DataFrame:
    pairs = transitions[
        [
            "transition_id",
            "geo_id",
            "date",
            "previous_major_regime",
            "major_regime",
            "previous_minor_regime",
            "minor_regime",
            "major_changed",
            "minor_changed",
            "regime_strength",
        ]
    ].copy()

    pairs["previous_date"] = (
        pairs.groupby(
            "geo_id"
        )["date"]
        .transform(
            lambda values: values
        )
    )

    return pairs


def _prior_date_lookup(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    work = timeline[
        [
            "geo_id",
            "date",
        ]
    ].drop_duplicates()

    work = work.sort_values(
        [
            "geo_id",
            "date",
        ]
    )

    work["previous_date"] = (
        work.groupby(
            "geo_id"
        )["date"].shift(1)
    )

    return work


def _counterfactual_coordinates(
    transitions: pd.DataFrame,
    timeline: pd.DataFrame,
    dimension_panel: pd.DataFrame,
    metric_panel: pd.DataFrame,
    *,
    weight_multiplier: float,
) -> pd.DataFrame:
    previous_dates = _prior_date_lookup(
        timeline
    )

    events = transitions.merge(
        previous_dates,
        on=[
            "geo_id",
            "date",
        ],
        how="left",
        validate="one_to_one",
    )

    rows: list[dict[str, object]] = []

    scenarios = (
        "freeze",
        "remove",
        "reweight",
    )

    for event in events.itertuples(
        index=False
    ):
        current_dimensions = (
            dimension_panel[
                dimension_panel[
                    "geo_id"
                ].eq(event.geo_id)
                & dimension_panel[
                    "date"
                ].eq(event.date)
            ].copy()
        )

        previous_dimensions = (
            dimension_panel[
                dimension_panel[
                    "geo_id"
                ].eq(event.geo_id)
                & dimension_panel[
                    "date"
                ].eq(event.previous_date)
            ].copy()
        )

        current_metrics = (
            metric_panel[
                metric_panel[
                    "geo_id"
                ].eq(event.geo_id)
                & metric_panel[
                    "date"
                ].eq(event.date)
            ].copy()
        )

        previous_metrics = (
            metric_panel[
                metric_panel[
                    "geo_id"
                ].eq(event.geo_id)
                & metric_panel[
                    "date"
                ].eq(event.previous_date)
            ].copy()
        )

        for target_dimension in TARGET_DIMENSIONS:
            target_available = (
                current_dimensions[
                    "dimension"
                ]
                .eq(target_dimension)
                .any()
            )

            for scenario in scenarios:
                modified = (
                    _apply_dimension_counterfactual(
                        current_dimensions,
                        previous_dimensions,
                        target_dimension=(
                            target_dimension
                        ),
                        scenario=scenario,
                        weight_multiplier=(
                            weight_multiplier
                        ),
                    )
                )

                rebuilt = (
                    _rebuild_axes_from_dimensions(
                        modified
                    )
                )

                rows.append(
                    {
                        "transition_id": (
                            event.transition_id
                        ),
                        "geo_id": event.geo_id,
                        "date": event.date,
                        "previous_date": (
                            event.previous_date
                        ),
                        "target_level": (
                            "dimension"
                        ),
                        "target_key": (
                            target_dimension
                        ),
                        "target_available": (
                            target_available
                        ),
                        "scenario": scenario,                        
                        "weight_multiplier": (
                            weight_multiplier
                            if scenario
                            == "reweight"
                            else np.nan
                        ),
                        **rebuilt,
                    }
                )

        for target_metric in TARGET_METRICS:
            target_available = (
                current_metrics[
                    "canonical_metric_key"
                ]
                .eq(target_metric)
                .any()
            )

            for scenario in scenarios:
                (
                    dimension,
                    rebuilt_dimension,
                ) = _apply_metric_counterfactual(
                    current_metrics,
                    previous_metrics,
                    target_metric=target_metric,
                    scenario=scenario,
                    weight_multiplier=(
                        weight_multiplier
                    ),
                )

                if (
                    dimension is None
                    or pd.isna(
                        rebuilt_dimension
                    )
                ):
                    continue

                modified_dimensions = (
                    current_dimensions.copy()
                )

                modified_dimensions.loc[
                    modified_dimensions[
                        "dimension"
                    ].eq(dimension),
                    "dimension_score",
                ] = rebuilt_dimension

                rebuilt = (
                    _rebuild_axes_from_dimensions(
                        modified_dimensions
                    )
                )

                rows.append(
                    {
                        "transition_id": (
                            event.transition_id
                        ),
                        "geo_id": event.geo_id,
                        "date": event.date,
                        "previous_date": (
                            event.previous_date
                        ),
                        "target_level": (
                            "metric"
                        ),
                        "target_key": (
                            target_metric
                        ),
                        "target_available": (
                            target_available
                        ),
                        "affected_dimension": (
                            dimension
                        ),                        
                        "scenario": scenario,
                        "weight_multiplier": (
                            weight_multiplier
                            if scenario
                            == "reweight"
                            else np.nan
                        ),
                        **rebuilt,
                    }
                )

    return pd.DataFrame(rows)


def _assign_counterfactual_labels(
    coordinates: pd.DataFrame,
) -> pd.DataFrame:
    """
    Assign regimes to repeated counterfactual scenarios.

    Production engines require one row per geo/date, so each scenario
    receives a synthetic geography identifier. The original geo_id and
    full scenario metadata are restored after assignment.
    """
    work = (
        coordinates
        .reset_index(drop=True)
        .reset_index(
            names="counterfactual_row_id"
        )
    )

    duplicate_ids = work[
        "counterfactual_row_id"
    ].duplicated(
        keep=False
    )

    if duplicate_ids.any():
        raise AssertionError(
            "counterfactual_row_id values are not unique"
        )

    work["actual_geo_id"] = (
        work["geo_id"]
    )

    work["diagnostic_geo_id"] = (
        work["actual_geo_id"].astype(str)
        + "__counterfactual__"
        + work[
            "counterfactual_row_id"
        ].astype(str)
    )

    assign_input = work[
        [
            "diagnostic_geo_id",
            "date",
            "demand_axis_score",
            "supply_axis_score",
        ]
    ].rename(
        columns={
            "diagnostic_geo_id": "geo_id",
        }
    )

    assignments = (
        _assign_counterfactual_regimes(
            assign_input
        )
    )

    assigned = assignments[
        [
            "geo_id",
            "date",
            "major_regime",
            "minor_regime",
            "regime_strength",
            "angle_degrees",
            "distance_to_boundary_degrees",
        ]
    ].rename(
        columns={
            "geo_id": "diagnostic_geo_id",
            "major_regime": (
                "counterfactual_major_regime"
            ),
            "minor_regime": (
                "counterfactual_minor_regime"
            ),
            "regime_strength": (
                "counterfactual_regime_strength"
            ),
            "angle_degrees": (
                "counterfactual_angle_degrees"
            ),
            "distance_to_boundary_degrees": (
                "counterfactual_distance_"
                "to_boundary_degrees"
            ),
        }
    )

    result = work.merge(
        assigned,
        on=[
            "diagnostic_geo_id",
            "date",
        ],
        how="left",
        validate="one_to_one",
    )

    missing_labels = result[
        result[
            "counterfactual_major_regime"
        ].isna()
        | result[
            "counterfactual_minor_regime"
        ].isna()
    ]

    if not missing_labels.empty:
        raise AssertionError(
            "Some counterfactual scenarios did not "
            "receive regime labels:\n"
            + missing_labels.head(30).to_string(
                index=False
            )
        )

    # Restore the original geography name used by downstream joins.
    result["geo_id"] = result[
        "actual_geo_id"
    ]

    return result.drop(
        columns=[
            "actual_geo_id",
        ]
    )


def _evaluate_transition_sensitivity(
    counterfactuals: pd.DataFrame,
    transitions: pd.DataFrame,
) -> pd.DataFrame:
    actual = transitions[
        [
            "transition_id",
            "previous_major_regime",
            "major_regime",
            "previous_minor_regime",
            "minor_regime",
            "major_changed",
            "minor_changed",
            "regime_strength",
        ]
    ]

    out = counterfactuals.merge(
        actual,
        on="transition_id",
        how="left",
        validate="many_to_one",
    )

    out[
        "counterfactual_major_matches_previous"
    ] = (
        out["counterfactual_major_regime"]
        == out["previous_major_regime"]
    )

    out[
        "counterfactual_minor_matches_previous"
    ] = (
        out["counterfactual_minor_regime"]
        == out["previous_minor_regime"]
    )

    out[
        "counterfactual_major_matches_actual"
    ] = (
        out["counterfactual_major_regime"]
        == out["major_regime"]
    )

    out[
        "counterfactual_minor_matches_actual"
    ] = (
        out["counterfactual_minor_regime"]
        == out["minor_regime"]
    )

    out[
        "major_transition_prevented"
    ] = (
        out["major_changed"]
        & out[
            "counterfactual_major_matches_previous"
        ]
    )

    out[
        "minor_transition_prevented"
    ] = (
        out["minor_changed"]
        & out[
            "counterfactual_minor_matches_previous"
        ]
    )

    out[
        "major_assignment_changed"
    ] = ~out[
        "counterfactual_major_matches_actual"
    ]

    out[
        "minor_assignment_changed"
    ] = ~out[
        "counterfactual_minor_matches_actual"
    ]

    out[
        "eligible_major_transition"
    ] = (
        out["target_available"]
        & out["major_changed"]
    )

    out[
        "eligible_minor_transition"
    ] = (
        out["target_available"]
        & out["minor_changed"]
    )

    out["mature_history_flag"] = (
        pd.to_datetime(
            out["date"]
        )
        >= MATURE_HISTORY_START
    )

    return out


def _transition_persistence(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    """
    Measure point-in-time and continuous regime persistence.

    Point persistence:
        Regime at exactly t+h equals the transition-month regime.

    Continuous persistence:
        Every observed month from t+1 through t+h remains in the
        transition-month regime.
    """
    work = timeline.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).copy()

    transitions = work[
        work["major_changed"]
        | work["minor_changed"]
    ].copy()

    rows: list[dict[str, object]] = []

    for transition in transitions.itertuples(
        index=False
    ):
        geo_history = (
            work[
                work["geo_id"].eq(
                    transition.geo_id
                )
                & work["date"].gt(
                    transition.date
                )
            ]
            .sort_values("date")
            .reset_index(drop=True)
        )

        output = transition._asdict()
        output["mature_history_flag"] = (
            pd.Timestamp(
                transition.date
            )
            >= MATURE_HISTORY_START
        )

        for horizon in PERSISTENCE_HORIZONS:
            future = geo_history.head(
                horizon
            )

            horizon_available = (
                len(future) == horizon
            )

            output[
                f"major_horizon_available_{horizon}m"
            ] = horizon_available

            output[
                f"minor_horizon_available_{horizon}m"
            ] = horizon_available

            if not horizon_available:
                output[
                    f"major_persists_{horizon}m"
                ] = False
                output[
                    f"minor_persists_{horizon}m"
                ] = False
                output[
                    f"major_reverses_{horizon}m"
                ] = False
                output[
                    f"minor_reverses_{horizon}m"
                ] = False
                output[
                    (
                        "major_continuously_"
                        f"persists_{horizon}m"
                    )
                ] = False
                output[
                    (
                        "minor_continuously_"
                        f"persists_{horizon}m"
                    )
                ] = False
                continue

            horizon_row = future.iloc[
                horizon - 1
            ]

            output[
                f"major_persists_{horizon}m"
            ] = bool(
                transition.major_changed
                and (
                    horizon_row["major_regime"]
                    == transition.major_regime
                )
            )

            output[
                f"minor_persists_{horizon}m"
            ] = bool(
                transition.minor_changed
                and (
                    horizon_row["minor_regime"]
                    == transition.minor_regime
                )
            )

            output[
                f"major_reverses_{horizon}m"
            ] = bool(
                transition.major_changed
                and (
                    horizon_row["major_regime"]
                    == transition.previous_major_regime
                )
            )

            output[
                f"minor_reverses_{horizon}m"
            ] = bool(
                transition.minor_changed
                and (
                    horizon_row["minor_regime"]
                    == transition.previous_minor_regime
                )
            )

            output[
                (
                    "major_continuously_"
                    f"persists_{horizon}m"
                )
            ] = bool(
                transition.major_changed
                and future[
                    "major_regime"
                ].eq(
                    transition.major_regime
                ).all()
            )

            output[
                (
                    "minor_continuously_"
                    f"persists_{horizon}m"
                )
            ] = bool(
                transition.minor_changed
                and future[
                    "minor_regime"
                ].eq(
                    transition.minor_regime
                ).all()
            )

        rows.append(output)

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _build_regime_dwell_times(
    timeline: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build contiguous major- and minor-regime episodes.
    """
    rows: list[dict[str, object]] = []

    for geo_id, geo_frame in timeline.groupby(
        "geo_id"
    ):
        geo_frame = geo_frame.sort_values(
            "date"
        ).reset_index(drop=True)

        for regime_level in (
            "major",
            "minor",
        ):
            regime_column = (
                f"{regime_level}_regime"
            )

            episode_id = (
                geo_frame[regime_column]
                .ne(
                    geo_frame[
                        regime_column
                    ].shift(1)
                )
                .cumsum()
            )

            episode_frame = (
                geo_frame.assign(
                    episode_id=episode_id
                )
                .groupby(
                    "episode_id",
                    as_index=False,
                )
                .agg(
                    regime=(
                        regime_column,
                        "first",
                    ),
                    start_date=(
                        "date",
                        "min",
                    ),
                    end_date=(
                        "date",
                        "max",
                    ),
                    observation_count=(
                        "date",
                        "size",
                    ),
                    mean_regime_strength=(
                        "regime_strength",
                        "mean",
                    ),
                    minimum_regime_strength=(
                        "regime_strength",
                        "min",
                    ),
                )
            )

            episode_frame[
                "geo_id"
            ] = geo_id

            episode_frame[
                "regime_level"
            ] = regime_level

            episode_frame[
                "duration_months"
            ] = episode_frame[
                "observation_count"
            ]

            rows.extend(
                episode_frame.to_dict(
                    orient="records"
                )
            )

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "geo_id",
                "regime_level",
                "start_date",
            ]
        )
        .reset_index(drop=True)
    )


def _conditional_rate(
    frame: pd.DataFrame,
    *,
    result_column: str,
    eligibility_column: str,
) -> float:
    eligible = frame[
        frame[eligibility_column]
    ]

    if eligible.empty:
        return np.nan

    return float(
        eligible[result_column].mean()
    )


def _sensitivity_summary(
    sensitivity: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    group_columns = [
        "target_level",
        "target_key",
        "scenario",
    ]

    for keys, frame in sensitivity.groupby(
        group_columns,
        dropna=False,
    ):
        (
            target_level,
            target_key,
            scenario,
        ) = keys

        available = frame[
            frame["target_available"]
        ]

        rows.append(
            {
                "target_level": target_level,
                "target_key": target_key,
                "scenario": scenario,
                "evaluated_transition_count": (
                    frame[
                        "transition_id"
                    ].nunique()
                ),
                "available_transition_count": (
                    available[
                        "transition_id"
                    ].nunique()
                ),
                "eligible_major_transition_count": int(
                    frame[
                        "eligible_major_transition"
                    ].sum()
                ),
                "eligible_minor_transition_count": int(
                    frame[
                        "eligible_minor_transition"
                    ].sum()
                ),
                "major_transition_prevented_count": int(
                    frame[
                        "major_transition_prevented"
                    ].sum()
                ),
                "major_transition_prevented_rate": (
                    _conditional_rate(
                        frame,
                        result_column=(
                            "major_transition_prevented"
                        ),
                        eligibility_column=(
                            "eligible_major_transition"
                        ),
                    )
                ),
                "minor_transition_prevented_count": int(
                    frame[
                        "minor_transition_prevented"
                    ].sum()
                ),
                "minor_transition_prevented_rate": (
                    _conditional_rate(
                        frame,
                        result_column=(
                            "minor_transition_prevented"
                        ),
                        eligibility_column=(
                            "eligible_minor_transition"
                        ),
                    )
                ),
                "major_assignment_changed_rate": (
                    available[
                        "major_assignment_changed"
                    ].mean()
                    if not available.empty
                    else np.nan
                ),
                "minor_assignment_changed_rate": (
                    available[
                        "minor_assignment_changed"
                    ].mean()
                    if not available.empty
                    else np.nan
                ),
                "mean_regime_strength": (
                    available[
                        "regime_strength"
                    ].mean()
                    if not available.empty
                    else np.nan
                ),
            }
        )

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "major_transition_prevented_rate",
                "minor_transition_prevented_rate",
            ],
            ascending=[
                False,
                False,
            ],
            na_position="last",
        )
        .reset_index(drop=True)
    )


def _build_segmented_sensitivity_summary(
    sensitivity: pd.DataFrame,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    full = sensitivity.copy()
    full["history_segment"] = (
        "all_available_history"
    )
    frames.append(full)

    mature = sensitivity[
        sensitivity[
            "mature_history_flag"
        ]
    ].copy()

    mature["history_segment"] = (
        "mature_history"
    )
    frames.append(mature)

    combined = pd.concat(
        frames,
        ignore_index=True,
    )

    summaries: list[pd.DataFrame] = []

    for history_segment, frame in (
        combined.groupby(
            "history_segment"
        )
    ):
        summary = _sensitivity_summary(
            frame
        )

        summary[
            "history_segment"
        ] = history_segment

        summaries.append(summary)

        for geo_id, geo_frame in (
            frame.groupby("geo_id")
        ):
            geo_summary = (
                _sensitivity_summary(
                    geo_frame
                )
            )

            geo_summary[
                "history_segment"
            ] = history_segment

            geo_summary["geo_id"] = (
                geo_id
            )

            summaries.append(
                geo_summary
            )

    result = pd.concat(
        summaries,
        ignore_index=True,
    )

    if "geo_id" not in result.columns:
        result["geo_id"] = pd.NA

    result["geo_id"] = result[
        "geo_id"
    ].fillna("all_geographies")

    return result.sort_values(
        [
            "history_segment",
            "geo_id",
            "major_transition_prevented_rate",
            "minor_transition_prevented_rate",
        ],
        ascending=[
            True,
            True,
            False,
            False,
        ],
        na_position="last",
    ).reset_index(drop=True)


def build_transition_sensitivity_audit(
    run_id: str = DEFAULT_RUN_ID,
    *,
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: list[str] | None = None,
    weight_multiplier: float = (
        DEFAULT_WEIGHT_MULTIPLIER
    ),
) -> dict[str, pd.DataFrame]:
    if not 0 < weight_multiplier < 1:
        raise ValueError(
            "weight_multiplier must be between "
            "zero and one"
        )

    if geo_ids is None:
        geo_ids = (
            DEFAULT_AUDIT_GEOS.copy()
        )

    contribution = (
        build_axis_contribution_audit(
            run_id=run_id,
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    chronology = (
        build_chronological_axis_review(
            run_id=run_id,
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    timeline = chronology[
        "monthly_timeline"
    ].copy()

    transitions = _transition_events(
        timeline
    )

    dimension_panel = _dimension_panel(
        contribution[
            "axis_contributions"
        ]
    )

    metric_panel = _metric_panel(
        contribution[
            "metric_contributions"
        ]
    )

    coordinates = (
        _counterfactual_coordinates(
            transitions,
            timeline,
            dimension_panel,
            metric_panel,
            weight_multiplier=(
                weight_multiplier
            ),
        )
    )

    assigned = (
        _assign_counterfactual_labels(
            coordinates
        )
    )

    sensitivity = (
        _evaluate_transition_sensitivity(
            assigned,
            transitions,
        )
    )

    persistence = (
        _transition_persistence(
            timeline
        )
    )

    summary = (
        _build_segmented_sensitivity_summary(
            sensitivity
        )
    )

    dwell_times = (
        _build_regime_dwell_times(
            timeline
        )
    )

    price_affordability = (
        sensitivity[
            sensitivity[
                "target_key"
            ].isin(
                {
                    "price",
                    "affordability",
                    "median_sale_price",
                    "median_ppsf",
                    "price_to_income",
                    "payment_burden",
                }
            )
        ]
        .sort_values(
            [
                "geo_id",
                "date",
                "target_level",
                "target_key",
                "scenario",
            ]
        )
        .reset_index(drop=True)
    )

    return {
        "transition_events": transitions,
        "counterfactual_coordinates": (
            coordinates
        ),
        "transition_sensitivity": (
            sensitivity
        ),
        "sensitivity_summary": summary,
        "transition_persistence": (
            persistence
        ),
        "price_affordability_sensitivity": (
            price_affordability
        ),
        "regime_dwell_times": (
            dwell_times
        ),
    }
