from __future__ import annotations
# regime/experiments/inventory_finalist_comparison.py

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)
from regime.diagnostics.transition_sensitivity import (
    build_transition_sensitivity_audit,
)


BASELINE_RUN_ID = (
    "macro_regime_v1_bps120_sources"
)

CHALLENGER_RUN_ID = (
    "inventory_ma3_deviation"
)

TARGET_METRIC = "active_inventory"

TARGET_DIMENSION = "supply"

TARGET_AXIS = "supply"

FOCUS_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

HORIZONS = (
    1,
    3,
    6,
)

HISTORICAL_PERIODS = (
    (
        "2009_2012",
        pd.Timestamp("2009-01-01"),
        pd.Timestamp("2012-12-31"),
    ),
    (
        "2013_2019",
        pd.Timestamp("2013-01-01"),
        pd.Timestamp("2019-12-31"),
    ),
    (
        "2020_2021",
        pd.Timestamp("2020-01-01"),
        pd.Timestamp("2021-12-31"),
    ),
    (
        "2022_rate_shock",
        pd.Timestamp("2022-01-01"),
        pd.Timestamp("2022-12-31"),
    ),
    (
        "2023_2026",
        pd.Timestamp("2023-01-01"),
        pd.Timestamp("2026-12-31"),
    ),
)


def _date_column(
    frame: pd.DataFrame,
) -> str:
    for column in (
        "date",
        "evaluation_date",
        "metric_date",
    ):
        if column in frame.columns:
            return column

    raise ValueError(
        "Could not resolve a date column from "
        f"{list(frame.columns)}"
    )


def _standardize_date(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    work = frame.copy()

    source_date_column = _date_column(
        work
    )

    if source_date_column != "date":
        if "date" in work.columns:
            raise ValueError(
                "Cannot standardize date because "
                "both date and another date column exist"
            )

        work = work.rename(
            columns={
                source_date_column: "date",
            }
        )

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    if work["date"].isna().any():
        raise ValueError(
            "Artifact contains invalid dates"
        )

    return work


def _sort_frame(
    frame: pd.DataFrame,
    preferred_keys: list[str],
) -> pd.DataFrame:
    keys = [
        column
        for column in preferred_keys
        if column in frame.columns
    ]

    if not keys:
        raise ValueError(
            "No valid sort keys were found. "
            f"Requested={preferred_keys}, "
            f"available={list(frame.columns)}"
        )

    return (
        frame.sort_values(
            keys,
            kind="mergesort",
        )
        .reset_index(drop=True)
    )


def _exact_parity_result(
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
    *,
    artifact_name: str,
    comparison_scope: str,
    sort_keys: list[str],
) -> dict[str, object]:
    baseline_sorted = _sort_frame(
        baseline,
        sort_keys,
    )

    challenger_sorted = _sort_frame(
        challenger,
        sort_keys,
    )

    error_message = ""

    try:
        pd.testing.assert_frame_equal(
            baseline_sorted,
            challenger_sorted,
            check_dtype=True,
            check_exact=True,
            check_like=False,
        )

        matches = True

    except AssertionError as exc:
        matches = False
        error_message = str(exc)[:4000]

    return {
        "artifact_name": artifact_name,
        "comparison_scope": (
            comparison_scope
        ),
        "baseline_rows": len(
            baseline_sorted
        ),
        "challenger_rows": len(
            challenger_sorted
        ),
        "exact_match": matches,
        "error_message": error_message,
    }


def _build_isolation_audit(
    store: RegimeArtifactStore,
    *,
    baseline_run_id: str,
    challenger_run_id: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    baseline_normalized = (
        store.read_dataframe(
            baseline_run_id,
            "normalized_features",
        )
    )

    challenger_normalized = (
        store.read_dataframe(
            challenger_run_id,
            "normalized_features",
        )
    )

    baseline_non_target = (
        baseline_normalized[
            ~baseline_normalized[
                "canonical_metric_key"
            ].eq(TARGET_METRIC)
        ].copy()
    )

    challenger_non_target = (
        challenger_normalized[
            ~challenger_normalized[
                "canonical_metric_key"
            ].eq(TARGET_METRIC)
        ].copy()
    )

    rows.append(
        _exact_parity_result(
            baseline_non_target,
            challenger_non_target,
            artifact_name=(
                "normalized_features"
            ),
            comparison_scope=(
                "all_non_active_inventory"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "canonical_metric_key",
                "feature_key",
            ],
        )
    )

    baseline_metrics = (
        store.read_dataframe(
            baseline_run_id,
            "metric_scores",
        )
    )

    challenger_metrics = (
        store.read_dataframe(
            challenger_run_id,
            "metric_scores",
        )
    )

    rows.append(
        _exact_parity_result(
            baseline_metrics[
                ~baseline_metrics[
                    "canonical_metric_key"
                ].eq(TARGET_METRIC)
            ].copy(),
            challenger_metrics[
                ~challenger_metrics[
                    "canonical_metric_key"
                ].eq(TARGET_METRIC)
            ].copy(),
            artifact_name="metric_scores",
            comparison_scope=(
                "all_non_active_inventory"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "canonical_metric_key",
            ],
        )
    )

    baseline_dimensions = (
        _standardize_date(
            store.read_dataframe(
                baseline_run_id,
                "dimension_scores",
            )
        )
    )

    challenger_dimensions = (
        _standardize_date(
            store.read_dataframe(
                challenger_run_id,
                "dimension_scores",
            )
        )
    )

    rows.append(
        _exact_parity_result(
            baseline_dimensions[
                ~baseline_dimensions[
                    "dimension"
                ].eq(TARGET_DIMENSION)
            ].copy(),
            challenger_dimensions[
                ~challenger_dimensions[
                    "dimension"
                ].eq(TARGET_DIMENSION)
            ].copy(),
            artifact_name="dimension_scores",
            comparison_scope=(
                "all_non_supply_dimensions"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "dimension",
            ],
        )
    )

    baseline_axes = _standardize_date(
        store.read_dataframe(
            baseline_run_id,
            "axis_scores",
        )
    )

    challenger_axes = _standardize_date(
        store.read_dataframe(
            challenger_run_id,
            "axis_scores",
        )
    )

    rows.append(
        _exact_parity_result(
            baseline_axes[
                ~baseline_axes[
                    "axis"
                ].eq(TARGET_AXIS)
            ].copy(),
            challenger_axes[
                ~challenger_axes[
                    "axis"
                ].eq(TARGET_AXIS)
            ].copy(),
            artifact_name="axis_scores",
            comparison_scope=(
                "all_non_supply_axes"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "axis",
            ],
        )
    )

    return pd.DataFrame(rows)


def _add_monthly_change(
    frame: pd.DataFrame,
    *,
    value_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    work = _standardize_date(
        frame
    )

    work[value_column] = pd.to_numeric(
        work[value_column],
        errors="coerce",
    )

    work = work.sort_values(
        [
            *group_columns,
            "date",
        ]
    ).reset_index(drop=True)

    grouped = work.groupby(
        group_columns,
        group_keys=False,
    )

    work[
        f"{value_column}_change_1m"
    ] = grouped[
        value_column
    ].diff()

    work[
        f"absolute_{value_column}_change_1m"
    ] = work[
        f"{value_column}_change_1m"
    ].abs()

    return work


def _stability_summary(
    frame: pd.DataFrame,
    *,
    value_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    absolute_change_column = (
        f"absolute_{value_column}_change_1m"
    )

    return (
        frame.groupby(
            group_columns,
            dropna=False,
        )
        .agg(
            rows=(value_column, "count"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            mean_value=(
                value_column,
                "mean",
            ),
            value_std=(
                value_column,
                "std",
            ),
            mean_absolute_change_1m=(
                absolute_change_column,
                "mean",
            ),
            median_absolute_change_1m=(
                absolute_change_column,
                "median",
            ),
            p90_absolute_change_1m=(
                absolute_change_column,
                lambda values: values.quantile(
                    0.90
                ),
            ),
            maximum_absolute_change_1m=(
                absolute_change_column,
                "max",
            ),
        )
        .reset_index()
    )


def _comparison_vs_baseline(
    summary: pd.DataFrame,
    *,
    identity_columns: list[str],
) -> pd.DataFrame:
    baseline = summary[
        summary["run_role"].eq(
            "baseline"
        )
    ].copy()

    challenger = summary[
        summary["run_role"].eq(
            "challenger"
        )
    ].copy()

    baseline = baseline.drop(
        columns=[
            "run_id",
            "run_role",
        ]
    )

    baseline = baseline.rename(
        columns={
            column: f"baseline_{column}"
            for column in baseline.columns
            if column
            not in identity_columns
        }
    )

    output = challenger.merge(
        baseline,
        on=identity_columns,
        how="left",
        validate="one_to_one",
    )

    for column in (
        "value_std",
        "mean_absolute_change_1m",
        "p90_absolute_change_1m",
        "maximum_absolute_change_1m",
    ):
        baseline_column = (
            f"baseline_{column}"
        )

        output[
            f"{column}_pct_vs_baseline"
        ] = np.where(
            output[
                baseline_column
            ].ne(0),
            (
                output[column]
                / output[
                    baseline_column
                ]
                - 1.0
            ),
            np.nan,
        )

    return output


def _build_metric_comparison(
    store: RegimeArtifactStore,
    *,
    baseline_run_id: str,
    challenger_run_id: str,
    geo_ids: list[str],
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    frames: list[pd.DataFrame] = []

    for run_role, run_id in (
        (
            "baseline",
            baseline_run_id,
        ),
        (
            "challenger",
            challenger_run_id,
        ),
    ):
        frame = store.read_dataframe(
            run_id,
            "metric_scores",
        )

        frame = frame[
            frame[
                "canonical_metric_key"
            ].eq(TARGET_METRIC)
            & frame[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        frame["run_id"] = run_id
        frame["run_role"] = run_role

        frames.append(frame)

    history = pd.concat(
        frames,
        ignore_index=True,
    )

    history = _add_monthly_change(
        history,
        value_column="metric_score",
        group_columns=[
            "run_id",
            "run_role",
            "geo_id",
        ],
    )

    summary = _stability_summary(
        history,
        value_column="metric_score",
        group_columns=[
            "run_id",
            "run_role",
            "geo_id",
        ],
    )

    comparison = (
        _comparison_vs_baseline(
            summary,
            identity_columns=[
                "geo_id",
            ],
        )
    )

    return (
        history,
        comparison,
    )


def _build_dimension_comparison(
    store: RegimeArtifactStore,
    *,
    baseline_run_id: str,
    challenger_run_id: str,
    geo_ids: list[str],
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    frames: list[pd.DataFrame] = []

    for run_role, run_id in (
        (
            "baseline",
            baseline_run_id,
        ),
        (
            "challenger",
            challenger_run_id,
        ),
    ):
        frame = _standardize_date(
            store.read_dataframe(
                run_id,
                "dimension_scores",
            )
        )

        frame = frame[
            frame["dimension"].eq(
                TARGET_DIMENSION
            )
            & frame[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        frame["run_id"] = run_id
        frame["run_role"] = run_role

        frames.append(frame)

    history = pd.concat(
        frames,
        ignore_index=True,
    )

    history = _add_monthly_change(
        history,
        value_column="dimension_score",
        group_columns=[
            "run_id",
            "run_role",
            "geo_id",
            "dimension",
        ],
    )

    summary = _stability_summary(
        history,
        value_column="dimension_score",
        group_columns=[
            "run_id",
            "run_role",
            "geo_id",
            "dimension",
        ],
    )

    comparison = (
        _comparison_vs_baseline(
            summary,
            identity_columns=[
                "geo_id",
                "dimension",
            ],
        )
    )

    return (
        history,
        comparison,
    )


def _build_axis_comparison(
    store: RegimeArtifactStore,
    *,
    baseline_run_id: str,
    challenger_run_id: str,
    geo_ids: list[str],
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    frames: list[pd.DataFrame] = []

    for run_role, run_id in (
        (
            "baseline",
            baseline_run_id,
        ),
        (
            "challenger",
            challenger_run_id,
        ),
    ):
        frame = _standardize_date(
            store.read_dataframe(
                run_id,
                "axis_scores",
            )
        )

        frame = frame[
            frame["axis"].eq(
                TARGET_AXIS
            )
            & frame[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        frame["run_id"] = run_id
        frame["run_role"] = run_role

        frames.append(frame)

    history = pd.concat(
        frames,
        ignore_index=True,
    )

    history = _add_monthly_change(
        history,
        value_column="axis_score",
        group_columns=[
            "run_id",
            "run_role",
            "geo_id",
            "axis",
        ],
    )

    summary = _stability_summary(
        history,
        value_column="axis_score",
        group_columns=[
            "run_id",
            "run_role",
            "geo_id",
            "axis",
        ],
    )

    comparison = (
        _comparison_vs_baseline(
            summary,
            identity_columns=[
                "geo_id",
                "axis",
            ],
        )
    )

    return (
        history,
        comparison,
    )


def _prepare_assignments(
    assignments: pd.DataFrame,
    *,
    run_id: str,
    run_role: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    work = _standardize_date(
        assignments
    )

    work = work[
        work["geo_id"].isin(
            geo_ids
        )
    ].copy()

    work = work.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(drop=True)

    grouped = work.groupby(
        "geo_id",
        group_keys=False,
    )

    work[
        "previous_major_regime"
    ] = grouped[
        "major_regime"
    ].shift(1)

    work[
        "previous_minor_regime"
    ] = grouped[
        "minor_regime"
    ].shift(1)

    work["major_changed"] = (
        work[
            "previous_major_regime"
        ].notna()
        & work[
            "major_regime"
        ].ne(
            work[
                "previous_major_regime"
            ]
        )
    )

    work["minor_changed"] = (
        work[
            "previous_minor_regime"
        ].notna()
        & work[
            "minor_regime"
        ].ne(
            work[
                "previous_minor_regime"
            ]
        )
    )

    work["run_id"] = run_id
    work["run_role"] = run_role

    return work


def _transition_summary(
    assignment_history: pd.DataFrame,
) -> pd.DataFrame:
    return (
        assignment_history.groupby(
            [
                "run_id",
                "run_role",
                "geo_id",
            ]
        )
        .agg(
            rows=("date", "size"),
            major_transitions=(
                "major_changed",
                "sum",
            ),
            minor_transitions=(
                "minor_changed",
                "sum",
            ),
            any_transition_months=(
                "date",
                lambda values: (
                    assignment_history.loc[
                        values.index,
                        "major_changed",
                    ]
                    | assignment_history.loc[
                        values.index,
                        "minor_changed",
                    ]
                ).sum(),
            ),
            mean_regime_strength=(
                "regime_strength",
                "mean",
            ),
            median_regime_strength=(
                "regime_strength",
                "median",
            ),
            mean_boundary_distance=(
                (
                    "distance_to_boundary_"
                    "degrees"
                ),
                "mean",
            ),
        )
        .reset_index()
    )


def _build_assignment_comparison(
    assignment_history: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    baseline = assignment_history[
        assignment_history[
            "run_role"
        ].eq("baseline")
    ].copy()

    challenger = assignment_history[
        assignment_history[
            "run_role"
        ].eq("challenger")
    ].copy()

    merged = baseline.merge(
        challenger,
        on=[
            "geo_id",
            "date",
        ],
        how="outer",
        suffixes=(
            "_baseline",
            "_challenger",
        ),
        indicator=True,
        validate="one_to_one",
    )

    merged[
        "major_assignment_changed"
    ] = (
        merged["_merge"].eq("both")
        & merged[
            "major_regime_baseline"
        ].ne(
            merged[
                "major_regime_challenger"
            ]
        )
    )

    merged[
        "minor_assignment_changed"
    ] = (
        merged["_merge"].eq("both")
        & merged[
            "minor_regime_baseline"
        ].ne(
            merged[
                "minor_regime_challenger"
            ]
        )
    )

    merged[
        "supply_score_delta"
    ] = (
        merged[
            (
                "supply_pressure_score_"
                "challenger"
            )
        ]
        - merged[
            (
                "supply_pressure_score_"
                "baseline"
            )
        ]
    )

    merged[
        "demand_score_delta"
    ] = (
        merged[
            (
                "demand_strength_score_"
                "challenger"
            )
        ]
        - merged[
            (
                "demand_strength_score_"
                "baseline"
            )
        ]
    )

    merged[
        "regime_strength_delta"
    ] = (
        merged[
            "regime_strength_challenger"
        ]
        - merged[
            "regime_strength_baseline"
        ]
    )

    summary = (
        merged.groupby(
            "geo_id"
        )
        .agg(
            merged_rows=(
                "_merge",
                lambda values: values.eq(
                    "both"
                ).sum(),
            ),
            baseline_only_rows=(
                "_merge",
                lambda values: values.eq(
                    "left_only"
                ).sum(),
            ),
            challenger_only_rows=(
                "_merge",
                lambda values: values.eq(
                    "right_only"
                ).sum(),
            ),
            major_assignment_changes=(
                "major_assignment_changed",
                "sum",
            ),
            minor_assignment_changes=(
                "minor_assignment_changed",
                "sum",
            ),
            major_assignment_change_rate=(
                "major_assignment_changed",
                "mean",
            ),
            minor_assignment_change_rate=(
                "minor_assignment_changed",
                "mean",
            ),
            mean_absolute_supply_score_delta=(
                "supply_score_delta",
                lambda values: values.abs().mean(),
            ),
            p90_absolute_supply_score_delta=(
                "supply_score_delta",
                lambda values: values.abs().quantile(
                    0.90
                ),
            ),
            mean_absolute_demand_score_delta=(
                "demand_score_delta",
                lambda values: values.abs().mean(),
            ),
            mean_absolute_regime_strength_delta=(
                "regime_strength_delta",
                lambda values: values.abs().mean(),
            ),
        )
        .reset_index()
    )

    return (
        merged,
        summary,
    )


def _build_continuous_persistence(
    assignment_history: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    rows: list[dict[str, object]] = []

    for (
        run_id,
        run_role,
        geo_id,
    ), geo_frame in (
        assignment_history.groupby(
            [
                "run_id",
                "run_role",
                "geo_id",
            ]
        )
    ):
        geo_frame = (
            geo_frame.sort_values(
                "date"
            )
            .reset_index(drop=True)
        )

        for index, event in (
            geo_frame.iterrows()
        ):
            if not (
                event["major_changed"]
                or event["minor_changed"]
            ):
                continue

            row: dict[str, object] = {
                "run_id": run_id,
                "run_role": run_role,
                "geo_id": geo_id,
                "date": event["date"],
                "major_changed": (
                    bool(
                        event[
                            "major_changed"
                        ]
                    )
                ),
                "minor_changed": (
                    bool(
                        event[
                            "minor_changed"
                        ]
                    )
                ),
                "major_regime": (
                    event["major_regime"]
                ),
                "minor_regime": (
                    event["minor_regime"]
                ),
            }

            for horizon in HORIZONS:
                future = geo_frame.iloc[
                    index + 1:
                    index + horizon + 1
                ]

                available = (
                    len(future)
                    == horizon
                )

                row[
                    (
                        "major_horizon_"
                        f"available_{horizon}m"
                    )
                ] = available

                row[
                    (
                        "minor_horizon_"
                        f"available_{horizon}m"
                    )
                ] = available

                row[
                    (
                        "major_continuously_"
                        f"persists_{horizon}m"
                    )
                ] = bool(
                    available
                    and event[
                        "major_changed"
                    ]
                    and future[
                        "major_regime"
                    ].eq(
                        event[
                            "major_regime"
                        ]
                    ).all()
                )

                row[
                    (
                        "minor_continuously_"
                        f"persists_{horizon}m"
                    )
                ] = bool(
                    available
                    and event[
                        "minor_changed"
                    ]
                    and future[
                        "minor_regime"
                    ].eq(
                        event[
                            "minor_regime"
                        ]
                    ).all()
                )

            rows.append(row)

    events = pd.DataFrame(rows)

    summary_rows: list[
        dict[str, object]
    ] = []

    for (
        run_id,
        run_role,
        geo_id,
    ), frame in events.groupby(
        [
            "run_id",
            "run_role",
            "geo_id",
        ]
    ):
        for regime_level in (
            "major",
            "minor",
        ):
            changed_column = (
                f"{regime_level}_changed"
            )

            for horizon in HORIZONS:
                available_column = (
                    f"{regime_level}_horizon_"
                    f"available_{horizon}m"
                )

                persistence_column = (
                    f"{regime_level}_"
                    "continuously_persists_"
                    f"{horizon}m"
                )

                eligible = frame[
                    frame[
                        changed_column
                    ]
                    & frame[
                        available_column
                    ]
                ]

                summary_rows.append(
                    {
                        "run_id": run_id,
                        "run_role": run_role,
                        "geo_id": geo_id,
                        "regime_level": (
                            regime_level
                        ),
                        "horizon_months": (
                            horizon
                        ),
                        "eligible_transitions": (
                            len(eligible)
                        ),
                        (
                            "continuous_"
                            "persistence_rate"
                        ): (
                            eligible[
                                persistence_column
                            ].mean()
                            if not eligible.empty
                            else np.nan
                        ),
                    }
                )

    return (
        events,
        pd.DataFrame(
            summary_rows
        ),
    )


def _build_dwell_summary(
    assignment_history: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    rows: list[dict[str, object]] = []

    for (
        run_id,
        run_role,
        geo_id,
    ), geo_frame in (
        assignment_history.groupby(
            [
                "run_id",
                "run_role",
                "geo_id",
            ]
        )
    ):
        geo_frame = (
            geo_frame.sort_values(
                "date"
            )
            .reset_index(drop=True)
        )

        for regime_level in (
            "major",
            "minor",
        ):
            regime_column = (
                f"{regime_level}_regime"
            )

            episode_id = (
                geo_frame[
                    regime_column
                ]
                .ne(
                    geo_frame[
                        regime_column
                    ].shift(1)
                )
                .cumsum()
            )

            episodes = (
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
                    duration_months=(
                        "date",
                        "size",
                    ),
                    mean_regime_strength=(
                        "regime_strength",
                        "mean",
                    ),
                )
            )

            episodes["run_id"] = (
                run_id
            )

            episodes["run_role"] = (
                run_role
            )

            episodes["geo_id"] = geo_id

            episodes[
                "regime_level"
            ] = regime_level

            rows.extend(
                episodes.to_dict(
                    orient="records"
                )
            )

    episodes = pd.DataFrame(rows)

    summary = (
        episodes.groupby(
            [
                "run_id",
                "run_role",
                "geo_id",
                "regime_level",
            ]
        )
        .agg(
            episodes=(
                "episode_id",
                "size",
            ),
            mean_duration_months=(
                "duration_months",
                "mean",
            ),
            median_duration_months=(
                "duration_months",
                "median",
            ),
            p90_duration_months=(
                "duration_months",
                lambda values: values.quantile(
                    0.90
                ),
            ),
            maximum_duration_months=(
                "duration_months",
                "max",
            ),
        )
        .reset_index()
    )

    return (
        episodes,
        summary,
    )


def _recovery_hypersupply_flips(
    assignment_history: pd.DataFrame,
) -> pd.DataFrame:
    valid_pairs = {
        (
            "recovery",
            "hypersupply",
        ),
        (
            "hypersupply",
            "recovery",
        ),
        (
            "recovery",
            "hyper supply",
        ),
        (
            "hyper supply",
            "recovery",
        ),
    }

    work = assignment_history.copy()

    work[
        "previous_major_normalized"
    ] = (
        work[
            "previous_major_regime"
        ]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    work[
        "major_normalized"
    ] = (
        work[
            "major_regime"
        ]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    work[
        "recovery_hypersupply_flip"
    ] = [
        (
            previous,
            current,
        ) in valid_pairs
        for previous, current in zip(
            work[
                "previous_major_normalized"
            ],
            work[
                "major_normalized"
            ],
            strict=True,
        )
    ]

    return (
        work.groupby(
            [
                "run_id",
                "run_role",
                "geo_id",
            ]
        )
        .agg(
            recovery_hypersupply_flips=(
                (
                    "recovery_"
                    "hypersupply_flip"
                ),
                "sum",
            )
        )
        .reset_index()
    )


def _historical_period_summary(
    assignment_history: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for (
        period_name,
        start_date,
        end_date,
    ) in HISTORICAL_PERIODS:
        period = assignment_history[
            assignment_history[
                "date"
            ].between(
                start_date,
                end_date,
                inclusive="both",
            )
        ]

        if period.empty:
            continue

        summary = (
            period.groupby(
                [
                    "run_id",
                    "run_role",
                    "geo_id",
                ]
            )
            .agg(
                rows=("date", "size"),
                major_transitions=(
                    "major_changed",
                    "sum",
                ),
                minor_transitions=(
                    "minor_changed",
                    "sum",
                ),
                mean_supply_score=(
                    "supply_pressure_score",
                    "mean",
                ),
                mean_demand_score=(
                    "demand_strength_score",
                    "mean",
                ),
                mean_regime_strength=(
                    "regime_strength",
                    "mean",
                ),
                median_regime_strength=(
                    "regime_strength",
                    "median",
                ),
            )
            .reset_index()
        )

        summary["period"] = period_name

        rows.extend(
            summary.to_dict(
                orient="records"
            )
        )

    return pd.DataFrame(rows)


def _build_sensitivity_comparison(
    *,
    baseline_run_id: str,
    challenger_run_id: str,
    artifact_root: str | Path,
    geo_ids: list[str],
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    frames: list[pd.DataFrame] = []

    for run_role, run_id in (
        (
            "baseline",
            baseline_run_id,
        ),
        (
            "challenger",
            challenger_run_id,
        ),
    ):
        audit = (
            build_transition_sensitivity_audit(
                run_id=run_id,
                artifact_root=artifact_root,
                geo_ids=geo_ids,
            )
        )

        summary = audit[
            "sensitivity_summary"
        ].copy()

        summary["run_id"] = run_id
        summary["run_role"] = run_role

        frames.append(summary)

    combined = pd.concat(
        frames,
        ignore_index=True,
    )

    focus = combined[
        combined[
            "target_key"
        ].isin(
            {
                "active_inventory",
                "supply",
            }
        )
    ].copy()

    identity_candidates = [
        "geo_id",
        "history_segment",
        "target_level",
        "target_key",
        "scenario",
    ]

    identity_columns = [
        column
        for column in identity_candidates
        if column in focus.columns
    ]

    metric_columns = [
        column
        for column in [
            (
                "major_transition_"
                "prevented_rate"
            ),
            (
                "minor_transition_"
                "prevented_rate"
            ),
            (
                "major_assignment_"
                "changed_rate"
            ),
            (
                "minor_assignment_"
                "changed_rate"
            ),
        ]
        if column in focus.columns
    ]

    baseline = focus[
        focus["run_role"].eq(
            "baseline"
        )
    ][
        [
            *identity_columns,
            *metric_columns,
        ]
    ].copy()

    baseline = baseline.rename(
        columns={
            column: f"baseline_{column}"
            for column in metric_columns
        }
    )

    challenger = focus[
        focus["run_role"].eq(
            "challenger"
        )
    ].copy()

    comparison = challenger.merge(
        baseline,
        on=identity_columns,
        how="left",
        validate="one_to_one",
    )

    for column in metric_columns:
        comparison[
            f"{column}_delta_vs_baseline"
        ] = (
            comparison[column]
            - comparison[
                f"baseline_{column}"
            ]
        )

    return (
        combined,
        comparison,
    )


def build_inventory_finalist_comparison(
    *,
    baseline_run_id: str = (
        BASELINE_RUN_ID
    ),
    challenger_run_id: str = (
        CHALLENGER_RUN_ID
    ),
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    if geo_ids is None:
        geo_ids = FOCUS_GEOS.copy()

    store = RegimeArtifactStore(
        artifact_root
    )

    isolation_audit = (
        _build_isolation_audit(
            store,
            baseline_run_id=(
                baseline_run_id
            ),
            challenger_run_id=(
                challenger_run_id
            ),
        )
    )

    (
        metric_history,
        metric_comparison,
    ) = _build_metric_comparison(
        store,
        baseline_run_id=(
            baseline_run_id
        ),
        challenger_run_id=(
            challenger_run_id
        ),
        geo_ids=geo_ids,
    )

    (
        dimension_history,
        dimension_comparison,
    ) = _build_dimension_comparison(
        store,
        baseline_run_id=(
            baseline_run_id
        ),
        challenger_run_id=(
            challenger_run_id
        ),
        geo_ids=geo_ids,
    )

    (
        axis_history,
        axis_comparison,
    ) = _build_axis_comparison(
        store,
        baseline_run_id=(
            baseline_run_id
        ),
        challenger_run_id=(
            challenger_run_id
        ),
        geo_ids=geo_ids,
    )

    assignment_frames: list[
        pd.DataFrame
    ] = []

    for run_role, run_id in (
        (
            "baseline",
            baseline_run_id,
        ),
        (
            "challenger",
            challenger_run_id,
        ),
    ):
        assignments = (
            store.read_dataframe(
                run_id,
                "regime_assignments",
            )
        )

        assignment_frames.append(
            _prepare_assignments(
                assignments,
                run_id=run_id,
                run_role=run_role,
                geo_ids=geo_ids,
            )
        )

    assignment_history = pd.concat(
        assignment_frames,
        ignore_index=True,
    )

    transition_summary = (
        _transition_summary(
            assignment_history
        )
    )

    (
        assignment_comparison,
        assignment_change_summary,
    ) = _build_assignment_comparison(
        assignment_history
    )

    (
        persistence_events,
        persistence_summary,
    ) = _build_continuous_persistence(
        assignment_history
    )

    (
        dwell_episodes,
        dwell_summary,
    ) = _build_dwell_summary(
        assignment_history
    )

    recovery_hypersupply = (
        _recovery_hypersupply_flips(
            assignment_history
        )
    )

    historical_summary = (
        _historical_period_summary(
            assignment_history
        )
    )

    (
        sensitivity_summary,
        sensitivity_comparison,
    ) = _build_sensitivity_comparison(
        baseline_run_id=(
            baseline_run_id
        ),
        challenger_run_id=(
            challenger_run_id
        ),
        artifact_root=artifact_root,
        geo_ids=geo_ids,
    )

    return {
        "isolation_audit": (
            isolation_audit
        ),
        "metric_history": metric_history,
        "metric_comparison": (
            metric_comparison
        ),
        "dimension_history": (
            dimension_history
        ),
        "dimension_comparison": (
            dimension_comparison
        ),
        "axis_history": axis_history,
        "axis_comparison": (
            axis_comparison
        ),
        "assignment_history": (
            assignment_history
        ),
        "transition_summary": (
            transition_summary
        ),
        "assignment_comparison": (
            assignment_comparison
        ),
        "assignment_change_summary": (
            assignment_change_summary
        ),
        "persistence_events": (
            persistence_events
        ),
        "persistence_summary": (
            persistence_summary
        ),
        "dwell_episodes": dwell_episodes,
        "dwell_summary": dwell_summary,
        "recovery_hypersupply_flips": (
            recovery_hypersupply
        ),
        "historical_period_summary": (
            historical_summary
        ),
        "sensitivity_summary": (
            sensitivity_summary
        ),
        "sensitivity_comparison": (
            sensitivity_comparison
        ),
    }
