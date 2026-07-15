from __future__ import annotations
# regime/experiments/labor_demand_chronological_review.py

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.experiments.labor_demand_comparison import (
    FOCUS_GEOS,
    LABOR_METRICS,
    build_labor_demand_comparison,
)

DEFAULT_OUTPUT_ROOT = Path(
    "artifacts/regime/comparisons/labor_demand_chronological"
)

RUN_ORDER = (
    "baseline",
    "labor_ma3_momentum_lag3",
    "labor_ma6_momentum_lag3",
)

RUN_LABELS = {
    "baseline": "Baseline",
    "labor_ma3_momentum_lag3": "MA3 lag3",
    "labor_ma6_momentum_lag3": "MA6 lag3",
}

GEO_LABELS = {
    "alameda_county_ca__county": "Alameda County",
    "district_of_columbia_dc__county": "District of Columbia",
}

HISTORICAL_WINDOWS = (
    (
        "pandemic_collapse",
        pd.Timestamp("2020-02-29"),
        pd.Timestamp("2020-12-31"),
    ),
    (
        "recovery",
        pd.Timestamp("2021-01-31"),
        pd.Timestamp("2022-12-31"),
    ),
    (
        "normalization",
        pd.Timestamp("2023-01-31"),
        pd.Timestamp("2023-12-31"),
    ),
    (
        "recent_weakening",
        pd.Timestamp("2024-01-31"),
        pd.Timestamp("2026-12-31"),
    ),
)

DECISION_START_DATE = pd.Timestamp(
    "2019-01-31"
)

EVENT_MATCH_WINDOW_MONTHS = 12

FOCUSED_EVENT_HALF_WINDOW_MONTHS = 3

FOCUSED_EVENT_COUNT_PER_GEO = 12


def _ensure_output_root(output_root: str | Path) -> Path:
    path = Path(output_root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_slug(value: str) -> str:
    return (
        value.strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
    )


def _validate_history(
    frame: pd.DataFrame,
    *,
    value_column: str,
    entity_column: str,
    expected_entity: str,
) -> pd.DataFrame:
    required = {
        "run_role",
        "geo_id",
        "date",
        entity_column,
        value_column,
    }
    missing = required - set(frame.columns)

    if missing:
        raise ValueError(
            "Chronological history is missing "
            f"columns: {sorted(missing)}"
        )

    work = frame[
        frame[entity_column].eq(expected_entity)
    ].copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )
    work[value_column] = pd.to_numeric(
        work[value_column],
        errors="coerce",
    )

    invalid = work[
        work["date"].isna()
        | work[value_column].isna()
        | ~np.isfinite(work[value_column])
    ]

    if not invalid.empty:
        raise ValueError(
            "Chronological history contains invalid rows:\n"
            + invalid.head(30).to_string(index=False)
        )

    duplicates = work.duplicated(
        subset=["run_role", "geo_id", "date"],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Chronological history is not unique "
            "by run/geo/date:\n"
            + work.loc[duplicates].head(30).to_string(index=False)
        )

    return work.sort_values(
        ["geo_id", "run_role", "date"]
    ).reset_index(drop=True)


def _build_complete_labor_coverage(
    metric_history: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "run_role",
        "geo_id",
        "date",
        "canonical_metric_key",
        "metric_score",
    }

    missing = required - set(
        metric_history.columns
    )

    if missing:
        raise ValueError(
            "Metric history is missing columns "
            "required for coverage filtering: "
            f"{sorted(missing)}"
        )

    work = metric_history[
        metric_history[
            "run_role"
        ].isin(
            RUN_ORDER
        )
        & metric_history[
            "canonical_metric_key"
        ].isin(
            LABOR_METRICS
        )
    ].copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["metric_score"] = pd.to_numeric(
        work["metric_score"],
        errors="coerce",
    )

    work = work[
        work["date"].notna()
        & work["metric_score"].notna()
        & np.isfinite(
            work["metric_score"]
        )
    ].copy()

    counts = (
        work.groupby(
            [
                "run_role",
                "geo_id",
                "date",
            ]
        )[
            "canonical_metric_key"
        ]
        .nunique()
        .reset_index(
            name="labor_metric_count"
        )
    )

    complete_by_run = counts[
        counts[
            "labor_metric_count"
        ].eq(
            len(LABOR_METRICS)
        )
    ][
        [
            "run_role",
            "geo_id",
            "date",
        ]
    ]

    run_counts = (
        complete_by_run.groupby(
            [
                "geo_id",
                "date",
            ]
        )[
            "run_role"
        ]
        .nunique()
        .reset_index(
            name="complete_run_count"
        )
    )

    complete = run_counts[
        run_counts[
            "complete_run_count"
        ].eq(
            len(RUN_ORDER)
        )
    ][
        [
            "geo_id",
            "date",
        ]
    ].copy()

    if complete.empty:
        raise AssertionError(
            "No months have complete three-metric "
            "labor coverage across baseline, MA3, "
            "and MA6"
        )

    return complete.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(
        drop=True
    )
    

def _common_coverage_panel(
    history: pd.DataFrame,
    complete_coverage: pd.DataFrame,
    *,
    value_column: str,
    output_prefix: str,
) -> pd.DataFrame:
    wide = history.pivot(
        index=[
            "geo_id",
            "date",
        ],
        columns="run_role",
        values=value_column,
    ).reset_index()

    missing_runs = (
        set(RUN_ORDER)
        - set(wide.columns)
    )

    if missing_runs:
        raise ValueError(
            "Chronological history is missing "
            f"run roles: {sorted(missing_runs)}"
        )

    common = wide.merge(
        complete_coverage,
        on=[
            "geo_id",
            "date",
        ],
        how="inner",
        validate="one_to_one",
    )

    common = common.dropna(
        subset=list(RUN_ORDER)
    ).copy()

    common = common.rename(
        columns={
            run_role: (
                f"{output_prefix}_{run_role}"
            )
            for run_role in RUN_ORDER
        }
    )

    return common.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(
        drop=True
    )


def _sign_with_neutral_band(
    values: pd.Series,
    *,
    neutral_threshold: float = 0.0,
) -> pd.Series:
    numeric = pd.to_numeric(
        values,
        errors="coerce",
    )

    return pd.Series(
        np.where(
            numeric.gt(neutral_threshold),
            1,
            np.where(
                numeric.lt(-neutral_threshold),
                -1,
                0,
            ),
        ),
        index=values.index,
        dtype=int,
    )


def _build_sign_change_events(
    panel: pd.DataFrame,
    *,
    value_prefix: str,
    series_name: str,
    neutral_threshold: float = 0.0,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for geo_id, geo_frame in panel.groupby("geo_id"):
        geo_frame = geo_frame.sort_values(
            "date"
        ).reset_index(drop=True)

        for run_role in RUN_ORDER:
            value_column = f"{value_prefix}_{run_role}"
            sign = _sign_with_neutral_band(
                geo_frame[value_column],
                neutral_threshold=neutral_threshold,
            )
            prior_sign = sign.shift(1)

            changed = (
                prior_sign.notna()
                & sign.ne(prior_sign)
                & sign.ne(0)
                & prior_sign.ne(0)
            )

            for index in geo_frame.index[changed]:
                previous = int(prior_sign.loc[index])
                current = int(sign.loc[index])

                rows.append(
                    {
                        "geo_id": geo_id,
                        "series_name": series_name,
                        "run_role": run_role,
                        "date": geo_frame.loc[index, "date"],
                        "previous_sign": previous,
                        "new_sign": current,
                        "transition": (
                            "negative_to_positive"
                            if previous < 0 and current > 0
                            else "positive_to_negative"
                        ),
                        "value": float(
                            geo_frame.loc[index, value_column]
                        ),
                    }
                )

    return pd.DataFrame(rows)


def _month_difference(
    later: pd.Timestamp,
    earlier: pd.Timestamp,
) -> int:
    return (
        (
            later.year
            - earlier.year
        )
        * 12
        + (
            later.month
            - earlier.month
        )
    )


def _match_group_one_to_one(
    baseline_group: pd.DataFrame,
    challenger_group: pd.DataFrame,
    *,
    maximum_lag_months: int,
) -> pd.DataFrame:
    baseline_group = (
        baseline_group.sort_values(
            "date"
        )
        .reset_index(
            drop=True
        )
    )

    challenger_group = (
        challenger_group.sort_values(
            "date"
        )
        .reset_index(
            drop=True
        )
    )

    candidate_pairs: list[
        dict[str, object]
    ] = []

    for challenger_index, challenger in (
        challenger_group.iterrows()
    ):
        for baseline_index, baseline in (
            baseline_group.iterrows()
        ):
            lag_months = _month_difference(
                challenger["date"],
                baseline["date"],
            )

            absolute_lag = abs(
                lag_months
            )

            if (
                absolute_lag
                <= maximum_lag_months
            ):
                candidate_pairs.append(
                    {
                        "challenger_index": (
                            challenger_index
                        ),
                        "baseline_index": (
                            baseline_index
                        ),
                        "lag_months": (
                            lag_months
                        ),
                        "absolute_lag_months": (
                            absolute_lag
                        ),
                        "challenger_date": (
                            challenger["date"]
                        ),
                        "baseline_date": (
                            baseline["date"]
                        ),
                    }
                )

    candidate_pairs = sorted(
        candidate_pairs,
        key=lambda row: (
            row[
                "absolute_lag_months"
            ],
            row[
                "challenger_date"
            ],
            row[
                "baseline_date"
            ],
        ),
    )

    used_challengers: set[int] = set()
    used_baselines: set[int] = set()

    selected: dict[
        int,
        dict[str, object],
    ] = {}

    for pair in candidate_pairs:
        challenger_index = int(
            pair[
                "challenger_index"
            ]
        )

        baseline_index = int(
            pair[
                "baseline_index"
            ]
        )

        if (
            challenger_index
            in used_challengers
            or baseline_index
            in used_baselines
        ):
            continue

        used_challengers.add(
            challenger_index
        )

        used_baselines.add(
            baseline_index
        )

        selected[
            challenger_index
        ] = pair

    rows: list[
        dict[str, object]
    ] = []

    for challenger_index, challenger in (
        challenger_group.iterrows()
    ):
        match = selected.get(
            challenger_index
        )

        if match is None:
            rows.append(
                {
                    **challenger.to_dict(),
                    "baseline_event_date": (
                        pd.NaT
                    ),
                    "lag_months": np.nan,
                    "absolute_lag_months": (
                        np.nan
                    ),
                    "matched_within_limit": (
                        False
                    ),
                }
            )

            continue

        rows.append(
            {
                **challenger.to_dict(),
                "baseline_event_date": (
                    match[
                        "baseline_date"
                    ]
                ),
                "lag_months": int(
                    match[
                        "lag_months"
                    ]
                ),
                "absolute_lag_months": int(
                    match[
                        "absolute_lag_months"
                    ]
                ),
                "matched_within_limit": True,
            }
        )

    return pd.DataFrame(
        rows
    )


def _match_events_to_baseline(
    events: pd.DataFrame,
    *,
    maximum_lag_months: int = (
        EVENT_MATCH_WINDOW_MONTHS
    ),
) -> pd.DataFrame:
    baseline = events[
        events[
            "run_role"
        ].eq(
            "baseline"
        )
    ].copy()

    challengers = events[
        ~events[
            "run_role"
        ].eq(
            "baseline"
        )
    ].copy()

    frames: list[
        pd.DataFrame
    ] = []

    grouping_columns = [
        "geo_id",
        "series_name",
        "transition",
        "run_role",
    ]

    for keys, challenger_group in (
        challengers.groupby(
            grouping_columns
        )
    ):
        (
            geo_id,
            series_name,
            transition,
            _run_role,
        ) = keys

        baseline_group = baseline[
            baseline[
                "geo_id"
            ].eq(
                geo_id
            )
            & baseline[
                "series_name"
            ].eq(
                series_name
            )
            & baseline[
                "transition"
            ].eq(
                transition
            )
        ].copy()

        frames.append(
            _match_group_one_to_one(
                baseline_group,
                challenger_group,
                maximum_lag_months=(
                    maximum_lag_months
                ),
            )
        )

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
    ).sort_values(
        [
            "geo_id",
            "series_name",
            "run_role",
            "date",
        ]
    ).reset_index(
        drop=True
    )


def _build_monthly_disagreement_panel(
    dimension_panel: pd.DataFrame,
    axis_panel: pd.DataFrame,
) -> pd.DataFrame:
    panel = dimension_panel.merge(
        axis_panel,
        on=["geo_id", "date"],
        how="inner",
        validate="one_to_one",
    )

    for series_name, prefix in (
        ("dimension", "dimension_score"),
        ("axis", "axis_score"),
    ):
        baseline_column = f"{prefix}_baseline"

        for run_role in (
            "labor_ma3_momentum_lag3",
            "labor_ma6_momentum_lag3",
        ):
            challenger_column = f"{prefix}_{run_role}"
            short_name = (
                "ma3" if "ma3" in run_role else "ma6"
            )

            panel[
                f"{series_name}_{short_name}_delta"
            ] = (
                panel[challenger_column]
                - panel[baseline_column]
            )

            panel[
                f"{series_name}_{short_name}_sign_diff"
            ] = (
                np.sign(panel[challenger_column])
                != np.sign(panel[baseline_column])
            )

    return panel.sort_values(
        ["geo_id", "date"]
    ).reset_index(drop=True)


def _build_window_summary(
    panel: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for window_name, start_date, end_date in HISTORICAL_WINDOWS:
        focus = panel[
            panel["date"].between(
                start_date,
                end_date,
                inclusive="both",
            )
        ].copy()

        if focus.empty:
            continue

        for geo_id, geo_frame in focus.groupby("geo_id"):
            for series_name in ("dimension", "axis"):
                for short_name in ("ma3", "ma6"):
                    delta_column = (
                        f"{series_name}_{short_name}_delta"
                    )
                    sign_column = (
                        f"{series_name}_{short_name}_sign_diff"
                    )

                    rows.append(
                        {
                            "window": window_name,
                            "geo_id": geo_id,
                            "series_name": series_name,
                            "challenger": short_name,
                            "rows": len(geo_frame),
                            "mean_delta": (
                                geo_frame[delta_column].mean()
                            ),
                            "mean_absolute_delta": (
                                geo_frame[
                                    delta_column
                                ].abs().mean()
                            ),
                            "p90_absolute_delta": (
                                geo_frame[
                                    delta_column
                                ].abs().quantile(0.90)
                            ),
                            "sign_disagreement_rate": (
                                geo_frame[sign_column].mean()
                            ),
                        }
                    )

    return pd.DataFrame(rows)


def _build_largest_disagreements(
    panel: pd.DataFrame,
    *,
    rows_per_geo: int = 20,
    start_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    work = panel.copy()

    if start_date is not None:
        work = work[
            work[
                "date"
            ].ge(
                start_date
            )
        ].copy()

    work[
        "ma6_joint_absolute_delta"
    ] = (
        work[
            "dimension_ma6_delta"
        ].abs()
        + work[
            "axis_ma6_delta"
        ].abs()
    )

    return (
        work.sort_values(
            [
                "geo_id",
                "ma6_joint_absolute_delta",
                "date",
            ],
            ascending=[
                True,
                False,
                True,
            ],
        )
        .groupby(
            "geo_id",
            as_index=False,
        )
        .head(
            rows_per_geo
        )
        .reset_index(
            drop=True
        )
    )


def _build_focused_event_windows(
    monthly_panel: pd.DataFrame,
    *,
    half_window_months: int = (
        FOCUSED_EVENT_HALF_WINDOW_MONTHS
    ),
    rows_per_geo: int = (
        FOCUSED_EVENT_COUNT_PER_GEO
    ),
) -> pd.DataFrame:
    events = _build_largest_disagreements(
        monthly_panel,
        rows_per_geo=rows_per_geo,
        start_date=DECISION_START_DATE,
    )

    frames: list[
        pd.DataFrame
    ] = []

    for _, event in events.iterrows():
        event_date = pd.Timestamp(
            event["date"]
        )

        start_date = (
            event_date
            - pd.DateOffset(
                months=half_window_months
            )
        )

        end_date = (
            event_date
            + pd.DateOffset(
                months=half_window_months
            )
        )

        focus = monthly_panel[
            monthly_panel[
                "geo_id"
            ].eq(
                event["geo_id"]
            )
            & monthly_panel[
                "date"
            ].between(
                start_date,
                end_date,
                inclusive="both",
            )
        ].copy()

        focus[
            "event_date"
        ] = event_date

        focus[
            "event_joint_absolute_delta"
        ] = event[
            "ma6_joint_absolute_delta"
        ]

        frames.append(
            focus
        )

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
    ).sort_values(
        [
            "geo_id",
            "event_date",
            "date",
        ]
    ).reset_index(
        drop=True
    )


def _plot_series(
    panel: pd.DataFrame,
    *,
    geo_id: str,
    value_prefix: str,
    title_suffix: str,
    y_label: str,
    output_path: Path,
) -> None:
    frame = panel[
        panel["geo_id"].eq(geo_id)
    ].sort_values("date")

    figure, axis = plt.subplots(
        figsize=(14, 6)
    )

    for run_role in RUN_ORDER:
        axis.plot(
            frame["date"],
            frame[f"{value_prefix}_{run_role}"],
            linewidth=(
                2.0 if run_role == "baseline" else 1.6
            ),
            label=RUN_LABELS[run_role],
        )

    axis.axhline(0.0, linewidth=1.0)
    axis.axhline(
        0.10,
        linewidth=0.8,
        linestyle="--",
    )
    axis.axhline(
        -0.10,
        linewidth=0.8,
        linestyle="--",
    )

    axis.set_title(
        f"{GEO_LABELS.get(geo_id, geo_id)} — "
        f"{title_suffix}"
    )
    axis.set_xlabel("Date")
    axis.set_ylabel(y_label)
    axis.legend(loc="upper left")

    figure.tight_layout()
    figure.savefig(
        output_path,
        dpi=160,
        bbox_inches="tight",
    )
    plt.close(figure)


def _plot_ma6_delta(
    disagreement_panel: pd.DataFrame,
    *,
    geo_id: str,
    output_path: Path,
) -> None:
    frame = disagreement_panel[
        disagreement_panel["geo_id"].eq(geo_id)
    ].sort_values("date")

    figure, axis = plt.subplots(
        figsize=(14, 5)
    )

    axis.plot(
        frame["date"],
        frame["dimension_ma6_delta"],
        linewidth=1.7,
        label="Core Demand dimension delta",
    )
    axis.plot(
        frame["date"],
        frame["axis_ma6_delta"],
        linewidth=1.7,
        label="Demand-axis delta",
    )

    axis.axhline(0.0, linewidth=1.0)
    axis.set_title(
        f"{GEO_LABELS.get(geo_id, geo_id)} — "
        "MA6 Minus Baseline"
    )
    axis.set_xlabel("Date")
    axis.set_ylabel("Score delta")
    axis.legend(loc="upper left")

    figure.tight_layout()
    figure.savefig(
        output_path,
        dpi=160,
        bbox_inches="tight",
    )
    plt.close(figure)


def build_labor_demand_chronological_review(
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    geo_ids: tuple[str, ...] = FOCUS_GEOS,
) -> dict[str, object]:
    output_path = _ensure_output_root(
        output_root
    )

    comparison = build_labor_demand_comparison(
        artifact_root=artifact_root,
        geo_ids=geo_ids,
    )

    complete_coverage = (
        _build_complete_labor_coverage(
            comparison[
                "metric_score_history"
            ]
        )
    )

    dimension_history = _validate_history(
        comparison["dimension_score_history"],
        value_column="dimension_score",
        entity_column="dimension",
        expected_entity="demand",
    )

    axis_history = _validate_history(
        comparison["axis_score_history"],
        value_column="axis_score",
        entity_column="axis",
        expected_entity="demand",
    )

    dimension_panel = (
        _common_coverage_panel(
            dimension_history,
            complete_coverage,
            value_column="dimension_score",
            output_prefix="dimension_score",
        )
    )
    
    axis_panel = (
        _common_coverage_panel(
            axis_history,
            complete_coverage,
            value_column="axis_score",
            output_prefix="axis_score",
        )
    )

    monthly_panel = _build_monthly_disagreement_panel(
        dimension_panel,
        axis_panel,
    )

    sign_change_events = pd.concat(
        [
            _build_sign_change_events(
                dimension_panel,
                value_prefix="dimension_score",
                series_name="core_demand_dimension",
            ),
            _build_sign_change_events(
                axis_panel,
                value_prefix="axis_score",
                series_name="demand_axis",
            ),
        ],
        ignore_index=True,
    ).sort_values(
        [
            "geo_id",
            "series_name",
            "date",
            "run_role",
        ]
    ).reset_index(drop=True)

    event_matches = _match_events_to_baseline(
        sign_change_events
    )

    matched_event_summary = (
        event_matches[
            event_matches[
                "matched_within_limit"
            ]
        ]
        .groupby(
            [
                "geo_id",
                "series_name",
                "run_role",
            ],
            dropna=False,
        )
        .agg(
            matched_events=(
                "date",
                "size",
            ),
            mean_lag_months=(
                "lag_months",
                "mean",
            ),
            median_lag_months=(
                "lag_months",
                "median",
            ),
            mean_absolute_lag_months=(
                "absolute_lag_months",
                "mean",
            ),
            median_absolute_lag_months=(
                "absolute_lag_months",
                "median",
            ),
            maximum_absolute_lag_months=(
                "absolute_lag_months",
                "max",
            ),
        )
        .reset_index()
    )

    
    window_summary = _build_window_summary(
        monthly_panel
    )

    largest_disagreements_full = (
        _build_largest_disagreements(
            monthly_panel
        )
    )
    
    decision_disagreements = (
        _build_largest_disagreements(
            monthly_panel,
            start_date=DECISION_START_DATE,
        )
    )
    
    focused_event_windows = (
        _build_focused_event_windows(
            monthly_panel
        )
    )

    csv_outputs = {
        "complete_coverage": (
            output_path
            / "complete_labor_coverage.csv"
        ),
        "monthly_panel": (
            output_path
            / "monthly_panel.csv"
        ),
        "sign_change_events": (
            output_path
            / "sign_change_events.csv"
        ),
        "event_matches": (
            output_path
            / "event_matches.csv"
        ),
        "matched_event_summary": (
            output_path
            / "matched_event_summary.csv"
        ),
        "window_summary": (
            output_path
            / "window_summary.csv"
        ),
        "largest_disagreements_full": (
            output_path
            / "largest_disagreements_full_history.csv"
        ),
        "decision_disagreements": (
            output_path
            / "largest_disagreements_post_2019.csv"
        ),
        "focused_event_windows": (
            output_path
            / "focused_event_windows.csv"
        ),
    }

    monthly_panel.to_csv(
        csv_outputs["monthly_panel"],
        index=False,
    )
    sign_change_events.to_csv(
        csv_outputs["sign_change_events"],
        index=False,
    )
    event_matches.to_csv(
        csv_outputs["event_matches"],
        index=False,
    )
    window_summary.to_csv(
        csv_outputs["window_summary"],
        index=False,
    )
    complete_coverage.to_csv(
        csv_outputs[
            "complete_coverage"
        ],
        index=False,
    )
    
    matched_event_summary.to_csv(
        csv_outputs[
            "matched_event_summary"
        ],
        index=False,
    )
    
    largest_disagreements_full.to_csv(
        csv_outputs[
            "largest_disagreements_full"
        ],
        index=False,
    )
    
    decision_disagreements.to_csv(
        csv_outputs[
            "decision_disagreements"
        ],
        index=False,
    )
    
    focused_event_windows.to_csv(
        csv_outputs[
            "focused_event_windows"
        ],
        index=False,
    )

    plot_paths: list[Path] = []

    for geo_id in geo_ids:
        dimension_plot = (
            output_path
            / (
                f"{_safe_slug(geo_id)}__"
                "core_demand_dimension.png"
            )
        )
        axis_plot = (
            output_path
            / (
                f"{_safe_slug(geo_id)}__"
                "demand_axis.png"
            )
        )
        delta_plot = (
            output_path
            / (
                f"{_safe_slug(geo_id)}__"
                "ma6_delta.png"
            )
        )

        _plot_series(
            dimension_panel,
            geo_id=geo_id,
            value_prefix="dimension_score",
            title_suffix=(
                "Core Demand Dimension Chronology"
            ),
            y_label="Dimension score",
            output_path=dimension_plot,
        )
        _plot_series(
            axis_panel,
            geo_id=geo_id,
            value_prefix="axis_score",
            title_suffix="Demand Axis Chronology",
            y_label="Axis score",
            output_path=axis_plot,
        )
        _plot_ma6_delta(
            monthly_panel,
            geo_id=geo_id,
            output_path=delta_plot,
        )

        plot_paths.extend(
            [
                dimension_plot,
                axis_plot,
                delta_plot,
            ]
        )

    return {
        "output_root": output_path,
        "plot_paths": plot_paths,
        "csv_outputs": csv_outputs,
        "dimension_panel": dimension_panel,
        "axis_panel": axis_panel,
        "monthly_panel": monthly_panel,
        "sign_change_events": sign_change_events,
        "event_matches": event_matches,
        "window_summary": window_summary,
        "complete_coverage": complete_coverage,
        "matched_event_summary": matched_event_summary,
        "largest_disagreements_full": largest_disagreements_full,
        "decision_disagreements": decision_disagreements,
        "focused_event_windows": focused_event_windows,
    }
