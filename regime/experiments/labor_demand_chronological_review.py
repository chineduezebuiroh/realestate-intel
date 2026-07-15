from __future__ import annotations
# regime/experiments/labor_demand_chronological_review.py

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.experiments.labor_demand_comparison import (
    FOCUS_GEOS,
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


def _common_coverage_panel(
    history: pd.DataFrame,
    *,
    value_column: str,
    output_prefix: str,
) -> pd.DataFrame:
    wide = history.pivot(
        index=["geo_id", "date"],
        columns="run_role",
        values=value_column,
    ).reset_index()

    missing_runs = set(RUN_ORDER) - set(wide.columns)

    if missing_runs:
        raise ValueError(
            "Chronological history is missing "
            f"run roles: {sorted(missing_runs)}"
        )

    common = wide.dropna(
        subset=list(RUN_ORDER)
    ).copy()

    common = common.rename(
        columns={
            run_role: f"{output_prefix}_{run_role}"
            for run_role in RUN_ORDER
        }
    )

    return common.sort_values(
        ["geo_id", "date"]
    ).reset_index(drop=True)


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


def _match_events_to_baseline(
    events: pd.DataFrame,
    *,
    maximum_lag_months: int = 12,
) -> pd.DataFrame:
    baseline = events[
        events["run_role"].eq("baseline")
    ].copy()

    challengers = events[
        ~events["run_role"].eq("baseline")
    ].copy()

    rows: list[dict[str, object]] = []

    for _, challenger in challengers.iterrows():
        candidates = baseline[
            baseline["geo_id"].eq(challenger["geo_id"])
            & baseline["series_name"].eq(
                challenger["series_name"]
            )
            & baseline["transition"].eq(
                challenger["transition"]
            )
        ].copy()

        if candidates.empty:
            rows.append(
                {
                    **challenger.to_dict(),
                    "baseline_event_date": pd.NaT,
                    "lag_months": np.nan,
                    "absolute_lag_months": np.nan,
                    "matched_within_limit": False,
                }
            )
            continue

        candidates["lag_months"] = (
            (
                challenger["date"].year
                - candidates["date"].dt.year
            )
            * 12
            + (
                challenger["date"].month
                - candidates["date"].dt.month
            )
        )
        candidates["absolute_lag_months"] = (
            candidates["lag_months"].abs()
        )

        best = candidates.sort_values(
            ["absolute_lag_months", "date"]
        ).iloc[0]

        absolute_lag = int(
            best["absolute_lag_months"]
        )

        rows.append(
            {
                **challenger.to_dict(),
                "baseline_event_date": best["date"],
                "lag_months": int(best["lag_months"]),
                "absolute_lag_months": absolute_lag,
                "matched_within_limit": (
                    absolute_lag <= maximum_lag_months
                ),
            }
        )

    return pd.DataFrame(rows)


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
) -> pd.DataFrame:
    work = panel.copy()
    work["ma6_joint_absolute_delta"] = (
        work["dimension_ma6_delta"].abs()
        + work["axis_ma6_delta"].abs()
    )

    return (
        work.sort_values(
            [
                "geo_id",
                "ma6_joint_absolute_delta",
                "date",
            ],
            ascending=[True, False, True],
        )
        .groupby("geo_id", as_index=False)
        .head(rows_per_geo)
        .reset_index(drop=True)
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

    dimension_panel = _common_coverage_panel(
        dimension_history,
        value_column="dimension_score",
        output_prefix="dimension_score",
    )

    axis_panel = _common_coverage_panel(
        axis_history,
        value_column="axis_score",
        output_prefix="axis_score",
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
    window_summary = _build_window_summary(
        monthly_panel
    )
    largest_disagreements = _build_largest_disagreements(
        monthly_panel
    )

    csv_outputs = {
        "monthly_panel": (
            output_path / "monthly_panel.csv"
        ),
        "sign_change_events": (
            output_path / "sign_change_events.csv"
        ),
        "event_matches": (
            output_path / "event_matches.csv"
        ),
        "window_summary": (
            output_path / "window_summary.csv"
        ),
        "largest_disagreements": (
            output_path / "largest_disagreements.csv"
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
    largest_disagreements.to_csv(
        csv_outputs["largest_disagreements"],
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
        "largest_disagreements": (
            largest_disagreements
        ),
    }
