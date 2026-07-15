from __future__ import annotations
# regime/experiments/demand_visual_review.py

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.experiments.core_demand_dimension_diagnostic import (
    FOCUS_GEOS,
    build_core_demand_dimension_diagnostic,
)
from regime.experiments.labor_demand_source_diagnostic import (
    build_labor_demand_source_diagnostic,
)


DEFAULT_OUTPUT_ROOT = Path(
    "artifacts/regime/comparisons/"
    "demand_visual_review"
)

CORE_DEMAND_METRICS = (
    "employment",
    "labor_force",
    "laus_unemployment_rate",
    "gdp_annual",
    "median_household_income",
    "population",
)

LABOR_METRICS = (
    "employment",
    "labor_force",
    "laus_unemployment_rate",
)

GEO_LABELS = {
    "alameda_county_ca__county": "Alameda County",
    "district_of_columbia_dc__county": "District of Columbia",
}


def _ensure_output_root(
    output_root: str | Path,
) -> Path:
    path = Path(output_root)
    path.mkdir(
        parents=True,
        exist_ok=True,
    )
    return path


def _safe_slug(
    value: str,
) -> str:
    return (
        value.strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
    )


def _build_core_demand_contributions(
    *,
    artifact_root: str | Path,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    diagnostic = (
        build_core_demand_dimension_diagnostic(
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    contributions = diagnostic[
        "metric_contributions"
    ].copy()

    dimension_history = diagnostic[
        "dimension_history"
    ].copy()

    required_contribution_columns = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "metric_score",
        "metric_weight",
        "effective_metric_weight",
        "weighted_metric_contribution",
    }

    missing_contribution_columns = (
        required_contribution_columns
        - set(contributions.columns)
    )

    if missing_contribution_columns:
        raise ValueError(
            "Core Demand metric contributions "
            "are missing required columns: "
            f"{sorted(missing_contribution_columns)}"
        )

    required_dimension_columns = {
        "geo_id",
        "date",
        "dimension",
        "dimension_score",
    }

    missing_dimension_columns = (
        required_dimension_columns
        - set(dimension_history.columns)
    )

    if missing_dimension_columns:
        raise ValueError(
            "Core Demand dimension history is "
            "missing required columns: "
            f"{sorted(missing_dimension_columns)}"
        )

    contributions["date"] = pd.to_datetime(
        contributions["date"],
        errors="coerce",
    )

    dimension_history["date"] = pd.to_datetime(
        dimension_history["date"],
        errors="coerce",
    )

    if contributions["date"].isna().any():
        raise ValueError(
            "Core Demand contributions contain "
            "invalid dates"
        )

    if dimension_history["date"].isna().any():
        raise ValueError(
            "Core Demand dimension history contains "
            "invalid dates"
        )

    dimension_history = dimension_history[
        dimension_history[
            "dimension"
        ].eq("demand")
        & dimension_history[
            "geo_id"
        ].isin(geo_ids)
    ][
        [
            "geo_id",
            "date",
            "dimension_score",
        ]
    ].copy()

    duplicates = dimension_history.duplicated(
        subset=[
            "geo_id",
            "date",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Core Demand dimension history is not "
            "unique by geo/date:\n"
            + dimension_history.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    contributions = contributions[
        contributions[
            "canonical_metric_key"
        ].isin(
            CORE_DEMAND_METRICS
        )
    ].copy()

    if contributions.empty:
        raise ValueError(
            "Core Demand diagnostic returned no "
            "metric-level contribution rows"
        )

    contributions = contributions.merge(
        dimension_history,
        on=[
            "geo_id",
            "date",
        ],
        how="left",
        validate="many_to_one",
    )

    missing_dimension_scores = contributions[
        contributions[
            "dimension_score"
        ].isna()
    ]

    if not missing_dimension_scores.empty:
        raise ValueError(
            "Metric contributions are missing their "
            "core Demand dimension score:\n"
            + missing_dimension_scores[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                ]
            ].head(30).to_string(
                index=False
            )
        )

    return (
        contributions.sort_values(
            [
                "geo_id",
                "date",
                "canonical_metric_key",
            ]
        )
        .reset_index(
            drop=True
        )
    )


def _load_labor_visual_panel(
    *,
    artifact_root: str | Path,
    geo_ids: tuple[str, ...],
) -> pd.DataFrame:
    diagnostic = (
        build_labor_demand_source_diagnostic(
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    source_panel = diagnostic[
        "source_panel"
    ].copy()

    current = diagnostic[
        "current_feature_history"
    ].copy()

    current = current[
        current[
            "feature_component"
        ].eq("short")
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "raw_feature_value",
        ]
    ].rename(
        columns={
            "raw_feature_value": (
                "current_short"
            ),
        }
    )

    panel = source_panel.merge(
        current,
        on=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        how="left",
        validate="one_to_one",
    )

    required_candidates = {
        "ma3_momentum_lag3",
        "ma6_momentum_lag3",
        "ma12_momentum_lag3",
    }

    missing = required_candidates - set(
        panel.columns
    )

    if missing:
        raise ValueError(
            "Labor source diagnostic is missing "
            f"candidate columns: {sorted(missing)}"
        )

    return panel


def _plot_core_demand_contributions(
    contributions: pd.DataFrame,
    *,
    output_root: Path,
    geo_id: str,
    start_date: str | None,
) -> Path:
    frame = contributions[
        contributions[
            "geo_id"
        ].eq(geo_id)
    ].copy()

    if start_date is not None:
        frame = frame[
            frame[
                "date"
            ].ge(
                pd.Timestamp(
                    start_date
                )
            )
        ].copy()

    if frame.empty:
        raise ValueError(
            f"No core Demand contributions for {geo_id}"
        )

    wide = frame.pivot(
        index="date",
        columns="canonical_metric_key",
        values="weighted_metric_contribution",
    ).sort_index()

    missing_metrics = (
        set(CORE_DEMAND_METRICS)
        - set(wide.columns)
    )

    if missing_metrics:
        raise ValueError(
            f"{geo_id} is missing core Demand metrics: "
            f"{sorted(missing_metrics)}"
        )

    dimension_score = (
        frame[
            [
                "date",
                "dimension_score",
            ]
        ]
        .drop_duplicates(
            subset=[
                "date",
            ]
        )
        .set_index("date")
        .sort_index()
    )

    figure, axis = plt.subplots(
        figsize=(
            14,
            7,
        )
    )

    positive = wide.clip(
        lower=0
    )

    negative = wide.clip(
        upper=0
    )

    axis.stackplot(
        positive.index,
        [
            positive[column]
            for column
            in CORE_DEMAND_METRICS
        ],
        labels=[
            f"{column} (+)"
            for column
            in CORE_DEMAND_METRICS
        ],
        alpha=0.55,
    )

    axis.stackplot(
        negative.index,
        [
            negative[column]
            for column
            in CORE_DEMAND_METRICS
        ],
        labels=[
            f"{column} (-)"
            for column
            in CORE_DEMAND_METRICS
        ],
        alpha=0.55,
    )

    axis.plot(
        dimension_score.index,
        dimension_score[
            "dimension_score"
        ],
        linewidth=2.2,
        label=(
            "Core Demand dimension"
        ),
    )

    axis.axhline(
        0.0,
        linewidth=1.0,
    )

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
        "Core Demand Metric Contributions"
    )

    axis.set_xlabel(
        "Date"
    )

    axis.set_ylabel(
        "Weighted contribution"
    )

    axis.legend(
        loc="upper left",
        ncol=2,
        fontsize=8,
    )

    figure.tight_layout()

    path = (
        output_root
        / f"{_safe_slug(geo_id)}__core_demand_contributions.png"
    )

    figure.savefig(
        path,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(
        figure
    )

    return path


def _plot_labor_short_comparison(
    panel: pd.DataFrame,
    *,
    output_root: Path,
    geo_id: str,
    metric_key: str,
    start_date: str | None,
) -> Path:
    frame = panel[
        panel[
            "geo_id"
        ].eq(geo_id)
        & panel[
            "canonical_metric_key"
        ].eq(metric_key)
    ].copy()

    if start_date is not None:
        frame = frame[
            frame[
                "date"
            ].ge(
                pd.Timestamp(
                    start_date
                )
            )
        ].copy()

    frame = frame.sort_values(
        "date"
    )

    if frame.empty:
        raise ValueError(
            f"No labor visual rows for "
            f"{geo_id}/{metric_key}"
        )

    figure, axis = plt.subplots(
        figsize=(
            14,
            6,
        )
    )

    axis.plot(
        frame["date"],
        frame[
            "current_short"
        ],
        linewidth=1.7,
        label=(
            "Current production short"
        ),
    )

    axis.plot(
        frame["date"],
        frame[
            "ma3_momentum_lag3"
        ],
        linewidth=1.6,
        label="MA3 / lag3",
    )

    axis.plot(
        frame["date"],
        frame[
            "ma6_momentum_lag3"
        ],
        linewidth=1.6,
        label="MA6 / lag3",
    )

    axis.plot(
        frame["date"],
        frame[
            "ma12_momentum_lag3"
        ],
        linewidth=1.4,
        label="MA12 / lag3",
    )

    axis.axhline(
        0.0,
        linewidth=1.0,
    )

    axis.set_title(
        f"{GEO_LABELS.get(geo_id, geo_id)} — "
        f"{metric_key} Short-Feature Comparison"
    )

    axis.set_xlabel(
        "Date"
    )

    axis.set_ylabel(
        "Raw short feature"
    )

    axis.legend(
        loc="upper left",
    )

    figure.tight_layout()

    path = (
        output_root
        / (
            f"{_safe_slug(geo_id)}__"
            f"{_safe_slug(metric_key)}__"
            "short_comparison.png"
        )
    )

    figure.savefig(
        path,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(
        figure
    )

    return path


def _build_cancellation_panel(
    contributions: pd.DataFrame,
) -> pd.DataFrame:
    wide = contributions.pivot(
        index=[
            "geo_id",
            "date",
        ],
        columns="canonical_metric_key",
        values="weighted_metric_contribution",
    ).reset_index()

    contribution_columns = [
        column
        for column
        in CORE_DEMAND_METRICS
        if column in wide.columns
    ]

    wide[
        "gross_contribution_magnitude"
    ] = wide[
        contribution_columns
    ].abs().sum(
        axis=1,
        min_count=1,
    )

    wide[
        "net_contribution"
    ] = wide[
        contribution_columns
    ].sum(
        axis=1,
        min_count=1,
    )

    wide[
        "cancellation_amount"
    ] = (
        wide[
            "gross_contribution_magnitude"
        ]
        - wide[
            "net_contribution"
        ].abs()
    )

    wide[
        "cancellation_rate"
    ] = np.where(
        wide[
            "gross_contribution_magnitude"
        ].gt(0),
        wide[
            "cancellation_amount"
        ]
        / wide[
            "gross_contribution_magnitude"
        ],
        0.0,
    )

    return wide.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(
        drop=True
    )


def _plot_cancellation(
    cancellation_panel: pd.DataFrame,
    *,
    output_root: Path,
    geo_id: str,
    start_date: str | None,
) -> Path:
    frame = cancellation_panel[
        cancellation_panel[
            "geo_id"
        ].eq(geo_id)
    ].copy()

    if start_date is not None:
        frame = frame[
            frame[
                "date"
            ].ge(
                pd.Timestamp(
                    start_date
                )
            )
        ].copy()

    figure, axis = plt.subplots(
        figsize=(
            14,
            5,
        )
    )

    axis.plot(
        frame["date"],
        frame[
            "cancellation_rate"
        ],
        linewidth=1.8,
        label=(
            "Core Demand cancellation rate"
        ),
    )

    axis.axhline(
        0.50,
        linewidth=0.9,
        linestyle="--",
    )

    axis.axhline(
        0.90,
        linewidth=0.9,
        linestyle="--",
    )

    axis.set_ylim(
        0.0,
        1.02,
    )

    axis.set_title(
        f"{GEO_LABELS.get(geo_id, geo_id)} — "
        "Core Demand Internal Cancellation"
    )

    axis.set_xlabel(
        "Date"
    )

    axis.set_ylabel(
        "Cancellation rate"
    )

    axis.legend(
        loc="upper left",
    )

    figure.tight_layout()

    path = (
        output_root
        / f"{_safe_slug(geo_id)}__core_demand_cancellation.png"
    )

    figure.savefig(
        path,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(
        figure
    )

    return path


def build_demand_visual_review(
    *,
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    output_root: str | Path = (
        DEFAULT_OUTPUT_ROOT
    ),
    geo_ids: tuple[str, ...] = (
        FOCUS_GEOS
    ),
    start_date: str | None = (
        "2018-01-01"
    ),
) -> dict[str, object]:
    output_path = _ensure_output_root(
        output_root
    )

    contributions = (
        _build_core_demand_contributions(
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    labor_panel = (
        _load_labor_visual_panel(
            artifact_root=artifact_root,
            geo_ids=geo_ids,
        )
    )

    cancellation_panel = (
        _build_cancellation_panel(
            contributions
        )
    )

    contribution_csv = (
        output_path
        / "core_demand_metric_contributions.csv"
    )

    labor_csv = (
        output_path
        / "labor_short_feature_panel.csv"
    )

    cancellation_csv = (
        output_path
        / "core_demand_cancellation_panel.csv"
    )

    contributions.to_csv(
        contribution_csv,
        index=False,
    )

    labor_panel.to_csv(
        labor_csv,
        index=False,
    )

    cancellation_panel.to_csv(
        cancellation_csv,
        index=False,
    )

    plot_paths: list[
        Path
    ] = []

    for geo_id in geo_ids:
        plot_paths.append(
            _plot_core_demand_contributions(
                contributions,
                output_root=output_path,
                geo_id=geo_id,
                start_date=start_date,
            )
        )

        plot_paths.append(
            _plot_cancellation(
                cancellation_panel,
                output_root=output_path,
                geo_id=geo_id,
                start_date=start_date,
            )
        )

        for metric_key in LABOR_METRICS:
            plot_paths.append(
                _plot_labor_short_comparison(
                    labor_panel,
                    output_root=output_path,
                    geo_id=geo_id,
                    metric_key=metric_key,
                    start_date=start_date,
                )
            )

    return {
        "output_root": output_path,
        "plot_paths": plot_paths,
        "contribution_csv": contribution_csv,
        "labor_csv": labor_csv,
        "cancellation_csv": cancellation_csv,
        "plot_count": len(
            plot_paths
        ),
    }
