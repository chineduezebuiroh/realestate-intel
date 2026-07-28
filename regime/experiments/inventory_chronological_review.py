from __future__ import annotations
# regime/experiments/inventory_chronological_review.py

from pathlib import Path
from typing import Protocol

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)
from regime.experiments.in_memory_challenger import build_in_memory_smoothing_challenger
from regime.review import ReviewResult



BASELINE_RUN_ID = "macro_regime_v1_bps120_sources"
CHALLENGER_RUN_ID = "inventory_ma3_deviation"

FOCUS_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

INVENTORY_FEATURE_KEYS = {
    "redfin_inventory_level": "level",
    "redfin_inventory_short": "short",
    "redfin_inventory_long": "long",
}

DEFAULT_OUTPUT_DIR = Path(
    "artifacts/regime/comparisons/"
    "inventory_ma3_deviation_chronological"
)

EVENT_WINDOW_MONTHS = 3


class ArtifactReader(Protocol):
    """
    Minimal artifact-reading contract required by this review.
    """

    def read_dataframe(
        self,
        run_id: str,
        artifact_name: str,
        *,
        validation: bool = False,
    ) -> pd.DataFrame:
        ...


class OverlayArtifactReader:
    """
    Read the persisted baseline from RegimeArtifactStore while
    serving one logical challenger run from in-memory frames.
    """

    def __init__(
        self,
        *,
        persisted_store: RegimeArtifactStore,
        challenger_run_id: str,
        challenger_artifacts: (
            dict[str, pd.DataFrame]
        ),
    ) -> None:
        if not challenger_run_id:
            raise ValueError(
                "challenger_run_id must be non-empty"
            )

        if not challenger_artifacts:
            raise ValueError(
                "challenger_artifacts must be non-empty"
            )

        self._persisted_store = (
            persisted_store
        )
        self._challenger_run_id = (
            challenger_run_id
        )
        self._challenger_artifacts = {
            name: frame.copy()
            for name, frame
            in challenger_artifacts.items()
        }

    def read_dataframe(
        self,
        run_id: str,
        artifact_name: str,
        *,
        validation: bool = False,
    ) -> pd.DataFrame:
        if run_id != self._challenger_run_id:
            return (
                self._persisted_store
                .read_dataframe(
                    run_id,
                    artifact_name,
                    validation=validation,
                )
            )

        if validation:
            raise ValueError(
                "In-memory challenger validation "
                "artifacts are not available"
            )

        if (
            artifact_name
            not in self._challenger_artifacts
        ):
            available = sorted(
                self._challenger_artifacts
            )

            raise KeyError(
                "In-memory challenger artifact "
                f"not found: {artifact_name}. "
                f"Available artifacts: {available}"
            )

        return self._challenger_artifacts[
            artifact_name
        ].copy()


def _standardize_date(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    work = frame.copy()

    date_candidates = [
        column
        for column in (
            "date",
            "evaluation_date",
            "metric_date",
        )
        if column in work.columns
    ]

    if not date_candidates:
        raise ValueError(
            "Could not resolve a date column from "
            f"{list(work.columns)}"
        )

    source_date = date_candidates[0]

    if source_date != "date":
        if "date" in work.columns:
            raise ValueError(
                "Ambiguous date-column contract"
            )

        work = work.rename(
            columns={
                source_date: "date",
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


def _period_name(
    date: pd.Timestamp,
) -> str:
    if date <= pd.Timestamp("2012-12-31"):
        return "2009_2012"

    if date <= pd.Timestamp("2019-12-31"):
        return "2013_2019"

    if date <= pd.Timestamp("2021-12-31"):
        return "2020_2021"

    if date <= pd.Timestamp("2022-12-31"):
        return "2022_rate_shock"

    return "2023_2026"


def _load_assignments(
    store: ArtifactReader,
    *,
    run_id: str,
    role: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    frame = _standardize_date(
        store.read_dataframe(
            run_id,
            "regime_assignments",
        )
    )

    frame = frame[
        frame["geo_id"].isin(
            geo_ids
        )
    ].copy()

    keep_columns = [
        "geo_id",
        "date",
        "major_regime",
        "minor_regime",
        "supply_pressure_score",
        "demand_strength_score",
        "regime_strength",
        "angle_degrees",
        "distance_to_boundary_degrees",
        "quadrant",
    ]

    missing = (
        set(keep_columns)
        - set(frame.columns)
    )

    if missing:
        raise ValueError(
            "regime_assignments is missing "
            f"columns: {sorted(missing)}"
        )

    frame = frame[
        keep_columns
    ].copy()

    rename = {
        column: f"{column}_{role}"
        for column in keep_columns
        if column
        not in {
            "geo_id",
            "date",
        }
    }

    return frame.rename(
        columns=rename
    )


def _load_inventory_metric_scores(
    store: ArtifactReader,
    *,
    run_id: str,
    role: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    frame = _standardize_date(
        store.read_dataframe(
            run_id,
            "metric_scores",
        )
    )

    frame = frame[
        frame["geo_id"].isin(
            geo_ids
        )
        & frame[
            "canonical_metric_key"
        ].eq("active_inventory")
    ][
        [
            "geo_id",
            "date",
            "metric_score",
            "feature_count",
            "feature_weight_sum",
        ]
    ].copy()

    return frame.rename(
        columns={
            "metric_score": (
                f"inventory_metric_score_{role}"
            ),
            "feature_count": (
                f"inventory_feature_count_{role}"
            ),
            "feature_weight_sum": (
                f"inventory_feature_weight_sum_{role}"
            ),
        }
    )


def _load_supply_dimension(
    store: ArtifactReader,
    *,
    run_id: str,
    role: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    frame = _standardize_date(
        store.read_dataframe(
            run_id,
            "dimension_scores",
        )
    )

    frame = frame[
        frame["geo_id"].isin(
            geo_ids
        )
        & frame["dimension"].eq(
            "supply"
        )
    ][
        [
            "geo_id",
            "date",
            "dimension_score",
        ]
    ].copy()

    return frame.rename(
        columns={
            "dimension_score": (
                f"supply_dimension_score_{role}"
            ),
        }
    )


def _load_axes(
    store: ArtifactReader,
    *,
    run_id: str,
    role: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    frame = _standardize_date(
        store.read_dataframe(
            run_id,
            "axis_scores",
        )
    )

    frame = frame[
        frame["geo_id"].isin(
            geo_ids
        )
        & frame["axis"].isin(
            [
                "supply",
                "demand",
            ]
        )
    ][
        [
            "geo_id",
            "date",
            "axis",
            "axis_score",
        ]
    ].copy()

    pivot = frame.pivot(
        index=[
            "geo_id",
            "date",
        ],
        columns="axis",
        values="axis_score",
    ).reset_index()

    pivot.columns.name = None

    return pivot.rename(
        columns={
            "supply": (
                f"supply_axis_score_{role}"
            ),
            "demand": (
                f"demand_axis_score_{role}"
            ),
        }
    )


def _load_inventory_features(
    store: ArtifactReader,
    *,
    run_id: str,
    role: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    frame = _standardize_date(
        store.read_dataframe(
            run_id,
            "normalized_features",
        )
    )

    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
        "raw_feature_value",
        "percentile",
        "feature_score",
    }

    missing = required - set(
        frame.columns
    )

    if missing:
        raise ValueError(
            "normalized_features is missing "
            f"columns: {sorted(missing)}"
        )

    frame = frame[
        frame["geo_id"].isin(
            geo_ids
        )
        & frame[
            "canonical_metric_key"
        ].eq("active_inventory")
        & frame[
            "feature_key"
        ].isin(
            INVENTORY_FEATURE_KEYS
        )
    ].copy()

    frame[
        "feature_component"
    ] = frame[
        "feature_key"
    ].map(
        INVENTORY_FEATURE_KEYS
    )

    value_frames = []

    for value_column in (
        "raw_feature_value",
        "percentile",
        "feature_score",
    ):
        pivot = frame.pivot(
            index=[
                "geo_id",
                "date",
            ],
            columns="feature_component",
            values=value_column,
        ).reset_index()

        pivot.columns.name = None

        pivot = pivot.rename(
            columns={
                component: (
                    f"inventory_{component}_"
                    f"{value_column}_{role}"
                )
                for component in (
                    "level",
                    "short",
                    "long",
                )
                if component
                in pivot.columns
            }
        )

        value_frames.append(
            pivot
        )

    output = value_frames[0]

    for frame_to_merge in (
        value_frames[1:]
    ):
        output = output.merge(
            frame_to_merge,
            on=[
                "geo_id",
                "date",
            ],
            how="outer",
            validate="one_to_one",
        )

    return output


def _load_raw_inventory(
    store: ArtifactReader,
    *,
    run_id: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    frame = _standardize_date(
        store.read_dataframe(
            run_id,
            "source_metrics",
        )
    )

    frame = frame[
        frame["geo_id"].isin(
            geo_ids
        )
        & frame[
            "canonical_metric_key"
        ].eq("active_inventory")
    ][
        [
            "geo_id",
            "date",
            "value",
        ]
    ].copy()

    frame = frame.rename(
        columns={
            "value": "raw_active_inventory",
        }
    )

    frame = frame.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(drop=True)

    frame[
        "raw_active_inventory_ma3"
    ] = (
        frame.groupby(
            "geo_id"
        )[
            "raw_active_inventory"
        ]
        .transform(
            lambda values: (
                values.rolling(
                    window=3,
                    min_periods=3,
                ).mean()
            )
        )
    )

    frame[
        "raw_active_inventory_ma12"
    ] = (
        frame.groupby(
            "geo_id"
        )[
            "raw_active_inventory"
        ]
        .transform(
            lambda values: (
                values.rolling(
                    window=12,
                    min_periods=12,
                ).mean()
            )
        )
    )

    return frame


def _merge_monthly_panel(
    store: ArtifactReader,
    *,
    baseline_run_id: str,
    challenger_run_id: str,
    geo_ids: list[str],
) -> pd.DataFrame:
    frames = [
        _load_assignments(
            store,
            run_id=baseline_run_id,
            role="baseline",
            geo_ids=geo_ids,
        ),
        _load_assignments(
            store,
            run_id=challenger_run_id,
            role="challenger",
            geo_ids=geo_ids,
        ),
        _load_inventory_metric_scores(
            store,
            run_id=baseline_run_id,
            role="baseline",
            geo_ids=geo_ids,
        ),
        _load_inventory_metric_scores(
            store,
            run_id=challenger_run_id,
            role="challenger",
            geo_ids=geo_ids,
        ),
        _load_supply_dimension(
            store,
            run_id=baseline_run_id,
            role="baseline",
            geo_ids=geo_ids,
        ),
        _load_supply_dimension(
            store,
            run_id=challenger_run_id,
            role="challenger",
            geo_ids=geo_ids,
        ),
        _load_axes(
            store,
            run_id=baseline_run_id,
            role="baseline",
            geo_ids=geo_ids,
        ),
        _load_axes(
            store,
            run_id=challenger_run_id,
            role="challenger",
            geo_ids=geo_ids,
        ),
        _load_inventory_features(
            store,
            run_id=baseline_run_id,
            role="baseline",
            geo_ids=geo_ids,
        ),
        _load_inventory_features(
            store,
            run_id=challenger_run_id,
            role="challenger",
            geo_ids=geo_ids,
        ),
        _load_raw_inventory(
            store,
            run_id=baseline_run_id,
            geo_ids=geo_ids,
        ),
    ]

    panel = frames[0]

    for frame in frames[1:]:
        panel = panel.merge(
            frame,
            on=[
                "geo_id",
                "date",
            ],
            how="outer",
            validate="one_to_one",
        )

    panel = panel.sort_values(
        [
            "geo_id",
            "date",
        ]
    ).reset_index(drop=True)

    panel["period"] = panel[
        "date"
    ].map(_period_name)

    panel[
        "major_assignment_changed"
    ] = (
        panel[
            "major_regime_baseline"
        ].notna()
        & panel[
            "major_regime_challenger"
        ].notna()
        & panel[
            "major_regime_baseline"
        ].ne(
            panel[
                "major_regime_challenger"
            ]
        )
    )

    panel[
        "minor_assignment_changed"
    ] = (
        panel[
            "minor_regime_baseline"
        ].notna()
        & panel[
            "minor_regime_challenger"
        ].notna()
        & panel[
            "minor_regime_baseline"
        ].ne(
            panel[
                "minor_regime_challenger"
            ]
        )
    )

    for variable in (
        "supply_pressure_score",
        "demand_strength_score",
        "regime_strength",
        "distance_to_boundary_degrees",
        "inventory_metric_score",
        "supply_dimension_score",
        "supply_axis_score",
        "demand_axis_score",
    ):
        baseline_column = (
            f"{variable}_baseline"
        )

        challenger_column = (
            f"{variable}_challenger"
        )

        if (
            baseline_column
            in panel.columns
            and challenger_column
            in panel.columns
        ):
            panel[
                f"{variable}_delta"
            ] = (
                panel[
                    challenger_column
                ]
                - panel[
                    baseline_column
                ]
            )

    panel[
        "absolute_demand_axis_baseline"
    ] = panel[
        "demand_axis_score_baseline"
    ].abs()

    panel[
        "absolute_supply_axis_baseline"
    ] = panel[
        "supply_axis_score_baseline"
    ].abs()

    panel[
        "absolute_demand_axis_challenger"
    ] = panel[
        "demand_axis_score_challenger"
    ].abs()

    panel[
        "absolute_supply_axis_challenger"
    ] = panel[
        "supply_axis_score_challenger"
    ].abs()

    panel[
        "demand_to_supply_magnitude_baseline"
    ] = (
        panel[
            "absolute_demand_axis_baseline"
        ]
        / (
            panel[
                "absolute_supply_axis_baseline"
            ]
            + 1e-12
        )
    )

    panel[
        "demand_to_supply_magnitude_challenger"
    ] = (
        panel[
            "absolute_demand_axis_challenger"
        ]
        / (
            panel[
                "absolute_supply_axis_challenger"
            ]
            + 1e-12
        )
    )

    return panel


def _build_changed_months(
    panel: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "geo_id",
        "date",
        "period",
        "major_regime_baseline",
        "major_regime_challenger",
        "minor_regime_baseline",
        "minor_regime_challenger",
        "supply_pressure_score_baseline",
        "supply_pressure_score_challenger",
        "supply_pressure_score_delta",
        "demand_strength_score_baseline",
        "demand_strength_score_challenger",
        "demand_strength_score_delta",
        "regime_strength_baseline",
        "regime_strength_challenger",
        "distance_to_boundary_degrees_baseline",
        "distance_to_boundary_degrees_challenger",
        "inventory_metric_score_baseline",
        "inventory_metric_score_challenger",
        "inventory_metric_score_delta",
        "supply_dimension_score_baseline",
        "supply_dimension_score_challenger",
        "supply_dimension_score_delta",
        "supply_axis_score_baseline",
        "supply_axis_score_challenger",
        "supply_axis_score_delta",
        "demand_axis_score_baseline",
        "demand_axis_score_challenger",
        "demand_axis_score_delta",
        "raw_active_inventory",
        "raw_active_inventory_ma3",
        "raw_active_inventory_ma12",
        (
            "inventory_level_"
            "raw_feature_value_baseline"
        ),
        (
            "inventory_level_"
            "raw_feature_value_challenger"
        ),
        (
            "inventory_short_"
            "raw_feature_value_baseline"
        ),
        (
            "inventory_short_"
            "raw_feature_value_challenger"
        ),
        (
            "inventory_long_"
            "raw_feature_value_baseline"
        ),
        (
            "inventory_long_"
            "raw_feature_value_challenger"
        ),
        (
            "demand_to_supply_"
            "magnitude_baseline"
        ),
        (
            "demand_to_supply_"
            "magnitude_challenger"
        ),
        "major_assignment_changed",
        "minor_assignment_changed",
    ]

    available_columns = [
        column
        for column in columns
        if column in panel.columns
    ]

    return (
        panel[
            panel[
                "major_assignment_changed"
            ]
            | panel[
                "minor_assignment_changed"
            ]
        ][
            available_columns
        ]
        .sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _build_event_windows(
    panel: pd.DataFrame,
    *,
    window_months: int,
) -> pd.DataFrame:
    events = panel[
        panel[
            "major_assignment_changed"
        ]
    ][
        [
            "geo_id",
            "date",
            "major_regime_baseline",
            "major_regime_challenger",
        ]
    ].copy()

    rows = []

    for event in events.itertuples(
        index=False
    ):
        start = (
            event.date
            - pd.DateOffset(
                months=window_months
            )
        )

        end = (
            event.date
            + pd.DateOffset(
                months=window_months
            )
        )

        window = panel[
            panel["geo_id"].eq(
                event.geo_id
            )
            & panel["date"].between(
                start,
                end,
                inclusive="both",
            )
        ].copy()

        window["event_date"] = (
            event.date
        )

        window[
            "event_major_regime_baseline"
        ] = (
            event.major_regime_baseline
        )

        window[
            "event_major_regime_challenger"
        ] = (
            event.major_regime_challenger
        )

        window[
            "months_from_event"
        ] = (
            (
                window["date"].dt.year
                - event.date.year
            )
            * 12
            + (
                window["date"].dt.month
                - event.date.month
            )
        )

        rows.append(window)

    if not rows:
        return pd.DataFrame()

    return pd.concat(
        rows,
        ignore_index=True,
    )


def _build_changed_month_summary(
    panel: pd.DataFrame,
) -> pd.DataFrame:
    work = panel.copy()

    work["assignment_group"] = np.where(
        work[
            "major_assignment_changed"
        ],
        "major_changed",
        np.where(
            work[
                "minor_assignment_changed"
            ],
            "minor_only_changed",
            "unchanged",
        ),
    )

    return (
        work.groupby(
            [
                "geo_id",
                "period",
                "assignment_group",
            ],
            dropna=False,
        )
        .agg(
            months=("date", "size"),
            mean_absolute_supply_baseline=(
                "absolute_supply_axis_baseline",
                "mean",
            ),
            mean_absolute_demand_baseline=(
                "absolute_demand_axis_baseline",
                "mean",
            ),
            mean_absolute_supply_challenger=(
                "absolute_supply_axis_challenger",
                "mean",
            ),
            mean_absolute_demand_challenger=(
                "absolute_demand_axis_challenger",
                "mean",
            ),
            mean_regime_strength_baseline=(
                "regime_strength_baseline",
                "mean",
            ),
            mean_regime_strength_challenger=(
                "regime_strength_challenger",
                "mean",
            ),
            mean_boundary_distance_baseline=(
                (
                    "distance_to_boundary_"
                    "degrees_baseline"
                ),
                "mean",
            ),
            mean_boundary_distance_challenger=(
                (
                    "distance_to_boundary_"
                    "degrees_challenger"
                ),
                "mean",
            ),
            mean_absolute_supply_delta=(
                "supply_pressure_score_delta",
                lambda values: values.abs().mean(),
            ),
            mean_absolute_inventory_metric_delta=(
                "inventory_metric_score_delta",
                lambda values: values.abs().mean(),
            ),
        )
        .reset_index()
    )


def _plot_geo_review(
    panel: pd.DataFrame,
    *,
    geo_id: str,
    output_dir: Path,
) -> list[Path]:
    geo = panel[
        panel["geo_id"].eq(
            geo_id
        )
    ].sort_values(
        "date"
    )

    if geo.empty:
        raise ValueError(
            f"No panel rows for {geo_id}"
        )

    paths = []

    figure, axis = plt.subplots(
        figsize=(15, 6)
    )

    axis.plot(
        geo["date"],
        geo[
            "supply_axis_score_baseline"
        ],
        label="Supply baseline",
    )

    axis.plot(
        geo["date"],
        geo[
            "supply_axis_score_challenger"
        ],
        label="Supply challenger",
    )

    axis.plot(
        geo["date"],
        geo[
            "demand_axis_score_baseline"
        ],
        label="Demand",
    )

    changed_dates = geo.loc[
        geo[
            "major_assignment_changed"
        ],
        "date",
    ]

    for date in changed_dates:
        axis.axvline(
            date,
            alpha=0.20,
        )

    axis.axhline(
        0.0,
        linewidth=1,
    )

    axis.set_title(
        f"{geo_id}: Supply and Demand axes"
    )

    axis.set_ylabel(
        "Axis score"
    )

    axis.legend()

    axis.grid(
        alpha=0.25
    )

    figure.tight_layout()

    path = output_dir / (
        f"{geo_id}__axes.png"
    )

    figure.savefig(
        path,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(figure)

    paths.append(path)

    figure, axis = plt.subplots(
        figsize=(15, 6)
    )

    axis.plot(
        geo["date"],
        geo[
            "raw_active_inventory"
        ],
        label="Raw inventory",
    )

    axis.plot(
        geo["date"],
        geo[
            "raw_active_inventory_ma3"
        ],
        label="MA3",
    )

    axis.plot(
        geo["date"],
        geo[
            "raw_active_inventory_ma12"
        ],
        label="MA12",
    )

    for date in changed_dates:
        axis.axvline(
            date,
            alpha=0.20,
        )

    axis.set_title(
        f"{geo_id}: Raw inventory, MA3 and MA12"
    )

    axis.set_ylabel(
        "Inventory"
    )

    axis.legend()

    axis.grid(
        alpha=0.25
    )

    figure.tight_layout()

    path = output_dir / (
        f"{geo_id}__inventory_levels.png"
    )

    figure.savefig(
        path,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(figure)

    paths.append(path)

    figure, axis = plt.subplots(
        figsize=(15, 6)
    )

    axis.plot(
        geo["date"],
        geo[
            "inventory_metric_score_baseline"
        ],
        label="Inventory score baseline",
    )

    axis.plot(
        geo["date"],
        geo[
            "inventory_metric_score_challenger"
        ],
        label="Inventory score challenger",
    )

    axis.plot(
        geo["date"],
        geo[
            "supply_dimension_score_baseline"
        ],
        label="Supply dimension baseline",
    )

    axis.plot(
        geo["date"],
        geo[
            "supply_dimension_score_challenger"
        ],
        label="Supply dimension challenger",
    )

    for date in changed_dates:
        axis.axvline(
            date,
            alpha=0.20,
        )

    axis.axhline(
        0.0,
        linewidth=1,
    )

    axis.set_title(
        f"{geo_id}: Inventory metric and Supply dimension"
    )

    axis.set_ylabel(
        "Score"
    )

    axis.legend()

    axis.grid(
        alpha=0.25
    )

    figure.tight_layout()

    path = output_dir / (
        f"{geo_id}__inventory_supply_scores.png"
    )

    figure.savefig(
        path,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(figure)

    paths.append(path)

    recent = geo[
        geo["date"].ge(
            pd.Timestamp(
                "2023-01-01"
            )
        )
    ].copy()

    figure, axis = plt.subplots(
        figsize=(15, 6)
    )

    axis.plot(
        recent["date"],
        recent[
            "supply_pressure_score_baseline"
        ],
        marker="o",
        label="Supply baseline",
    )

    axis.plot(
        recent["date"],
        recent[
            "supply_pressure_score_challenger"
        ],
        marker="o",
        label="Supply challenger",
    )

    axis.plot(
        recent["date"],
        recent[
            "demand_strength_score_baseline"
        ],
        marker="o",
        label="Demand",
    )

    recent_changed_dates = (
        recent.loc[
            recent[
                "major_assignment_changed"
            ],
            "date",
        ]
    )

    for date in recent_changed_dates:
        axis.axvline(
            date,
            alpha=0.25,
        )

    axis.axhline(
        0.0,
        linewidth=1,
    )

    axis.set_title(
        f"{geo_id}: 2023–2026 coordinate inputs"
    )

    axis.set_ylabel(
        "Coordinate score"
    )

    axis.legend()

    axis.grid(
        alpha=0.25
    )

    figure.tight_layout()

    path = output_dir / (
        f"{geo_id}__recent_coordinates.png"
    )

    figure.savefig(
        path,
        dpi=160,
        bbox_inches="tight",
    )

    plt.close(figure)

    paths.append(path)

    return paths


def build_inventory_chronological_review(
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
    event_window_months: int = (
        EVENT_WINDOW_MONTHS
    ),
) -> ReviewResult:
    if geo_ids is None:
        geo_ids = FOCUS_GEOS.copy()

    persisted_store = RegimeArtifactStore(
        artifact_root
    )

    source_metrics = (
        persisted_store.read_dataframe(
            baseline_run_id,
            "source_metrics",
        )
    )

    baseline_features = (
        persisted_store.read_dataframe(
            baseline_run_id,
            "features",
        )
    )

    challenger = (
        build_in_memory_smoothing_challenger(
            baseline_features=(
                baseline_features
            ),
            source_metrics=source_metrics,
            experiment_id=(
                challenger_run_id
            ),
        )
    )

    artifact_reader = OverlayArtifactReader(
        persisted_store=persisted_store,
        challenger_run_id=(
            challenger_run_id
        ),
        challenger_artifacts=(
            challenger.as_mapping()
        ),
    )

    panel = _merge_monthly_panel(
        artifact_reader,
        baseline_run_id=(
            baseline_run_id
        ),
        challenger_run_id=(
            challenger_run_id
        ),
        geo_ids=geo_ids,
    )

    changed_months = (
        _build_changed_months(
            panel
        )
    )

    event_windows = (
        _build_event_windows(
            panel,
            window_months=(
                event_window_months
            ),
        )
    )

    changed_month_summary = (
        _build_changed_month_summary(
            panel
        )
    )

    review = ReviewResult(
        metadata={
            "review_type": (
                "inventory_chronological_review"
            ),
            "baseline_run_id": (
                baseline_run_id
            ),
            "challenger_run_id": (
                challenger_run_id
            ),
            "challenger_materialization": (
                "in_memory"
            ),
            "challenger_persisted": False,
            "geo_ids": list(geo_ids),
            "event_window_months": (
                event_window_months
            ),
            "challenger_feature_rows": (
                len(challenger.features)
            ),
            "challenger_assignment_rows": (
                len(
                    challenger.regime_assignments
                )
            ),
            "smoothing_lineage_rows": (
                len(
                    challenger.smoothing_lineage
                )
            ),
        }
    )

    review.add_table(
        "monthly_panel",
        panel,
    )

    review.add_table(
        "changed_months",
        changed_months,
    )

    review.add_table(
        "major_event_windows",
        event_windows,
    )

    review.add_table(
        "changed_month_summary",
        changed_month_summary,
    )

    return review


def write_inventory_chronological_review(
    review: ReviewResult,
    *,
    output_dir: str | Path = (
        DEFAULT_OUTPUT_DIR
    ),
) -> dict[str, list[Path]]:
    destination = Path(
        output_dir
    )

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    table_paths = []

    for name, frame in (
        review.tables.items()
    ):
        path = destination / (
            f"{name}.csv"
        )

        frame.to_csv(
            path,
            index=False,
        )

        table_paths.append(path)

    plot_paths = []

    panel = review.tables[
        "monthly_panel"
    ]

    for geo_id in sorted(
        panel["geo_id"].unique()
    ):
        plot_paths.extend(
            _plot_geo_review(
                panel,
                geo_id=geo_id,
                output_dir=destination,
            )
        )

    return {
        "tables": table_paths,
        "plots": plot_paths,
    }
