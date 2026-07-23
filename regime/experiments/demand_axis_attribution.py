from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# =============================================================================
# Contract
# =============================================================================

AXIS_KEY = "demand"

DIMENSIONS = (
    "demand",
    "price",
    "affordability",
    "capital_markets",
)

INPUT_PATH = Path(
    "artifacts/regime/review_exports/"
    "integrated_demand_chronology/"
    "monthly_integrated_demand_axis.csv"
)

OUTPUT_DIR = Path(
    "artifacts/regime/review_exports/"
    "demand_axis_attribution_d2"
)

AXIS_CHANGE_EPSILON = 1e-12

EXTREME_MOVE_COUNT_PER_GEO = 20


# =============================================================================
# Helpers
# =============================================================================

def _write_csv(
    frame: pd.DataFrame,
    path: Path,
) -> None:
    output = frame.copy()

    for column in output.columns:
        if pd.api.types.is_datetime64_any_dtype(
            output[column]
        ):
            output[column] = (
                output[column]
                .dt.strftime("%Y-%m-%d")
            )

    output.to_csv(
        path,
        index=False,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")

    if isinstance(value, Path):
        return str(value)

    if hasattr(value, "item"):
        return value.item()

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return value


def _write_json(
    payload: dict[str, Any],
    path: Path,
) -> None:
    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=_json_default,
        )
        + "\n",
        encoding="utf-8",
    )


def _safe_divide(
    numerator: pd.Series,
    denominator: pd.Series,
    *,
    epsilon: float = AXIS_CHANGE_EPSILON,
) -> pd.Series:
    valid = (
        numerator.notna()
        & denominator.notna()
        & denominator.abs().gt(epsilon)
    )

    output = pd.Series(
        np.nan,
        index=numerator.index,
        dtype=float,
    )

    output.loc[valid] = (
        numerator.loc[valid]
        / denominator.loc[valid]
    )

    return output


def _dominant_dimension(
    frame: pd.DataFrame,
    columns: dict[str, str],
) -> pd.Series:
    values = pd.DataFrame(
        {
            dimension: pd.to_numeric(
                frame[column],
                errors="coerce",
            )
            for dimension, column
            in columns.items()
        },
        index=frame.index,
    )

    absolute = values.abs()

    valid = absolute.notna().any(axis=1)

    output = pd.Series(
        pd.NA,
        index=frame.index,
        dtype="object",
    )

    output.loc[valid] = (
        absolute.loc[valid]
        .idxmax(axis=1)
    )

    return output


# =============================================================================
# Input
# =============================================================================

def load_integrated_chronology(
    path: Path = INPUT_PATH,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            "Missing Phase D1 integrated chronology: "
            f"{path}"
        )

    frame = pd.read_csv(
        path,
        low_memory=False,
    )

    required = {
        "geo_id",
        "date",
        "integrated_demand_axis",
        "complete_dimension_coverage",
    }

    for dimension in DIMENSIONS:
        required.update(
            {
                dimension,
                f"{dimension}_weight",
                f"{dimension}_weighted_contribution",
                f"{dimension}_source_role",
            }
        )

    missing = required - set(frame.columns)

    if missing:
        raise ValueError(
            f"{path} is missing required D2 columns: "
            f"{sorted(missing)}"
        )

    frame["date"] = pd.to_datetime(
        frame["date"],
        errors="coerce",
    )

    for column in (
        "integrated_demand_axis",
        *DIMENSIONS,
        *[
            f"{dimension}_weight"
            for dimension in DIMENSIONS
        ],
        *[
            f"{dimension}_weighted_contribution"
            for dimension in DIMENSIONS
        ],
    ):
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        )

    complete_flag = (
        frame["complete_dimension_coverage"]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(
            {
                "true",
                "1",
                "yes",
                "y",
            }
        )
    )

    frame = frame[
        complete_flag
    ].copy()

    frame = frame.dropna(
        subset=[
            "geo_id",
            "date",
            "integrated_demand_axis",
            *DIMENSIONS,
        ]
    )

    if frame.empty:
        raise ValueError(
            "Phase D1 chronology contains no complete "
            "rows usable for D2."
        )

    duplicates = frame.duplicated(
        subset=[
            "geo_id",
            "date",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Phase D1 chronology contains duplicate "
            "geo/date rows."
        )

    frame = frame.sort_values(
        [
            "geo_id",
            "date",
        ],
        kind="mergesort",
    ).reset_index(drop=True)

    return frame


# =============================================================================
# Monthly attribution
# =============================================================================

def build_monthly_attribution(
    chronology: pd.DataFrame,
) -> pd.DataFrame:
    monthly = chronology.copy()

    grouped = monthly.groupby(
        "geo_id",
        sort=False,
        group_keys=False,
    )

    monthly["axis_change"] = grouped[
        "integrated_demand_axis"
    ].diff()

    contribution_change_columns: list[str] = []
    score_change_columns: list[str] = []

    for dimension in DIMENSIONS:
        score_change = (
            f"{dimension}_score_change"
        )

        contribution_change = (
            f"{dimension}_contribution_change"
        )

        score_change_columns.append(
            score_change
        )

        contribution_change_columns.append(
            contribution_change
        )

        monthly[score_change] = grouped[
            dimension
        ].diff()

        monthly[contribution_change] = grouped[
            f"{dimension}_weighted_contribution"
        ].diff()

        monthly[
            f"{dimension}_aligned_contribution_change"
        ] = np.where(
            monthly["axis_change"].notna()
            & monthly[contribution_change].notna()
            & (
                np.sign(monthly["axis_change"])
                == np.sign(
                    monthly[contribution_change]
                )
            ),
            monthly[
                contribution_change
            ].abs(),
            0.0,
        )

        monthly[
            f"{dimension}_opposing_contribution_change"
        ] = np.where(
            monthly["axis_change"].notna()
            & monthly[contribution_change].notna()
            & (
                np.sign(monthly["axis_change"])
                != np.sign(
                    monthly[contribution_change]
                )
            )
            & monthly[contribution_change].ne(0),
            monthly[
                contribution_change
            ].abs(),
            0.0,
        )

    monthly["reconstructed_axis_change"] = (
        monthly[
            contribution_change_columns
        ].sum(
            axis=1,
            min_count=len(
                contribution_change_columns
            ),
        )
    )

    monthly[
        "axis_change_reconstruction_residual"
    ] = (
        monthly["axis_change"]
        - monthly[
            "reconstructed_axis_change"
        ]
    )

    monthly["gross_component_activity"] = (
        monthly[
            contribution_change_columns
        ].abs().sum(
            axis=1,
            min_count=len(
                contribution_change_columns
            ),
        )
    )
    
    squared_component_activity = (
        monthly[
            contribution_change_columns
        ]
        .pow(2)
        .sum(
            axis=1,
            min_count=len(
                contribution_change_columns
            ),
        )
    )

    monthly["effective_component_count"] = (
        monthly[
            "gross_component_activity"
        ].pow(2)
        / squared_component_activity
    )

    monthly.loc[
        squared_component_activity.le(
            AXIS_CHANGE_EPSILON
        ),
        "effective_component_count",
    ] = np.nan

    monthly["aligned_component_activity"] = (
        monthly[
            [
                f"{dimension}_aligned_contribution_change"
                for dimension in DIMENSIONS
            ]
        ].sum(axis=1)
    )

    monthly["opposing_component_activity"] = (
        monthly[
            [
                f"{dimension}_opposing_contribution_change"
                for dimension in DIMENSIONS
            ]
        ].sum(axis=1)
    )

    monthly["net_to_gross_ratio"] = _safe_divide(
        monthly["axis_change"].abs(),
        monthly["gross_component_activity"],
    )

    monthly["component_cancellation"] = (
        monthly["gross_component_activity"]
        - monthly["axis_change"].abs()
    )

    monthly[
        "component_cancellation_rate"
    ] = _safe_divide(
        monthly["component_cancellation"],
        monthly["gross_component_activity"],
    )

    for dimension in DIMENSIONS:
        contribution_change = (
            f"{dimension}_contribution_change"
        )

        monthly[
            f"{dimension}_share_of_axis_change"
        ] = _safe_divide(
            monthly[contribution_change],
            monthly["axis_change"],
        )

        monthly[
            f"{dimension}_share_of_absolute_change"
        ] = _safe_divide(
            monthly[
                contribution_change
            ].abs(),
            monthly[
                "gross_component_activity"
            ],
        )

    monthly[
        "dominant_dimension_by_absolute_change"
    ] = _dominant_dimension(
        monthly,
        {
            dimension:
                f"{dimension}_contribution_change"
            for dimension in DIMENSIONS
        },
    )

    monthly[
        "dominant_dimension_by_score_change"
    ] = _dominant_dimension(
        monthly,
        {
            dimension:
                f"{dimension}_score_change"
            for dimension in DIMENSIONS
        },
    )

    monthly["axis_direction"] = np.select(
        [
            monthly["axis_change"].gt(
                AXIS_CHANGE_EPSILON
            ),
            monthly["axis_change"].lt(
                -AXIS_CHANGE_EPSILON
            ),
        ],
        [
            "positive",
            "negative",
        ],
        default="flat",
    )

    monthly.loc[
        monthly["axis_change"].isna(),
        "axis_direction",
    ] = pd.NA

    return monthly


def build_long_attribution(
    monthly: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for row in monthly.itertuples(
        index=False
    ):
        for dimension in DIMENSIONS:
            records.append(
                {
                    "geo_id": row.geo_id,
                    "date": row.date,
                    "axis": AXIS_KEY,
                    "dimension": dimension,
                    "dimension_score": getattr(
                        row,
                        dimension,
                    ),
                    "dimension_score_change": getattr(
                        row,
                        f"{dimension}_score_change",
                    ),
                    "configured_weight": getattr(
                        row,
                        f"{dimension}_weight",
                    ),
                    "weighted_contribution": getattr(
                        row,
                        f"{dimension}_weighted_contribution",
                    ),
                    "weighted_contribution_change": getattr(
                        row,
                        f"{dimension}_contribution_change",
                    ),
                    "share_of_axis_change": getattr(
                        row,
                        f"{dimension}_share_of_axis_change",
                    ),
                    "share_of_absolute_change": getattr(
                        row,
                        f"{dimension}_share_of_absolute_change",
                    ),
                    "aligned_contribution_activity": getattr(
                        row,
                        f"{dimension}_aligned_contribution_change",
                    ),
                    "opposing_contribution_activity": getattr(
                        row,
                        f"{dimension}_opposing_contribution_change",
                    ),
                    "integrated_demand_axis":
                        row.integrated_demand_axis,
                    "axis_change":
                        row.axis_change,
                    "reconstructed_axis_change":
                        row.reconstructed_axis_change,
                    "axis_change_reconstruction_residual":
                        row.axis_change_reconstruction_residual,
                    "aligned_component_activity":
                        row.aligned_component_activity,
                    "opposing_component_activity":
                        row.opposing_component_activity,
                    "gross_component_activity":
                        row.gross_component_activity,
                    "effective_component_count":
                        row.effective_component_count,
                    "component_cancellation":
                        row.component_cancellation,
                    "component_cancellation_rate":
                        row.component_cancellation_rate,
                    "net_to_gross_ratio":
                        row.net_to_gross_ratio,
                    "axis_direction":
                        row.axis_direction,
                    "dominant_dimension_by_absolute_change":
                        row.dominant_dimension_by_absolute_change,
                    "source_role": getattr(
                        row,
                        f"{dimension}_source_role",
                    ),
                }
            )

    return pd.DataFrame(records)


# =============================================================================
# Summaries
# =============================================================================

def build_monthly_dimension_contributions(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    return long_attribution[
        [
            "geo_id",
            "date",
            "axis",
            "dimension",
            "dimension_score",
            "dimension_score_change",
            "configured_weight",
            "weighted_contribution",
            "weighted_contribution_change",
            "share_of_axis_change",
            "share_of_absolute_change",
            "aligned_contribution_activity",
            "opposing_contribution_activity",
            "integrated_demand_axis",
            "axis_change",
            "gross_component_activity",
            "effective_component_count",
            "component_cancellation",
            "component_cancellation_rate",
            "net_to_gross_ratio",
            "axis_direction",
            "dominant_dimension_by_absolute_change",
            "source_role",
        ]
    ].copy()


def build_monthly_dimension_volatility(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    usable = long_attribution.dropna(
        subset=[
            "weighted_contribution_change",
        ]
    ).copy()

    summary = (
        usable.groupby(
            [
                "geo_id",
                "dimension",
            ],
            as_index=False,
        )
        .agg(
            observations=(
                "weighted_contribution_change",
                "size",
            ),
            first_date=(
                "date",
                "min",
            ),
            last_date=(
                "date",
                "max",
            ),
            mean_score_change=(
                "dimension_score_change",
                "mean",
            ),
            mean_absolute_score_change=(
                "dimension_score_change",
                lambda series:
                    series.abs().mean(),
            ),
            score_change_std=(
                "dimension_score_change",
                "std",
            ),
            mean_contribution_change=(
                "weighted_contribution_change",
                "mean",
            ),
            mean_absolute_contribution_change=(
                "weighted_contribution_change",
                lambda series:
                    series.abs().mean(),
            ),
            contribution_change_std=(
                "weighted_contribution_change",
                "std",
            ),
            mean_share_of_absolute_change=(
                "share_of_absolute_change",
                "mean",
            ),
            p90_share_of_absolute_change=(
                "share_of_absolute_change",
                lambda series:
                    series.quantile(0.90),
            ),
            aligned_activity_total=(
                "aligned_contribution_activity",
                "sum",
            ),
            opposing_activity_total=(
                "opposing_contribution_activity",
                "sum",
            ),
        )
    )

    return summary.sort_values(
        [
            "geo_id",
            "mean_absolute_contribution_change",
        ],
        ascending=[
            True,
            False,
        ],
    ).reset_index(drop=True)


def build_dimension_share_of_axis_change(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    usable = long_attribution.dropna(
        subset=[
            "axis_change",
            "weighted_contribution_change",
        ]
    ).copy()

    usable = usable[
        usable["axis_change"]
        .abs()
        .gt(AXIS_CHANGE_EPSILON)
    ]

    return (
        usable.groupby(
            [
                "geo_id",
                "dimension",
            ],
            as_index=False,
        )
        .agg(
            observations=(
                "share_of_axis_change",
                "count",
            ),
            mean_share_of_axis_change=(
                "share_of_axis_change",
                "mean",
            ),
            median_share_of_axis_change=(
                "share_of_axis_change",
                "median",
            ),
            mean_absolute_share_of_axis_change=(
                "share_of_axis_change",
                lambda series:
                    series.abs().mean(),
            ),
            positive_axis_month_mean_share=(
                "share_of_axis_change",
                lambda series:
                    series[
                        usable.loc[
                            series.index,
                            "axis_change",
                        ].gt(0)
                    ].mean(),
            ),
            negative_axis_month_mean_share=(
                "share_of_axis_change",
                lambda series:
                    series[
                        usable.loc[
                            series.index,
                            "axis_change",
                        ].lt(0)
                    ].mean(),
            ),
        )
        .sort_values(
            [
                "geo_id",
                "mean_absolute_share_of_axis_change",
            ],
            ascending=[
                True,
                False,
            ],
        )
        .reset_index(drop=True)
    )


def build_dimension_share_of_absolute_change(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    usable = long_attribution.dropna(
        subset=[
            "share_of_absolute_change",
        ]
    ).copy()

    return (
        usable.groupby(
            [
                "geo_id",
                "dimension",
            ],
            as_index=False,
        )
        .agg(
            observations=(
                "share_of_absolute_change",
                "count",
            ),
            mean_share_of_absolute_change=(
                "share_of_absolute_change",
                "mean",
            ),
            median_share_of_absolute_change=(
                "share_of_absolute_change",
                "median",
            ),
            p75_share_of_absolute_change=(
                "share_of_absolute_change",
                lambda series:
                    series.quantile(0.75),
            ),
            p90_share_of_absolute_change=(
                "share_of_absolute_change",
                lambda series:
                    series.quantile(0.90),
            ),
            dominant_month_count=(
                "share_of_absolute_change",
                lambda series:
                    int(
                        series.ge(0.5).sum()
                    ),
            ),
        )
        .sort_values(
            [
                "geo_id",
                "mean_share_of_absolute_change",
            ],
            ascending=[
                True,
                False,
            ],
        )
        .reset_index(drop=True)
    )


def build_monthly_extreme_moves(
    monthly: pd.DataFrame,
    *,
    count_per_geo: int = EXTREME_MOVE_COUNT_PER_GEO,
) -> pd.DataFrame:
    usable = monthly.dropna(
        subset=[
            "axis_change",
        ]
    ).copy()

    usable["absolute_axis_change"] = (
        usable["axis_change"].abs()
    )

    selected = (
        usable.sort_values(
            [
                "geo_id",
                "absolute_axis_change",
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
            group_keys=False,
        )
        .head(count_per_geo)
        .copy()
    )

    columns = [
        "geo_id",
        "date",
        "integrated_demand_axis",
        "axis_change",
        "absolute_axis_change",
        "axis_direction",
        "dominant_dimension_by_absolute_change",
        "gross_component_activity",
        "effective_component_count",
        "aligned_component_activity",
        "opposing_component_activity",
        "component_cancellation",
        "component_cancellation_rate",
        "net_to_gross_ratio",
        "axis_change_reconstruction_residual",
    ]

    for dimension in DIMENSIONS:
        columns.extend(
            [
                f"{dimension}_score_change",
                f"{dimension}_contribution_change",
                f"{dimension}_share_of_axis_change",
                f"{dimension}_share_of_absolute_change",
            ]
        )

    return selected[
        columns
    ].sort_values(
        [
            "geo_id",
            "absolute_axis_change",
        ],
        ascending=[
            True,
            False,
        ],
    ).reset_index(drop=True)


def build_geography_summary(
    monthly: pd.DataFrame,
) -> pd.DataFrame:
    usable = monthly.dropna(
        subset=[
            "axis_change",
        ]
    ).copy()

    return (
        usable.groupby(
            "geo_id",
            as_index=False,
        )
        .agg(
            observations=(
                "axis_change",
                "size",
            ),
            first_change_date=(
                "date",
                "min",
            ),
            last_change_date=(
                "date",
                "max",
            ),
            mean_axis_change=(
                "axis_change",
                "mean",
            ),
            mean_absolute_axis_change=(
                "axis_change",
                lambda series:
                    series.abs().mean(),
            ),
            axis_change_std=(
                "axis_change",
                "std",
            ),
            p90_absolute_axis_change=(
                "axis_change",
                lambda series:
                    series.abs().quantile(0.90),
            ),
            mean_gross_component_activity=(
                "gross_component_activity",
                "mean",
            ),
            mean_component_cancellation=(
                "component_cancellation",
                "mean",
            ),
            mean_component_cancellation_rate=(
                "component_cancellation_rate",
                "mean",
            ),
            mean_net_to_gross_ratio=(
                "net_to_gross_ratio",
                "mean",
            ),
            max_absolute_reconstruction_residual=(
                "axis_change_reconstruction_residual",
                lambda series:
                    series.abs().max(),
            ),
            mean_effective_component_count=(
                "effective_component_count",
                "mean",
            ),
            median_effective_component_count=(
                "effective_component_count",
                "median",
            ),
            p90_effective_component_count=(
                "effective_component_count",
                lambda series:
                    series.quantile(0.90),
            ),
        )
    )


def build_geography_dimension_summary(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    usable = long_attribution.dropna(
        subset=[
            "weighted_contribution_change",
        ]
    ).copy()

    return (
        usable.groupby(
            [
                "geo_id",
                "dimension",
            ],
            as_index=False,
        )
        .agg(
            observations=(
                "weighted_contribution_change",
                "size",
            ),
            total_contribution_change=(
                "weighted_contribution_change",
                "sum",
            ),
            total_absolute_contribution_change=(
                "weighted_contribution_change",
                lambda series:
                    series.abs().sum(),
            ),
            mean_absolute_contribution_change=(
                "weighted_contribution_change",
                lambda series:
                    series.abs().mean(),
            ),
            mean_share_of_absolute_change=(
                "share_of_absolute_change",
                "mean",
            ),
            aligned_activity_total=(
                "aligned_contribution_activity",
                "sum",
            ),
            opposing_activity_total=(
                "opposing_contribution_activity",
                "sum",
            ),
        )
        .sort_values(
            [
                "geo_id",
                "total_absolute_contribution_change",
            ],
            ascending=[
                True,
                False,
            ],
        )
        .reset_index(drop=True)
    )


def build_candidate_summary(
    monthly: pd.DataFrame,
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    usable_monthly = monthly.dropna(
        subset=[
            "axis_change",
        ]
    ).copy()

    usable_long = long_attribution.dropna(
        subset=[
            "weighted_contribution_change",
        ]
    ).copy()

    rows: list[dict[str, Any]] = []

    for geo_id, group in usable_monthly.groupby(
        "geo_id",
        sort=True,
    ):
        long_group = usable_long[
            usable_long["geo_id"].eq(
                geo_id
            )
        ]

        dimension_activity = (
            long_group.groupby(
                "dimension"
            )[
                "weighted_contribution_change"
            ]
            .apply(
                lambda series:
                    series.abs().sum()
            )
            .sort_values(
                ascending=False
            )
        )

        dominant_dimension = (
            dimension_activity.index[0]
            if not dimension_activity.empty
            else None
        )

        rows.append(
            {
                "geo_id": geo_id,
                "rows": int(len(group)),
                "first_date":
                    group["date"].min(),
                "last_date":
                    group["date"].max(),
                "mean_axis_change": float(
                    group["axis_change"].mean()
                ),
                "mean_absolute_axis_change": float(
                    group[
                        "axis_change"
                    ].abs().mean()
                ),
                "axis_change_std": float(
                    group["axis_change"].std()
                ),
                "mean_gross_component_activity": float(
                    group[
                        "gross_component_activity"
                    ].mean()
                ),
                "mean_component_cancellation_rate": float(
                    group[
                        "component_cancellation_rate"
                    ].mean()
                ),
                "dominant_dimension_by_total_activity":
                    dominant_dimension,
                "max_absolute_reconstruction_residual":
                    float(
                        group[
                            "axis_change_reconstruction_residual"
                        ].abs().max()
                    ),
                "mean_effective_component_count": float(
                    group[
                        "effective_component_count"
                    ].mean()
                ),
                "median_effective_component_count": float(
                    group[
                        "effective_component_count"
                    ].median()
                ),
            }
        )

    return pd.DataFrame(rows)


def build_latest_attribution_state(
    monthly: pd.DataFrame,
) -> pd.DataFrame:
    usable = monthly.dropna(
        subset=[
            "axis_change",
        ]
    ).copy()

    return (
        usable.sort_values("date")
        .groupby(
            "geo_id",
            group_keys=False,
        )
        .tail(1)
        .sort_values("geo_id")
        .reset_index(drop=True)
    )


def build_axis_reconstruction_diagnostics(
    monthly: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for geo_id, group in monthly.groupby(
        "geo_id",
        sort=True,
    ):
        compared = group.dropna(
            subset=[
                "axis_change",
                "reconstructed_axis_change",
            ]
        )

        rows.append(
            {
                "geo_id": geo_id,
                "rows_total": int(
                    len(group)
                ),
                "rows_compared": int(
                    len(compared)
                ),
                "first_compared_date": (
                    compared["date"].min()
                    if not compared.empty
                    else pd.NaT
                ),
                "last_compared_date": (
                    compared["date"].max()
                    if not compared.empty
                    else pd.NaT
                ),
                "mean_residual": (
                    float(
                        compared[
                            "axis_change_reconstruction_residual"
                        ].mean()
                    )
                    if not compared.empty
                    else np.nan
                ),
                "mean_absolute_residual": (
                    float(
                        compared[
                            "axis_change_reconstruction_residual"
                        ].abs().mean()
                    )
                    if not compared.empty
                    else np.nan
                ),
                "max_absolute_residual": (
                    float(
                        compared[
                            "axis_change_reconstruction_residual"
                        ].abs().max()
                    )
                    if not compared.empty
                    else np.nan
                ),
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# Runner
# =============================================================================

def run_demand_axis_attribution(
    *,
    input_path: Path = INPUT_PATH,
    output_dir: Path = OUTPUT_DIR,
) -> dict[str, Any]:
    print(
        "[demand_axis_attribution_d2] "
        "stage=load_integrated_chronology"
    )

    chronology = load_integrated_chronology(
        input_path
    )

    print(
        f"  rows={len(chronology):,}"
    )
    print(
        "  geographies="
        f"{chronology['geo_id'].nunique():,}"
    )
    print(
        "  chronology="
        f"{chronology['date'].min().date()} "
        "to "
        f"{chronology['date'].max().date()}"
    )

    print(
        "\n[demand_axis_attribution_d2] "
        "stage=build_monthly_attribution"
    )

    monthly = build_monthly_attribution(
        chronology
    )

    long_attribution = (
        build_long_attribution(
            monthly
        )
    )

    contributions = (
        build_monthly_dimension_contributions(
            long_attribution
        )
    )

    volatility = (
        build_monthly_dimension_volatility(
            long_attribution
        )
    )

    axis_share = (
        build_dimension_share_of_axis_change(
            long_attribution
        )
    )

    absolute_share = (
        build_dimension_share_of_absolute_change(
            long_attribution
        )
    )

    extreme_moves = (
        build_monthly_extreme_moves(
            monthly
        )
    )

    geography_summary = (
        build_geography_summary(
            monthly
        )
    )

    geography_dimension_summary = (
        build_geography_dimension_summary(
            long_attribution
        )
    )

    candidate_summary = (
        build_candidate_summary(
            monthly,
            long_attribution,
        )
    )

    latest_state = (
        build_latest_attribution_state(
            monthly
        )
    )

    diagnostics = (
        build_axis_reconstruction_diagnostics(
            monthly
        )
    )

    compared = monthly.dropna(
        subset=[
            "axis_change",
            "reconstructed_axis_change",
        ]
    )

    if compared.empty:
        raise ValueError(
            "D2 produced no comparable monthly changes."
        )

    max_residual = float(
        compared[
            "axis_change_reconstruction_residual"
        ].abs().max()
    )

    if max_residual > 1e-10:
        raise ValueError(
            "Demand-axis monthly change reconstruction "
            "failed: "
            f"max residual={max_residual:.12g}"
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    outputs = {
        "monthly_axis_attribution.csv":
            monthly,
        "monthly_dimension_contributions.csv":
            contributions,
        "monthly_dimension_volatility.csv":
            volatility,
        "dimension_share_of_axis_change.csv":
            axis_share,
        "dimension_share_of_absolute_change.csv":
            absolute_share,
        "monthly_extreme_moves.csv":
            extreme_moves,
        "geography_summary.csv":
            geography_summary,
        "geography_dimension_summary.csv":
            geography_dimension_summary,
        "candidate_summary.csv":
            candidate_summary,
        "latest_attribution_state.csv":
            latest_state,
        "axis_reconstruction_diagnostics.csv":
            diagnostics,
    }

    for filename, frame in outputs.items():
        _write_csv(
            frame,
            output_dir / filename,
        )

    manifest = {
        "phase": "D2",
        "artifact_contract":
            "monthly_demand_axis_attribution",
        "input_path": input_path,
        "input_rows": int(
            len(chronology)
        ),
        "geographies": int(
            chronology["geo_id"].nunique()
        ),
        "first_date":
            chronology["date"].min(),
        "last_date":
            chronology["date"].max(),
        "change_rows": int(
            monthly["axis_change"]
            .notna()
            .sum()
        ),
        "dimensions":
            list(DIMENSIONS),
        "axis":
            AXIS_KEY,
        "max_axis_change_reconstruction_residual":
            max_residual,
        "output_directory":
            output_dir,
        "output_files":
            sorted(outputs),
    }

    _write_json(
        manifest,
        output_dir
        / "attribution_manifest.json",
    )

    print(
        "\n[demand_axis_attribution_d2] "
        "stage=validation"
    )

    print(
        diagnostics.to_string(
            index=False
        )
    )

    print("\nCandidate summary:")

    print(
        candidate_summary.to_string(
            index=False
        )
    )

    print("\nDimension activity summary:")

    print(
        geography_dimension_summary.to_string(
            index=False
        )
    )

    print("\nLatest attribution state:")

    latest_columns = [
        "geo_id",
        "date",
        "integrated_demand_axis",
        "axis_change",
        "axis_direction",
        "dominant_dimension_by_absolute_change",
        "gross_component_activity",
        "effective_component_count",
        "component_cancellation_rate",
        *[
            f"{dimension}_contribution_change"
            for dimension in DIMENSIONS
        ],
    ]

    print(
        latest_state[
            latest_columns
        ].to_string(
            index=False
        )
    )

    print("\nArtifacts written to:")
    print(output_dir)

    return {
        "chronology": chronology,
        "monthly_axis_attribution":
            monthly,
        "monthly_dimension_contributions":
            contributions,
        "monthly_dimension_volatility":
            volatility,
        "dimension_share_of_axis_change":
            axis_share,
        "dimension_share_of_absolute_change":
            absolute_share,
        "monthly_extreme_moves":
            extreme_moves,
        "geography_summary":
            geography_summary,
        "geography_dimension_summary":
            geography_dimension_summary,
        "candidate_summary":
            candidate_summary,
        "latest_attribution_state":
            latest_state,
        "axis_reconstruction_diagnostics":
            diagnostics,
        "manifest":
            manifest,
    }
