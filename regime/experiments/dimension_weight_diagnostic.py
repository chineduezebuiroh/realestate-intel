"""Phase D4: Dimension influence and structural-weight diagnosis.

Consumes the frozen Phase D2 attribution artifacts and produces:

- dimension_weight_scorecard.csv
- dimension_pair_relationships.csv
- dimension_sensitivity_analysis.csv
- dimension_rankings.csv
- dimension_diagnostic_manifest.json

D4 does not optimize or alter axis weights. It diagnoses behavior under the
currently configured production weights.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


DIMENSIONS: tuple[str, ...] = (
    "demand",
    "price",
    "affordability",
    "capital_markets",
)

DEFAULT_D2_DIR = Path(
    "artifacts/regime/review_exports/demand_axis_attribution_d2"
)

DEFAULT_OUTPUT_DIR = Path(
    "artifacts/regime/review_exports/dimension_weight_diagnostic"
)

EPSILON = 1e-12


def _first_existing_column(
    frame: pd.DataFrame,
    candidates: Iterable[str],
    *,
    label: str,
) -> str:
    """Return the first candidate column present in the dataframe."""

    for column in candidates:
        if column in frame.columns:
            return column

    raise KeyError(
        f"Unable to identify {label}. "
        f"Expected one of: {list(candidates)}. "
        f"Available columns: {sorted(frame.columns.tolist())}"
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact does not exist: {path}")

    frame = pd.read_csv(path)

    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="raise")

    return frame


def load_d2_artifacts(
    d2_dir: Path = DEFAULT_D2_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the canonical D2 monthly and long attribution artifacts."""

    monthly_path = d2_dir / "monthly_axis_attribution.csv"
    long_path = d2_dir / "monthly_dimension_contributions.csv"

    monthly = _read_csv(monthly_path)
    long_attribution = _read_csv(long_path)

    required_monthly = {
        "geo_id",
        "date",
        "integrated_demand_axis",
        "axis_change",
        "gross_component_activity",
        "component_cancellation_rate",
        "effective_component_count",
    }

    missing_monthly = required_monthly.difference(monthly.columns)
    if missing_monthly:
        raise ValueError(
            "monthly_axis_attribution.csv is missing required columns: "
            f"{sorted(missing_monthly)}"
        )

    required_long = {
        "geo_id",
        "date",
        "dimension",
    }

    missing_long = required_long.difference(long_attribution.columns)
    if missing_long:
        raise ValueError(
            "monthly_dimension_contributions.csv is missing required columns: "
            f"{sorted(missing_long)}"
        )

    observed_dimensions = set(
        long_attribution["dimension"].dropna().astype(str).unique()
    )

    missing_dimensions = set(DIMENSIONS).difference(observed_dimensions)
    if missing_dimensions:
        raise ValueError(
            "D2 long attribution is missing dimensions: "
            f"{sorted(missing_dimensions)}"
        )

    monthly = monthly.sort_values(["geo_id", "date"]).reset_index(drop=True)
    long_attribution = long_attribution.sort_values(
        ["geo_id", "date", "dimension"]
    ).reset_index(drop=True)

    return monthly, long_attribution


def resolve_long_columns(
    long_attribution: pd.DataFrame,
) -> dict[str, str]:
    """Resolve D2 column names while tolerating minor naming differences."""

    return {
        "score_change": _first_existing_column(
            long_attribution,
            (
                "dimension_score_change",
                "score_change",
            ),
            label="dimension score-change column",
        ),
        "contribution_change": _first_existing_column(
            long_attribution,
            (
                "weighted_contribution_change",
                "contribution_change",
                "dimension_contribution_change",
            ),
            label="weighted contribution-change column",
        ),
        "absolute_share": _first_existing_column(
            long_attribution,
            (
                "share_of_absolute_change",
                "absolute_change_share",
            ),
            label="share-of-absolute-change column",
        ),
        "aligned_activity": _first_existing_column(
            long_attribution,
            (
                "aligned_contribution_activity",
                "aligned_activity",
            ),
            label="aligned contribution-activity column",
        ),
        "opposing_activity": _first_existing_column(
            long_attribution,
            (
                "opposing_contribution_activity",
                "opposing_activity",
            ),
            label="opposing contribution-activity column",
        ),
        "weight": _first_existing_column(
            long_attribution,
            (
                "configured_weight",
                "dimension_weight",
                "weight",
            ),
            label="configured dimension-weight column",
        ),
    }


def build_dimension_weight_scorecard(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    """Build one diagnostic row per geography and dimension."""

    columns = resolve_long_columns(long_attribution)

    work = long_attribution.copy()

    contribution_column = columns["contribution_change"]
    score_change_column = columns["score_change"]
    share_column = columns["absolute_share"]
    aligned_column = columns["aligned_activity"]
    opposing_column = columns["opposing_activity"]
    weight_column = columns["weight"]

    work["absolute_contribution_change"] = work[
        contribution_column
    ].abs()

    work["absolute_score_change"] = work[
        score_change_column
    ].abs()

    work["gross_pair_activity"] = (
        work[aligned_column] + work[opposing_column]
    )

    work["net_activity"] = (
        work[aligned_column] - work[opposing_column]
    )

    work["net_to_gross_ratio"] = np.where(
        work["gross_pair_activity"].gt(EPSILON),
        work["net_activity"] / work["gross_pair_activity"],
        np.nan,
    )

    # Attribution only exists once a preceding month is available.
    # A geo-month is valid when at least one dimension has a contribution
    # change. The initial chronology month for each geography is excluded.
    work["is_attribution_observation"] = (
        work.groupby(
            ["geo_id", "date"]
        )[contribution_column]
        .transform("count")
        .gt(0)
    )

    attribution_work = work.loc[
        work["is_attribution_observation"]
    ].copy()

    if attribution_work.empty:
        raise ValueError(
            "No valid attribution observations were found after "
            "excluding geo-months with missing contribution changes."
        )

    # Identify the dimension with the largest absolute contribution change
    # in each valid geo-month.
    dominant_indices = (
        attribution_work.groupby(
            ["geo_id", "date"]
        )["absolute_contribution_change"]
        .idxmax()
    )

    dominant = (
        attribution_work.loc[
            dominant_indices,
            [
                "geo_id",
                "date",
                "dimension",
            ],
        ]
        .assign(is_dominant=1.0)
    )

    attribution_work = attribution_work.merge(
        dominant,
        on=["geo_id", "date", "dimension"],
        how="left",
        validate="one_to_one",
    )

    attribution_work["is_dominant"] = (
        attribution_work["is_dominant"].fillna(0.0)
    )

    required_context_columns = {
        "component_cancellation_rate",
        "effective_component_count",
    }

    missing_context_columns = (
        required_context_columns.difference(
            attribution_work.columns
        )
    )

    if missing_context_columns:
        raise KeyError(
            "D2 monthly dimension contributions are missing "
            "required monthly context columns: "
            f"{sorted(missing_context_columns)}"
        )

    attribution_work["active"] = attribution_work[
        "absolute_contribution_change"
    ].gt(EPSILON)

    active_work = attribution_work.loc[
        attribution_work["active"]
    ].copy()

    active_summary = (
        active_work.groupby(
            ["geo_id", "dimension"],
            as_index=False,
        )
        .agg(
            active_months=("date", "size"),
            mean_cancellation_rate_when_active=(
                "component_cancellation_rate",
                "mean",
            ),
            mean_effective_component_count_when_active=(
                "effective_component_count",
                "mean",
            ),
        )
    )

    scorecard = (
        attribution_work.groupby(
            ["geo_id", "dimension"],
            as_index=False,
        )
        .agg(
            configured_weight=(weight_column, "first"),
            observations=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            mean_signed_score_change=(
                score_change_column,
                "mean",
            ),
            mean_absolute_score_change=(
                "absolute_score_change",
                "mean",
            ),
            score_change_std=(
                score_change_column,
                "std",
            ),
            mean_signed_contribution_change=(
                contribution_column,
                "mean",
            ),
            mean_absolute_contribution_change=(
                "absolute_contribution_change",
                "mean",
            ),
            contribution_change_std=(
                contribution_column,
                "std",
            ),
            mean_share_of_absolute_change=(
                share_column,
                "mean",
            ),
            dominant_month_fraction=(
                "is_dominant",
                "mean",
            ),
            aligned_activity_total=(
                aligned_column,
                "sum",
            ),
            opposing_activity_total=(
                opposing_column,
                "sum",
            ),
            gross_activity_total=(
                "gross_pair_activity",
                "sum",
            ),
            net_activity_total=(
                "net_activity",
                "sum",
            ),
            mean_net_to_gross_ratio=(
                "net_to_gross_ratio",
                "mean",
            ),
        )
    )

    scorecard = scorecard.merge(
        active_summary,
        on=["geo_id", "dimension"],
        how="left",
        validate="one_to_one",
    )

    # A dimension may never exceed the activity threshold, so retain it
    # in the scorecard with zero active months.
    scorecard["active_months"] = (
        scorecard["active_months"].fillna(0).astype(int)
    )

    scorecard["active_month_fraction"] = (
        scorecard["active_months"]
        / scorecard["observations"]
    )

    scorecard["opposing_activity_fraction"] = np.where(
        scorecard["gross_activity_total"].gt(EPSILON),
        scorecard["opposing_activity_total"]
        / scorecard["gross_activity_total"],
        np.nan,
    )

    return scorecard.sort_values(
        ["geo_id", "mean_share_of_absolute_change"],
        ascending=[True, False],
    ).reset_index(drop=True)


def _pair_cancellation(
    left: pd.Series,
    right: pd.Series,
) -> pd.Series:
    """Return gross activity minus absolute net activity for a pair."""

    gross = left.abs() + right.abs()
    net = (left + right).abs()
    return gross - net


def build_dimension_pair_relationships(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    """Quantify same-month relationships between every dimension pair."""

    columns = resolve_long_columns(long_attribution)
    contribution_column = columns["contribution_change"]
    score_change_column = columns["score_change"]

    contribution_wide = long_attribution.pivot(
        index=["geo_id", "date"],
        columns="dimension",
        values=contribution_column,
    ).reset_index()

    score_change_wide = long_attribution.pivot(
        index=["geo_id", "date"],
        columns="dimension",
        values=score_change_column,
    ).reset_index()

    records: list[dict[str, object]] = []

    for geo_id, contribution_geo in contribution_wide.groupby(
        "geo_id",
        sort=True,
    ):
        contribution_geo = contribution_geo.sort_values("date")

        score_geo = (
            score_change_wide.loc[
                score_change_wide["geo_id"].eq(geo_id)
            ]
            .sort_values("date")
            .set_index("date")
        )

        contribution_geo = contribution_geo.set_index("date")

        for left_dimension, right_dimension in combinations(
            DIMENSIONS,
            2,
        ):
            left = contribution_geo[left_dimension]
            right = contribution_geo[right_dimension]

            left_score = score_geo[left_dimension]
            right_score = score_geo[right_dimension]

            valid = left.notna() & right.notna()

            left_valid = left.loc[valid]
            right_valid = right.loc[valid]

            both_active = (
                left_valid.abs().gt(EPSILON)
                & right_valid.abs().gt(EPSILON)
            )

            same_sign = (
                np.sign(left_valid.loc[both_active])
                == np.sign(right_valid.loc[both_active])
            )

            opposite_sign = (
                np.sign(left_valid.loc[both_active])
                == -np.sign(right_valid.loc[both_active])
            )

            joint_gross = (
                left_valid.abs() + right_valid.abs()
            )

            pair_cancellation = _pair_cancellation(
                left_valid,
                right_valid,
            )

            records.append(
                {
                    "geo_id": geo_id,
                    "dimension_left": left_dimension,
                    "dimension_right": right_dimension,
                    "observations": int(valid.sum()),
                    "both_active_observations": int(
                        both_active.sum()
                    ),
                    "contribution_correlation": (
                        left_valid.corr(right_valid)
                    ),
                    "absolute_activity_correlation": (
                        left_valid.abs().corr(
                            right_valid.abs()
                        )
                    ),
                    "score_change_correlation": (
                        left_score.corr(right_score)
                    ),
                    "same_sign_fraction_when_both_active": (
                        float(same_sign.mean())
                        if not same_sign.empty
                        else np.nan
                    ),
                    "opposite_sign_fraction_when_both_active": (
                        float(opposite_sign.mean())
                        if not opposite_sign.empty
                        else np.nan
                    ),
                    "mean_joint_gross_activity": (
                        float(joint_gross.mean())
                    ),
                    "total_joint_gross_activity": (
                        float(joint_gross.sum())
                    ),
                    "mean_pair_cancellation": (
                        float(pair_cancellation.mean())
                    ),
                    "total_pair_cancellation": (
                        float(pair_cancellation.sum())
                    ),
                    "pair_cancellation_rate": (
                        float(
                            pair_cancellation.sum()
                            / joint_gross.sum()
                        )
                        if joint_gross.sum() > EPSILON
                        else np.nan
                    ),
                }
            )

    result = pd.DataFrame(records)

    return result.sort_values(
        [
            "geo_id",
            "pair_cancellation_rate",
        ],
        ascending=[True, False],
    ).reset_index(drop=True)


def _effective_component_count(
    contribution_frame: pd.DataFrame,
) -> pd.Series:
    gross = contribution_frame.abs().sum(axis=1)
    squared = contribution_frame.pow(2).sum(axis=1)

    effective = gross.pow(2) / squared

    effective.loc[squared.le(EPSILON)] = np.nan
    return effective


def _cancellation_rate(
    contribution_frame: pd.DataFrame,
) -> pd.Series:
    gross = contribution_frame.abs().sum(axis=1)
    net = contribution_frame.sum(axis=1).abs()
    cancellation = gross - net

    rate = cancellation / gross
    rate.loc[gross.le(EPSILON)] = np.nan

    return rate


def build_dimension_sensitivity_analysis(
    long_attribution: pd.DataFrame,
) -> pd.DataFrame:
    """Build leave-one-dimension-out structural sensitivity diagnostics.

    Contributions are removed without redistributing or renormalizing weights.
    This is a diagnostic counterfactual, not a proposed production axis.
    """

    columns = resolve_long_columns(long_attribution)
    contribution_column = columns["contribution_change"]

    contribution_wide = long_attribution.pivot(
        index=["geo_id", "date"],
        columns="dimension",
        values=contribution_column,
    ).reset_index()

    records: list[dict[str, object]] = []

    for geo_id, geo_frame in contribution_wide.groupby(
        "geo_id",
        sort=True,
    ):
        geo_frame = geo_frame.sort_values("date").copy()

        baseline_components = geo_frame[list(DIMENSIONS)]
        baseline_change = baseline_components.sum(axis=1)

        baseline_gross = baseline_components.abs().sum(axis=1)
        baseline_cancellation_rate = _cancellation_rate(
            baseline_components
        )
        baseline_effective_count = _effective_component_count(
            baseline_components
        )

        # The first chronology month has no contribution changes, so every
        # component is NaN. Restrict idxmax() to rows containing at least one
        # finite value to avoid future pandas errors.

        baseline_dominant = pd.Series(
            pd.NA,
            index=baseline_components.index,
            dtype="object",
        )

        baseline_valid = baseline_components.notna().any(axis=1)

        baseline_dominant.loc[baseline_valid] = (
            baseline_components.loc[
                baseline_valid
            ]
            .abs()
            .idxmax(axis=1)
        )

        for removed_dimension in DIMENSIONS:
            remaining_dimensions = [
                dimension
                for dimension in DIMENSIONS
                if dimension != removed_dimension
            ]

            remaining_components = geo_frame[remaining_dimensions]

            counterfactual_change = remaining_components.sum(axis=1)
            counterfactual_gross = remaining_components.abs().sum(axis=1)

            counterfactual_cancellation_rate = _cancellation_rate(
                remaining_components
            )

            counterfactual_effective_count = (
                _effective_component_count(
                    remaining_components
                )
            )

            counterfactual_dominant = pd.Series(
                pd.NA,
                index=remaining_components.index,
                dtype="object",
            )

            counterfactual_valid = (
                remaining_components.notna().any(axis=1)
            )

            counterfactual_dominant.loc[
                counterfactual_valid
            ] = (
                remaining_components.loc[
                    counterfactual_valid
                ]
                .abs()
                .idxmax(axis=1)
            )

            dominant_changed = (
                baseline_dominant != counterfactual_dominant
            )

            axis_difference = (
                baseline_change - counterfactual_change
            )

            records.append(
                {
                    "geo_id": geo_id,
                    "removed_dimension": removed_dimension,
                    "observations": int(len(geo_frame)),
                    "first_date": geo_frame["date"].min(),
                    "last_date": geo_frame["date"].max(),
                    "baseline_mean_absolute_axis_change": float(
                        baseline_change.abs().mean()
                    ),
                    "counterfactual_mean_absolute_axis_change": float(
                        counterfactual_change.abs().mean()
                    ),
                    "change_in_mean_absolute_axis_change": float(
                        counterfactual_change.abs().mean()
                        - baseline_change.abs().mean()
                    ),
                    "baseline_axis_change_std": float(
                        baseline_change.std()
                    ),
                    "counterfactual_axis_change_std": float(
                        counterfactual_change.std()
                    ),
                    "change_in_axis_change_std": float(
                        counterfactual_change.std()
                        - baseline_change.std()
                    ),
                    "baseline_mean_gross_activity": float(
                        baseline_gross.mean()
                    ),
                    "counterfactual_mean_gross_activity": float(
                        counterfactual_gross.mean()
                    ),
                    "change_in_mean_gross_activity": float(
                        counterfactual_gross.mean()
                        - baseline_gross.mean()
                    ),
                    "baseline_mean_cancellation_rate": float(
                        baseline_cancellation_rate.mean()
                    ),
                    "counterfactual_mean_cancellation_rate": float(
                        counterfactual_cancellation_rate.mean()
                    ),
                    "change_in_mean_cancellation_rate": float(
                        counterfactual_cancellation_rate.mean()
                        - baseline_cancellation_rate.mean()
                    ),
                    "baseline_mean_effective_component_count": float(
                        baseline_effective_count.mean()
                    ),
                    "counterfactual_mean_effective_component_count": float(
                        counterfactual_effective_count.mean()
                    ),
                    "change_in_mean_effective_component_count": float(
                        counterfactual_effective_count.mean()
                        - baseline_effective_count.mean()
                    ),
                    "dominant_dimension_changed_fraction": float(
                        dominant_changed.mean()
                    ),
                    "mean_absolute_axis_difference": float(
                        axis_difference.abs().mean()
                    ),
                    "max_absolute_axis_difference": float(
                        axis_difference.abs().max()
                    ),
                    "axis_difference_std": float(
                        axis_difference.std()
                    ),
                }
            )

    result = pd.DataFrame(records)

    return result.sort_values(
        [
            "geo_id",
            "mean_absolute_axis_difference",
        ],
        ascending=[True, False],
    ).reset_index(drop=True)


def _min_max_scale(
    series: pd.Series,
) -> pd.Series:
    """Scale finite values to [0, 1] without fabricating variation."""

    result = pd.Series(
        np.nan,
        index=series.index,
        dtype=float,
    )

    finite = series.replace([np.inf, -np.inf], np.nan).dropna()

    if finite.empty:
        return result

    minimum = finite.min()
    maximum = finite.max()

    if np.isclose(minimum, maximum):
        result.loc[finite.index] = 0.0
        return result

    result.loc[finite.index] = (
        finite - minimum
    ) / (
        maximum - minimum
    )

    return result


def build_dimension_rankings(
    scorecard: pd.DataFrame,
    sensitivity: pd.DataFrame,
    pair_relationships: pd.DataFrame,
) -> pd.DataFrame:
    """Create a transparent investigation-priority ranking.

    This does not rank dimensions as good or bad and does not recommend new
    weights. It ranks which dimensions deserve deeper investigation first.

    Priority components:
    - observed share of absolute axis activity
    - frequency as dominant monthly contributor
    - leave-one-out axis sensitivity
    - involvement in pairwise cancellation
    """

    sensitivity_subset = sensitivity[
        [
            "geo_id",
            "removed_dimension",
            "mean_absolute_axis_difference",
            "dominant_dimension_changed_fraction",
        ]
    ].rename(
        columns={
            "removed_dimension": "dimension",
        }
    )

    pair_left = pair_relationships[
        [
            "geo_id",
            "dimension_left",
            "total_pair_cancellation",
        ]
    ].rename(
        columns={
            "dimension_left": "dimension",
        }
    )

    pair_right = pair_relationships[
        [
            "geo_id",
            "dimension_right",
            "total_pair_cancellation",
        ]
    ].rename(
        columns={
            "dimension_right": "dimension",
        }
    )

    pair_involvement = (
        pd.concat(
            [pair_left, pair_right],
            ignore_index=True,
        )
        .groupby(
            ["geo_id", "dimension"],
            as_index=False,
        )
        .agg(
            total_pair_cancellation_involvement=(
                "total_pair_cancellation",
                "sum",
            ),
        )
    )

    rankings = scorecard.merge(
        sensitivity_subset,
        on=["geo_id", "dimension"],
        how="left",
        validate="one_to_one",
    )

    rankings = rankings.merge(
        pair_involvement,
        on=["geo_id", "dimension"],
        how="left",
        validate="one_to_one",
    )

    component_columns = {
        "activity_priority_component":
            "mean_share_of_absolute_change",
        "dominance_priority_component":
            "dominant_month_fraction",
        "sensitivity_priority_component":
            "mean_absolute_axis_difference",
        "cancellation_priority_component":
            "total_pair_cancellation_involvement",
    }

    for output_column, source_column in component_columns.items():
        rankings[output_column] = (
            rankings.groupby("geo_id")[source_column]
            .transform(_min_max_scale)
        )

    rankings["diagnostic_priority_score"] = (
        0.35 * rankings["activity_priority_component"]
        + 0.25 * rankings["dominance_priority_component"]
        + 0.25 * rankings["sensitivity_priority_component"]
        + 0.15 * rankings["cancellation_priority_component"]
    )

    rankings["investigation_rank"] = (
        rankings.groupby("geo_id")[
            "diagnostic_priority_score"
        ]
        .rank(
            method="dense",
            ascending=False,
        )
        .astype(int)
    )

    rankings["priority_band"] = pd.cut(
        rankings["diagnostic_priority_score"],
        bins=[
            -np.inf,
            0.25,
            0.50,
            0.75,
            np.inf,
        ],
        labels=[
            "low",
            "moderate",
            "high",
            "very_high",
        ],
    ).astype(str)

    def build_reason(row: pd.Series) -> str:
        """
        Produce a human-readable explanation of why this dimension
        ranked where it did.

        This is intentionally descriptive rather than prescriptive.
        """

        reasons = []

        if row["activity_priority_component"] >= 0.75:
            reasons.append(
                f"Large share of axis movement "
                f"({row['mean_share_of_absolute_change']:.1%})"
            )

        if row["dominance_priority_component"] >= 0.75:
            reasons.append(
                f"Dominant contributor in "
                f"{row['dominant_month_fraction']:.1%} of months"
            )

        if row["sensitivity_priority_component"] >= 0.75:
            reasons.append(
                "Large leave-one-out axis impact"
            )

        if row["cancellation_priority_component"] >= 0.75:
            reasons.append(
                "Strong involvement in pairwise cancellation"
            )

        if not reasons:

            largest = max(
                [
                    (
                        row["activity_priority_component"],
                        (
                            f"Moderate share of axis movement "
                            f"({row['mean_share_of_absolute_change']:.1%})"
                        ),
                    ),
                    (
                        row["dominance_priority_component"],
                        (
                            f"Dominant in "
                            f"{row['dominant_month_fraction']:.1%} of months"
                        ),
                    ),
                    (
                        row["sensitivity_priority_component"],
                        "Moderate leave-one-out sensitivity",
                    ),
                    (
                        row["cancellation_priority_component"],
                        "Moderate pairwise cancellation involvement",
                    ),
                ],
                key=lambda x: x[0],
            )

            reasons.append(largest[1])

        return "; ".join(reasons)

    rankings["priority_reason"] = rankings.apply(
        build_reason,
        axis=1,
    )

    selected_columns = [
        "geo_id",
        "investigation_rank",
        "dimension",
        "priority_band",
        "diagnostic_priority_score",
        "configured_weight",
        "mean_share_of_absolute_change",
        "dominant_month_fraction",
        "mean_absolute_axis_difference",
        "dominant_dimension_changed_fraction",
        "total_pair_cancellation_involvement",
        "activity_priority_component",
        "dominance_priority_component",
        "sensitivity_priority_component",
        "cancellation_priority_component",
        "priority_reason",
    ]

    return rankings[selected_columns].sort_values(
        ["geo_id", "investigation_rank", "dimension"]
    ).reset_index(drop=True)


def validate_outputs(
    scorecard: pd.DataFrame,
    pair_relationships: pd.DataFrame,
    sensitivity: pd.DataFrame,
    rankings: pd.DataFrame,
) -> None:
    """Apply structural D4 validation invariants."""

    expected_dimensions = set(DIMENSIONS)
    expected_pair_count = len(list(combinations(DIMENSIONS, 2)))

    for geo_id, group in scorecard.groupby("geo_id"):
        observed = set(group["dimension"])
        if observed != expected_dimensions:
            raise AssertionError(
                f"Scorecard dimension mismatch for {geo_id}: "
                f"{sorted(observed)}"
            )

    for geo_id, group in pair_relationships.groupby("geo_id"):
        if len(group) != expected_pair_count:
            raise AssertionError(
                f"Expected {expected_pair_count} dimension pairs for "
                f"{geo_id}; observed {len(group)}"
            )

    for geo_id, group in sensitivity.groupby("geo_id"):
        observed = set(group["removed_dimension"])
        if observed != expected_dimensions:
            raise AssertionError(
                f"Sensitivity dimension mismatch for {geo_id}: "
                f"{sorted(observed)}"
            )

    for geo_id, group in rankings.groupby("geo_id"):
        observed = set(group["dimension"])
        if observed != expected_dimensions:
            raise AssertionError(
                f"Ranking dimension mismatch for {geo_id}: "
                f"{sorted(observed)}"
            )

        if set(group["investigation_rank"]) != set(
            range(1, len(DIMENSIONS) + 1)
        ):
            raise AssertionError(
                f"Ranking sequence is invalid for {geo_id}"
            )

    numeric_scorecard = scorecard.select_dtypes(
        include=[np.number]
    )

    if np.isinf(numeric_scorecard.to_numpy()).any():
        raise AssertionError(
            "Scorecard contains infinite numeric values."
        )


def write_outputs(
    scorecard: pd.DataFrame,
    pair_relationships: pd.DataFrame,
    sensitivity: pd.DataFrame,
    rankings: pd.DataFrame,
    *,
    d2_dir: Path,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    scorecard.to_csv(
        output_dir / "dimension_weight_scorecard.csv",
        index=False,
    )

    pair_relationships.to_csv(
        output_dir / "dimension_pair_relationships.csv",
        index=False,
    )

    sensitivity.to_csv(
        output_dir / "dimension_sensitivity_analysis.csv",
        index=False,
    )

    rankings.to_csv(
        output_dir / "dimension_rankings.csv",
        index=False,
    )

    manifest = {
        "phase": "D4",
        "status": "diagnostic",
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "purpose": (
            "Diagnose dimension influence, interaction, cancellation, "
            "and leave-one-dimension-out sensitivity under current "
            "production weights."
        ),
        "weight_policy": {
            "configured_weights_preserved": True,
            "alternative_weights_evaluated": False,
            "leave_one_out_renormalization": False,
        },
        "dimensions": list(DIMENSIONS),
        "inputs": {
            "d2_directory": str(d2_dir),
            "monthly_axis_attribution": str(
                d2_dir / "monthly_axis_attribution.csv"
            ),
            "monthly_dimension_contributions": str(
                d2_dir
                / "monthly_dimension_contributions.csv"
            ),
        },
        "outputs": [
            "dimension_weight_scorecard.csv",
            "dimension_pair_relationships.csv",
            "dimension_sensitivity_analysis.csv",
            "dimension_rankings.csv",
            "dimension_diagnostic_manifest.json",
        ],
        "ranking_formula": {
            "axis_activity": 0.35,
            "monthly_dominance": 0.25,
            "leave_one_out_sensitivity": 0.25,
            "pairwise_cancellation_involvement": 0.15,
            "interpretation": (
                "Ranks investigation priority only. It does not rank "
                "dimension quality and does not prescribe new weights."
            ),
        },
    }

    with (
        output_dir / "dimension_diagnostic_manifest.json"
    ).open("w", encoding="utf-8") as handle:
        json.dump(
            manifest,
            handle,
            indent=2,
            sort_keys=True,
        )


def run_dimension_weight_diagnostic(
    *,
    d2_dir: Path = DEFAULT_D2_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, pd.DataFrame]:
    """Execute the complete D4 diagnostic pipeline."""

    print(
        "[dimension_weight_diagnostic] "
        "stage=load_d2_artifacts"
    )

    monthly, long_attribution = load_d2_artifacts(
        d2_dir=d2_dir
    )

    print(f"  monthly_rows={len(monthly):,}")
    print(f"  long_rows={len(long_attribution):,}")
    print(
        "  geographies="
        f"{monthly['geo_id'].nunique():,}"
    )
    print(
        "  chronology="
        f"{monthly['date'].min().date()} to "
        f"{monthly['date'].max().date()}"
    )

    print(
        "\n[dimension_weight_diagnostic] "
        "stage=build_dimension_scorecard"
    )

    scorecard = build_dimension_weight_scorecard(
        long_attribution,
    )

    print(
        "\n[dimension_weight_diagnostic] "
        "stage=build_pair_relationships"
    )

    pair_relationships = (
        build_dimension_pair_relationships(
            long_attribution
        )
    )

    print(
        "\n[dimension_weight_diagnostic] "
        "stage=build_sensitivity_analysis"
    )

    sensitivity = build_dimension_sensitivity_analysis(
        long_attribution,
    )

    print(
        "\n[dimension_weight_diagnostic] "
        "stage=build_rankings"
    )

    rankings = build_dimension_rankings(
        scorecard,
        sensitivity,
        pair_relationships,
    )

    print(
        "\n[dimension_weight_diagnostic] "
        "stage=validation"
    )

    validate_outputs(
        scorecard,
        pair_relationships,
        sensitivity,
        rankings,
    )

    print("\nDimension investigation rankings:")
    print(
        rankings[
            [
                "geo_id",
                "investigation_rank",
                "dimension",
                "priority_band",
                "diagnostic_priority_score",
                "configured_weight",
                "mean_share_of_absolute_change",
                "dominant_month_fraction",
                "mean_absolute_axis_difference",
                "priority_reason",
            ]
        ].to_string(index=False)
    )

    print("\nHighest-cancellation dimension pairs:")
    print(
        pair_relationships[
            [
                "geo_id",
                "dimension_left",
                "dimension_right",
                "contribution_correlation",
                "opposite_sign_fraction_when_both_active",
                "pair_cancellation_rate",
            ]
        ]
        .sort_values(
            ["geo_id", "pair_cancellation_rate"],
            ascending=[True, False],
        )
        .groupby("geo_id", as_index=False)
        .head(3)
        .to_string(index=False)
    )

    write_outputs(
        scorecard,
        pair_relationships,
        sensitivity,
        rankings,
        d2_dir=d2_dir,
        output_dir=output_dir,
    )

    print("\nArtifacts written to:")
    print(output_dir)

    return {
        "scorecard": scorecard,
        "pair_relationships": pair_relationships,
        "sensitivity": sensitivity,
        "rankings": rankings,
    }


def main() -> None:
    run_dimension_weight_diagnostic()


if __name__ == "__main__":
    main()
