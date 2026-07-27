from __future__ import annotations
# regime/experiments/source_substitution.py

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd


SUBSTITUTION_LINEAGE_COLUMNS = [
    "substitution_id",
    "geo_id",
    "date",
    "canonical_metric_key",
    "original_value",
    "replacement_value",
    "output_value",
    "replacement_available",
    "row_action",
]


@dataclass(frozen=True)
class SourceSubstitutionResult:
    source_metrics: pd.DataFrame
    substitution_lineage: pd.DataFrame


def _validate_source_metrics(
    source_metrics: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
    }

    missing = required - set(
        source_metrics.columns
    )

    if missing:
        raise ValueError(
            "Source substitution input is missing "
            f"required columns: {sorted(missing)}"
        )

    work = source_metrics.copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["value"] = pd.to_numeric(
        work["value"],
        errors="coerce",
    )

    invalid = work[
        work["date"].isna()
        | work["value"].isna()
        | ~np.isfinite(
            work["value"]
        )
    ]

    if not invalid.empty:
        raise ValueError(
            "Source substitution input contains "
            "invalid rows:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicate_mask = work.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise ValueError(
            "Source substitution input is not "
            "unique by geo/date/metric:\n"
            + work.loc[
                duplicate_mask
            ].head(30).to_string(
                index=False
            )
        )

    return work


def _validate_replacement_series(
    replacement_series: pd.DataFrame,
    *,
    metric_key: str,
    replacement_value_column: str,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        replacement_value_column,
    }

    missing = required - set(
        replacement_series.columns
    )

    if missing:
        raise ValueError(
            "Replacement series is missing "
            f"required columns: {sorted(missing)}"
        )

    replacement = replacement_series[
        [
            "geo_id",
            "date",
            replacement_value_column,
        ]
    ].copy()

    replacement["date"] = pd.to_datetime(
        replacement["date"],
        errors="coerce",
    )

    replacement[
        replacement_value_column
    ] = pd.to_numeric(
        replacement[
            replacement_value_column
        ],
        errors="coerce",
    )

    duplicate_mask = replacement.duplicated(
        subset=[
            "geo_id",
            "date",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise ValueError(
            f"Replacement series for {metric_key!r} "
            "is not unique by geo/date:\n"
            + replacement.loc[
                duplicate_mask
            ].head(30).to_string(
                index=False
            )
        )

    invalid_dates = replacement[
        replacement["date"].isna()
    ]

    if not invalid_dates.empty:
        raise ValueError(
            f"Replacement series for {metric_key!r} "
            "contains invalid dates"
        )

    finite_or_missing = (
        replacement[
            replacement_value_column
        ].isna()
        | np.isfinite(
            replacement[
                replacement_value_column
            ]
        )
    )

    if not finite_or_missing.all():
        raise ValueError(
            f"Replacement series for {metric_key!r} "
            "contains infinite values"
        )

    return replacement.rename(
        columns={
            replacement_value_column: (
                "replacement_value"
            ),
        }
    )


def apply_metric_source_substitution(
    source_metrics: pd.DataFrame,
    replacement_series: pd.DataFrame,
    *,
    metric_key: str,
    substitution_id: str,
    replacement_value_column: str = "value",
    missing_policy: Literal[
        "null",
        "drop",
        "error",
    ] = "null",
) -> SourceSubstitutionResult:
    """
    Replace one canonical source metric with an externally calculated
    series while preserving every unrelated source row exactly.

    Parameters
    ----------
    source_metrics:
        Canonical source observations. Must be unique by
        geo_id/date/canonical_metric_key.

    replacement_series:
        Replacement observations keyed by geo_id/date. The replacement
        value may be missing during a transform warm-up period.

    metric_key:
        Canonical source metric to replace.

    substitution_id:
        Immutable experiment/policy identifier persisted in lineage.

    replacement_value_column:
        Column in replacement_series containing the replacement value.

    missing_policy:
        "null":
            Preserve the original target row and set its value to
            missing when no replacement is available. This preserves
            the monthly source-date scaffold without silently falling
            back to the original raw value.

        "drop":
            Remove target-metric rows where the replacement is missing.

        "error":
            Require a valid replacement for every original target row.

    Returns
    -------
    SourceSubstitutionResult:
        source_metrics:
            Substituted canonical source panel.

        substitution_lineage:
            One row per original target observation showing whether it
            was replaced or dropped.

    Notes
    -----
    The function does not recalculate derived metrics. Call the existing
    canonical derived-metric builder after substitution.
    """
    if not substitution_id.strip():
        raise ValueError(
            "substitution_id must be non-empty"
        )

    if missing_policy not in {
        "null",
        "drop",
        "error",
    }:
        raise ValueError(
            "Unsupported missing_policy: "
            f"{missing_policy!r}"
        )

    source = _validate_source_metrics(
        source_metrics
    )

    replacement = (
        _validate_replacement_series(
            replacement_series,
            metric_key=metric_key,
            replacement_value_column=(
                replacement_value_column
            ),
        )
    )

    target = source[
        source[
            "canonical_metric_key"
        ].eq(metric_key)
    ].copy()

    if target.empty:
        raise ValueError(
            "Source panel contains no rows for "
            f"metric_key={metric_key!r}"
        )

    unrelated = source[
        ~source[
            "canonical_metric_key"
        ].eq(metric_key)
    ].copy()

    target_keys = target[
        [
            "geo_id",
            "date",
        ]
    ]

    extra_replacements = replacement.merge(
        target_keys,
        on=[
            "geo_id",
            "date",
        ],
        how="left",
        indicator=True,
        validate="one_to_one",
    )

    extra_replacements = extra_replacements[
        extra_replacements[
            "_merge"
        ].eq("left_only")
    ]

    if not extra_replacements.empty:
        raise ValueError(
            "Replacement series contains geo/date "
            "keys absent from the original target "
            f"metric {metric_key!r}:\n"
            + extra_replacements.head(
                30
            ).to_string(
                index=False
            )
        )

    merged = target.merge(
        replacement,
        on=[
            "geo_id",
            "date",
        ],
        how="left",
        validate="one_to_one",
    )

    merged["original_value"] = (
        merged["value"]
    )

    merged["replacement_available"] = (
        merged[
            "replacement_value"
        ].notna()
    )

    if (
        missing_policy == "error"
        and not merged[
            "replacement_available"
        ].all()
    ):
        missing_rows = merged[
            ~merged[
                "replacement_available"
            ]
        ]

        raise ValueError(
            "Replacement series does not cover every "
            f"{metric_key!r} source row:\n"
            + missing_rows[
                [
                    "geo_id",
                    "date",
                    "original_value",
                ]
            ].head(30).to_string(
                index=False
            )
        )

    merged["output_value"] = np.where(
        merged[
            "replacement_available"
        ],
        merged[
            "replacement_value"
        ],
        np.nan,
    )

    merged["row_action"] = np.where(
        merged[
            "replacement_available"
        ],
        "replace",
        missing_policy,
    )

    lineage = merged[
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "original_value",
            "replacement_value",
            "output_value",
            "replacement_available",
            "row_action",
        ]
    ].copy()

    lineage.insert(
        0,
        "substitution_id",
        substitution_id,
    )

    if missing_policy == "null":
        output_target = merged[
            source.columns
        ].copy()

        output_target["value"] = (
            merged["replacement_value"]
        )

    elif missing_policy == "drop":
        output_target = merged[
            merged[
                "replacement_available"
            ]
        ][
            source.columns
        ].copy()

        output_target["value"] = merged.loc[
            merged[
                "replacement_available"
            ],
            "replacement_value",
        ].to_numpy()

    else:
        output_target = merged[
            source.columns
        ].copy()

        output_target["value"] = (
            merged["replacement_value"]
        )

    output = pd.concat(
        [
            unrelated,
            output_target,
        ],
        ignore_index=True,
    )

    output = output.sort_values(
        [
            "geo_id",
            "canonical_metric_key",
            "date",
        ]
    ).reset_index(
        drop=True
    )

    duplicate_output = output.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_output.any():
        raise AssertionError(
            "Source substitution produced duplicate "
            "canonical rows"
        )

    unrelated_before = unrelated.sort_values(
        list(
            unrelated.columns
        )
    ).reset_index(
        drop=True
    )

    unrelated_after = output[
        ~output[
            "canonical_metric_key"
        ].eq(metric_key)
    ].sort_values(
        list(
            unrelated.columns
        )
    ).reset_index(
        drop=True
    )

    try:
        pd.testing.assert_frame_equal(
            unrelated_before,
            unrelated_after,
            check_dtype=True,
            check_exact=True,
        )
    except AssertionError as exc:
        raise AssertionError(
            "Source substitution changed unrelated "
            "canonical source rows"
        ) from exc

    return SourceSubstitutionResult(
        source_metrics=output,
        substitution_lineage=(
            lineage[
                SUBSTITUTION_LINEAGE_COLUMNS
            ]
            .sort_values(
                [
                    "geo_id",
                    "date",
                ]
            )
            .reset_index(
                drop=True
            )
        ),
    )
