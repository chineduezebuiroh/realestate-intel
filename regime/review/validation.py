from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

import numpy as np
import pandas as pd


def _frame_label(frame_name: str | None) -> str:
    return frame_name if frame_name else "dataframe"


def assert_non_empty(
    frame: pd.DataFrame,
    *,
    frame_name: str | None = None,
) -> None:
    if frame.empty:
        raise AssertionError(f"{_frame_label(frame_name)} is empty")


def assert_required_columns(
    frame: pd.DataFrame,
    required_columns: Iterable[str],
    *,
    frame_name: str | None = None,
) -> None:
    missing = sorted(set(required_columns).difference(frame.columns))
    if missing:
        raise AssertionError(
            f"{_frame_label(frame_name)} is missing required columns: {missing}"
        )


def assert_finite(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    frame_name: str | None = None,
    allow_null: bool = True,
) -> None:
    assert_required_columns(frame, columns, frame_name=frame_name)
    for column in columns:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if not allow_null and numeric.isna().any():
            raise AssertionError(
                f"{_frame_label(frame_name)}.{column} contains null or non-numeric values"
            )
        finite_values = numeric.dropna()
        if not np.isfinite(finite_values.to_numpy()).all():
            raise AssertionError(
                f"{_frame_label(frame_name)}.{column} contains non-finite values"
            )


def assert_no_duplicate_keys(
    frame: pd.DataFrame,
    key_columns: Sequence[str],
    *,
    frame_name: str | None = None,
) -> None:
    assert_required_columns(frame, key_columns, frame_name=frame_name)
    duplicate_mask = frame.duplicated(subset=list(key_columns), keep=False)
    if duplicate_mask.any():
        sample = (
            frame.loc[duplicate_mask, list(key_columns)]
            .head(10)
            .to_dict(orient="records")
        )
        raise AssertionError(
            f"{_frame_label(frame_name)} contains duplicate keys for "
            f"{list(key_columns)}; sample={sample}"
        )


def assert_expected_values(
    frame: pd.DataFrame,
    column: str,
    expected_values: Iterable[Any],
    *,
    frame_name: str | None = None,
    exact: bool = True,
) -> None:
    assert_required_columns(frame, (column,), frame_name=frame_name)
    actual = set(frame[column].dropna().unique())
    expected = set(expected_values)
    valid = actual == expected if exact else expected.issubset(actual)
    if not valid:
        raise AssertionError(
            f"{_frame_label(frame_name)}.{column} values differ; "
            f"actual={sorted(actual)} expected={sorted(expected)} exact={exact}"
        )


def _assert_same_value_set(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    column: str,
    left_name: str,
    right_name: str,
) -> None:
    assert_required_columns(left, (column,), frame_name=left_name)
    assert_required_columns(right, (column,), frame_name=right_name)
    left_values = set(left[column].dropna().unique())
    right_values = set(right[column].dropna().unique())
    if left_values != right_values:
        raise AssertionError(
            f"{column} sets differ between {left_name} and {right_name}; "
            f"only_left={sorted(left_values - right_values)} "
            f"only_right={sorted(right_values - left_values)}"
        )


def assert_same_geographies(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    geo_column: str = "geo_id",
    left_name: str = "left",
    right_name: str = "right",
) -> None:
    _assert_same_value_set(
        left,
        right,
        column=geo_column,
        left_name=left_name,
        right_name=right_name,
    )


def assert_same_dates(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    date_column: str = "date",
    left_name: str = "left",
    right_name: str = "right",
) -> None:
    _assert_same_value_set(
        left,
        right,
        column=date_column,
        left_name=left_name,
        right_name=right_name,
    )
