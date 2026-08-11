"""Governed calendar-month moving-average utilities.

Moving averages use trailing calendar months, require two-thirds coverage, and
average only observed values.  Calendar rows are evaluation dates; a missing
raw observation is never filled or interpreted as zero.
"""
from __future__ import annotations

from math import ceil

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END


MA_MIN_COVERAGE_FRACTION = 2 / 3


def minimum_valid_observations(window_months: int) -> int:
    if window_months <= 0:
        raise ValueError("window_months must be positive")
    return ceil(window_months * MA_MIN_COVERAGE_FRACTION)


def calendarize_comparable_series(
    observations: pd.DataFrame,
    *,
    value_column: str = "value",
    origin_column: str | None = "metric_origin",
) -> pd.DataFrame:
    """Expand each proven source-origin run onto an explicit month-end grid.

    A gap bracketed by the same origin belongs to that comparable run.  A real
    origin change starts a new grid; months between unlike origins are omitted
    because their lineage is ambiguous.
    """
    if observations.empty:
        return observations.copy()
    work = observations.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.to_period("M").dt.to_timestamp("M")
    work[value_column] = pd.to_numeric(work[value_column], errors="coerce")
    work = work.sort_values("date")
    if work["date"].duplicated().any():
        raise ValueError("calendar MA input contains duplicate calendar months")

    if origin_column and origin_column in work:
        origins = work[origin_column].replace("", np.nan)
        if origins.isna().any():
            raise ValueError("calendar MA origin lineage is ambiguous")
        segment = origins.ne(origins.shift()).cumsum()
    else:
        segment = pd.Series(1, index=work.index)

    frames: list[pd.DataFrame] = []
    for segment_id, observed in work.groupby(segment, sort=False):
        grid = pd.DataFrame({
            "date": pd.date_range(observed["date"].iloc[0], observed["date"].iloc[-1], freq=MONTH_END)
        })
        merged = grid.merge(observed, on="date", how="left", suffixes=("", "_observed"))
        merged["source_origin_segment"] = int(segment_id)
        for column in observed.columns.difference(["date", value_column]):
            if observed[column].nunique(dropna=False) == 1:
                merged[column] = observed[column].iloc[0]
        if origin_column and origin_column in work:
            merged[origin_column] = observed[origin_column].iloc[0]
        frames.append(merged)
    return pd.concat(frames, ignore_index=True)


def calendar_moving_average(values: pd.Series, window_months: int) -> pd.DataFrame:
    """Return the governed MA and deterministic coverage diagnostics."""
    numeric = pd.to_numeric(values, errors="coerce")
    minimum = minimum_valid_observations(window_months)
    valid = numeric.notna().rolling(window_months, min_periods=1).sum().astype("int64")
    expected = pd.Series(window_months, index=numeric.index, dtype="int64")
    mean = numeric.rolling(window_months, min_periods=minimum).mean()
    # A comparable segment must first establish a complete calendar horizon.
    # Thereafter, missing raw observations may consume the coverage allowance.
    if window_months > 1:
        mean.iloc[: window_months - 1] = np.nan
    return pd.DataFrame({
        "calendar_ma": mean,
        "valid_observation_count": valid,
        "expected_observation_count": expected,
        "coverage_fraction": valid / expected,
    }, index=numeric.index)
