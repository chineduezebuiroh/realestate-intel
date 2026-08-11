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
    work["date"] = pd.to_datetime(work["date"])
    work["_calendar_month"] = work["date"].dt.to_period("M")
    work[value_column] = pd.to_numeric(
        work[value_column],
        errors="coerce",
    )
    work = work.sort_values("_calendar_month")

    if work["_calendar_month"].duplicated().any():
        raise ValueError(
            "calendar MA input contains duplicate calendar months"
        )

    if origin_column and origin_column in work:
        origins = work[origin_column].replace("", np.nan)
        if origins.isna().any():
            raise ValueError("calendar MA origin lineage is ambiguous")
        segment = origins.ne(origins.shift()).cumsum()
    else:
        segment = pd.Series(1, index=work.index)

    frames: list[pd.DataFrame] = []

    for segment_id, observed in work.groupby(
        segment,
        sort=False,
    ):
        observed = observed.copy()

        # Calendar-window arithmetic is month based, but persisted feature
        # dates must preserve the source series' established monthly anchor.
        #
        # Month-start and month-end are both valid production conventions.
        # Do not silently migrate one convention to the other.
        dates = pd.to_datetime(observed["date"])

        all_month_start = dates.dt.is_month_start.all()
        all_month_end = dates.dt.is_month_end.all()

        if all_month_start:
            anchor = "start"
        elif all_month_end:
            anchor = "end"
        else:
            raise ValueError(
                "calendar MA source series has ambiguous monthly "
                "date anchoring; expected consistently month-start "
                "or month-end"
            )

        months = pd.period_range(
            observed["_calendar_month"].iloc[0],
            observed["_calendar_month"].iloc[-1],
            freq="M",
        )

        grid = pd.DataFrame(
            {"_calendar_month": months}
        )

        merged = grid.merge(
            observed,
            on="_calendar_month",
            how="left",
            suffixes=("", "_observed"),
            validate="one_to_one",
        )

        # Preserve exact observed dates where source rows exist.
        # Only synthesized missing-calendar rows need a generated date.
        generated_date = (
            merged["_calendar_month"].dt.to_timestamp(
                how="start"
            )
            if anchor == "start"
            else merged["_calendar_month"].dt.to_timestamp(
                "M"
            )
        )

        merged["date"] = pd.to_datetime(
            merged["date"]
        ).fillna(generated_date)

        merged["source_origin_segment"] = int(
            segment_id
        )

        for column in observed.columns.difference(
            [
                "date",
                "_calendar_month",
                value_column,
            ]
        ):
            if observed[column].nunique(
                dropna=False
            ) == 1:
                merged[column] = observed[column].iloc[0]

        if origin_column and origin_column in work:
            merged[origin_column] = (
                observed[origin_column].iloc[0]
            )

        frames.append(
            merged.drop(
                columns=["_calendar_month"]
            )
        )

    return pd.concat(
        frames,
        ignore_index=True,
    )


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
