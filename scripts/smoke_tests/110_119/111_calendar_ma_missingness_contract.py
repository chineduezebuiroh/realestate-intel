"""Production smoke for the governed calendar-MA missingness contract."""
from __future__ import annotations

import numpy as np
import pandas as pd

from regime._01_feature_engine import _compute_feature
from regime.calendar_ma import (
    MA_MIN_COVERAGE_FRACTION,
    calendar_moving_average,
    calendarize_comparable_series,
    minimum_valid_observations,
)
from regime.smoothing_features import build_smoothed_metric_features_wide
from regime.smoothing_policy import SmoothingMetricPolicy
from regime.pandas_compat import MONTH_END


def grid(values, origin="laus"):
    dates = pd.date_range("2024-01-31", periods=len(values), freq=MONTH_END)
    return pd.DataFrame({"geo_id": "g", "date": dates, "canonical_metric_key": "m", "value": values, "metric_origin": origin})


assert MA_MIN_COVERAGE_FRACTION == 2 / 3
for window, threshold in ((3, 2), (6, 4), (9, 6), (12, 8)):
    assert minimum_valid_observations(window) == threshold
    passing = pd.Series([float(i + 1) if i < threshold else np.nan for i in range(window)])
    failing = passing.copy(); failing.iloc[threshold - 1] = np.nan
    result = calendar_moving_average(passing, window)
    assert result.iloc[-1].valid_observation_count == threshold
    assert result.iloc[-1].expected_observation_count == window
    assert np.isclose(result.iloc[-1].calendar_ma, passing.dropna().mean())
    assert np.isnan(calendar_moving_average(failing, window).iloc[-1].calendar_ma)

# Complete histories retain the former full-window arithmetic result to 1e-12.
complete = grid(np.arange(1.0, 31.0))
legacy = complete.value.rolling(6, min_periods=6).mean()
production = _compute_feature(complete, "ma_level", "6m", "test")
assert np.nanmax(np.abs(legacy - production)) < 1e-12

# LAUS-shaped shutdown fixture: October is a calendar evaluation row, never zero.
laus = grid(np.arange(1.0, 16.0))
laus["date"] = pd.date_range("2025-05-31", periods=15, freq=MONTH_END)
laus = laus[laus.date.ne(pd.Timestamp("2025-10-31"))]
calendar = calendarize_comparable_series(laus).reset_index(drop=True)
ma6 = calendar_moving_average(calendar.value, 6)
calendar = pd.concat([calendar, ma6], axis=1)
october = calendar.loc[calendar.date.eq(pd.Timestamp("2025-10-31"))].iloc[0]
assert np.isnan(october.value) and october.valid_observation_count == 5
assert np.isclose(october.calendar_ma, np.mean([1, 2, 3, 4, 5]))
for date in pd.date_range("2025-10-31", "2026-03-31", freq=MONTH_END):
    row = calendar.loc[calendar.date.eq(date)].iloc[0]
    assert row.valid_observation_count == 5 and pd.notna(row.calendar_ma)
april = calendar.loc[calendar.date.eq(pd.Timestamp("2026-04-30"))].iloc[0]
assert april.valid_observation_count == 6

# Multiple gaps pass at 4/6 and fail at 3/6, without zero contribution.
assert np.isclose(calendar_moving_average(pd.Series([1., np.nan, 3., np.nan, 5., 7.]), 6).iloc[-1].calendar_ma, 4.0)
assert np.isnan(calendar_moving_average(pd.Series([1., np.nan, 3., np.nan, 5., np.nan]), 6).iloc[-1].calendar_ma)

# Shifts on the complete grid are exact calendar lags despite missing raw rows.
for lag in (3, 12):
    shifted_dates = calendar.date.shift(lag)
    valid = shifted_dates.notna()
    months = (calendar.loc[valid, "date"].dt.to_period("M").astype(int).to_numpy() - shifted_dates[valid].dt.to_period("M").astype(int).to_numpy())
    assert np.all(months == lag)

# Unlike origins are hard boundaries and the ambiguous intervening month is absent.
boundary = grid([1., 2., 3., 4.])
boundary["date"] = pd.to_datetime(["2025-01-31", "2025-02-28", "2025-04-30", "2025-05-31"])
boundary["metric_origin"] = ["A", "A", "B", "B"]
segmented = calendarize_comparable_series(boundary)
assert pd.Timestamp("2025-03-31") not in set(segmented.date)
assert segmented.source_origin_segment.nunique() == 2

# Challenger and production call the same calendar helper and agree exactly.
policy = SmoothingMetricPolicy(experiment_id="smoke", metric_key="m", policy_role="direct", transform_strategy="ma_momentum", level_window=6, short_window=6, short_lag_periods=3, long_window=6, long_lag_periods=12, recompute_dependents=False)
challenger_input = laus.rename(columns={"value": "raw_value"})
challenger = build_smoothed_metric_features_wide(challenger_input, policy=policy)
expected = _compute_feature(calendar.rename(columns={"calendar_ma": "unused"}), "ma_level", "6m", "test")
assert np.nanmax(np.abs(challenger.level_ma_value.to_numpy() - expected.to_numpy())) < 1e-12
assert not challenger.duplicated(["geo_id", "date", "canonical_metric_key"]).any()
pd.testing.assert_frame_equal(challenger, build_smoothed_metric_features_wide(challenger_input, policy=policy))

print("Calendar MA missingness contract smoke test passed")
