from __future__ import annotations

# forecast/exog_forecast.py

import numpy as np
import pandas as pd

def forecast_exog_seasonal_naive(
    s_hist: pd.Series,
    horizon_idx: pd.DatetimeIndex,
    season: int = 12,
) -> pd.Series:
    """
    Seasonal naive forecast:
      x_hat[t] = x[t-season] if available else last observed x
    Assumes month-end index.
    """
    s_hist = s_hist.dropna().copy()
    if s_hist.empty:
        # nothing to forecast from
        return pd.Series(index=horizon_idx, dtype=float)

    last_val = float(s_hist.iloc[-1])

    # Build forecast by looking back season months for each horizon date.
    out = []
    for dt in horizon_idx:
        dt = pd.Timestamp(dt)
        lookback = (dt.to_period("M") - season).to_timestamp(how="end")
        if lookback in s_hist.index and pd.notna(s_hist.loc[lookback]):
            out.append(float(s_hist.loc[lookback]))
        else:
            out.append(last_val)

    return pd.Series(out, index=horizon_idx, name=s_hist.name)
