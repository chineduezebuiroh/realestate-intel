from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Dict, Any, Tuple

import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX


@dataclass(frozen=True)
class SarimaxExogSpec:
    order: tuple[int, int, int] = (1, 1, 1)
    seasonal_order: tuple[int, int, int, int] = (1, 1, 1, 12)
    enforce_stationarity: bool = False
    enforce_invertibility: bool = False


def fit_sarimax_exog(
    y_train: pd.Series,
    X_train: pd.DataFrame,
    spec: SarimaxExogSpec,
):
    # Pass numpy arrays to avoid statsmodels index/freq issues
    y_arr = y_train.astype(float).to_numpy()
    X_arr = X_train.astype(float).to_numpy()

    model = SARIMAX(
        endog=y_arr,
        exog=X_arr,
        order=spec.order,
        seasonal_order=spec.seasonal_order,
        enforce_stationarity=spec.enforce_stationarity,
        enforce_invertibility=spec.enforce_invertibility,
    )
    #res = model.fit(disp=False)
    res = model.fit(disp=False, maxiter=250)
    return res


def forecast_sarimax_exog(
    res,
    X_future: pd.DataFrame,
    steps: int,
) -> Tuple[np.ndarray, Optional[np.ndarray]]:
    Xf_arr = X_future.astype(float).to_numpy()

    fc = res.get_forecast(steps=steps, exog=Xf_arr)
    mean_fc = fc.predicted_mean.astype(float)
    ci = None
    try:
        ci = fc.conf_int().astype(float)
    except Exception:
        ci = None

    # Normalize outputs to numpy arrays
    mean_fc = np.asarray(mean_fc, dtype=float)
    ci = np.asarray(ci, dtype=float) if ci is not None else None
    return mean_fc, ci

