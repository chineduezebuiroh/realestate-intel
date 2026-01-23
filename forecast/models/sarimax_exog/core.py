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
    model = SARIMAX(
        endog=y_train,
        exog=X_train,
        order=spec.order,
        seasonal_order=spec.seasonal_order,
        enforce_stationarity=spec.enforce_stationarity,
        enforce_invertibility=spec.enforce_invertibility,
    )
    res = model.fit(disp=False)
    return res


def forecast_sarimax_exog(
    res,
    X_future: pd.DataFrame,
    steps: int,
) -> Tuple[np.ndarray, Optional[np.ndarray]]:
    fc = res.get_forecast(steps=steps, exog=X_future)
    mean_fc = fc.predicted_mean.values.astype(float)
    ci = None
    try:
        ci = fc.conf_int().values.astype(float)
    except Exception:
        ci = None
    return mean_fc, ci
