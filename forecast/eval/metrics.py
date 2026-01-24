from __future__ import annotations
# forecast/eval/metrics.py

import numpy as np
import pandas as pd


def _safe_div(n: float, d: float) -> float:
    return float(n) / float(d) if d not in (0, 0.0) else float("nan")


def rmse(err: pd.Series) -> float:
    return float(np.sqrt(np.mean(np.square(err.astype(float).values))))


def mae(err: pd.Series) -> float:
    return float(np.mean(np.abs(err.astype(float).values)))


def mape(y_true: pd.Series, y_hat: pd.Series) -> float:
    y_true = y_true.astype(float)
    y_hat = y_hat.astype(float)
    denom = y_true.replace(0.0, np.nan).abs()
    return float(np.nanmean(((y_true - y_hat).abs() / denom).values))


def smape(y_true: pd.Series, y_hat: pd.Series) -> float:
    y_true = y_true.astype(float)
    y_hat = y_hat.astype(float)
    denom = (y_true.abs() + y_hat.abs()).replace(0.0, np.nan)
    return float(np.nanmean((2.0 * (y_true - y_hat).abs() / denom).values))


def wape(y_true: pd.Series, y_hat: pd.Series) -> float:
    y_true = y_true.astype(float)
    y_hat = y_hat.astype(float)
    num = float((y_true - y_hat).abs().sum())
    den = float(y_true.abs().sum())
    return _safe_div(num, den)


def interval_coverage(y_true: pd.Series, lo: pd.Series, hi: pd.Series) -> float:
    # proportion of y_true inside [lo, hi]
    y_true = y_true.astype(float)
    lo = lo.astype(float)
    hi = hi.astype(float)
    ok = (y_true >= lo) & (y_true <= hi)
    return float(np.mean(ok.values))


def interval_width(lo: pd.Series, hi: pd.Series) -> float:
    return float(np.mean((hi.astype(float) - lo.astype(float)).values))
