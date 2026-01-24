from __future__ import annotations
# forecast/models/xgb/backtest_forecast_runner.py

from typing import Optional, List

import numpy as np
import pandas as pd

from forecast.db_forecast import get_connection, new_batch_id, insert_run, insert_predictions
from forecast.backtest_utils import (
    choose_anchor_dates,
    month_end_index,
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
)

# XGBoost import path can vary; prefer your existing dependency style.
from xgboost import XGBRegressor


def _parse_data_asof(x: Optional[str], default_date: pd.Timestamp):
    if x is None or str(x).strip() == "":
        return default_date.date()
    return pd.to_datetime(x).date()


def load_target_series(metric_id: str, geo_id: str, property_type_id: str) -> pd.Series:
    con = get_connection()
    sql = """
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
        ORDER BY date
    """
    df = con.execute(sql, [metric_id, geo_id, property_type_id]).fetchdf()
    con.close()

    if df.empty:
        raise ValueError(f"No data for metric={metric_id}, geo={geo_id}, pt={property_type_id}")

    s = df.set_index("date")["value"].astype(float)
    return s


def _make_ar_lag_frame(y: pd.Series, lags: List[int]) -> pd.DataFrame:
    df = pd.DataFrame({"y": y})
    for L in lags:
        df[f"lag_{L}"] = y.shift(L)
    return df


def _fit_predict_ar_xgb(
    y: pd.Series,
    anchor_date: pd.Timestamp,
    horizon: int,
    lags: List[int],
    seed: int = 0,
) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """
    Train on y<=anchor_date, predict next horizon months (month-end grid) using recursive AR lags.
    Deterministic: fixed seed, no randomness beyond that.
    """
    y_train = y.loc[:anchor_date].copy()
    y_train = y_train.dropna()

    # supervised frame
    df = _make_ar_lag_frame(y_train, lags).dropna()
    X = df[[f"lag_{L}" for L in lags]].values
    yv = df["y"].values

    # Minimal, deterministic model params (do not tune in Phase C)
    model = XGBRegressor(
        n_estimators=500,
        max_depth=4,
        learning_rate=0.05,
        subsample=1.0,
        colsample_bytree=1.0,
        reg_lambda=1.0,
        random_state=seed,
        objective="reg:squarederror",
    )
    model.fit(X, yv)

    # recursive forecasts on month-end grid
    start_period = anchor_date.to_period("M")
    future_periods = [start_period + i for i in range(1, horizon + 1)]
    future_index = pd.DatetimeIndex([p.to_timestamp(how="end") for p in future_periods])

    history = y_train.copy()
    preds = []

    for dt in future_index:
        row = []
        for L in lags:
            # lag relative to dt: value at dt - L months end
            lag_dt = (dt.to_period("M") - L).to_timestamp(how="end")
            val = history.get(lag_dt, np.nan)
            row.append(val)
        x_row = np.array(row, dtype=float).reshape(1, -1)

        # if any lag missing, fall back to last observed (keeps runner from exploding)
        if np.isnan(x_row).any():
            yhat = float(history.dropna().iloc[-1])
        else:
            yhat = float(model.predict(x_row)[0])

        preds.append(yhat)
        history.loc[dt] = yhat

    return np.array(preds, dtype=float), future_index


def run_backtest_xgb_forecast(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
    *,
    lags: Optional[List[int]] = None,
    min_train_len: int = DEFAULT_MIN_TRAIN_LEN,
    anchor_step_months: int = DEFAULT_ANCHOR_STEP_MONTHS,
    max_anchors: int = DEFAULT_MAX_ANCHORS,
    latest_anchor_offset_months: Optional[int] = None,
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,
    anchors_csv: Optional[str] = None,
):
    batch_id = batch_id or new_batch_id()

    s = load_target_series(metric_id, geo_id, property_type_id)

    s = s.copy()
    s.index = month_end_index(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()

    
    # IMPORTANT: resolve + clamp as-of BEFORE choosing anchors
    data_asof_date = _parse_data_asof(data_asof, s.index.max())  # returns date
    data_asof_ts = pd.Timestamp(data_asof_date).to_period("M").to_timestamp(how="end")

    # clamp history to as-of (this makes as-of REAL, not cosmetic)
    s = s.loc[:data_asof_ts].copy()
    if s.empty:
        raise ValueError(f"[xgb_forecast_backtest] no target data <= data_asof={data_asof_date}")
    
    print(f"[xgb_forecast_backtest] batch_id={batch_id} data_asof={data_asof_date}")

    
    lags = lags or [1, 3, 6, 12]


    if anchors_csv:
        anchors = [pd.Timestamp(a.strip()) for a in anchors_csv.split(",") if a.strip()]
    else:
        anchors = choose_anchor_dates(
            s,
            horizon=horizon,
            min_train_len=min_train_len,
            step_months=anchor_step_months,
            max_anchors=max_anchors,
            latest_anchor_offset_months=latest_anchor_offset_months,
        )

    if not anchors:
        print("[xgb_forecast_backtest] Not enough history to run backtests.")
        return

    last_date = s.index[-1]
    results_summary = []

    for anchor_date in anchors:
        print(f"\n[xgb_forecast_backtest] Anchor at date={anchor_date.date()}")

        y_train = s.loc[:anchor_date]
        anchor_period = anchor_date.to_period("M")
        last_period = last_date.to_period("M")
        months_available = (last_period.year - anchor_period.year) * 12 + (last_period.month - anchor_period.month)

        horizon_bt = min(horizon, months_available)
        if horizon_bt <= 0:
            print("[xgb_forecast_backtest] No future months available for this anchor; skipping.")
            continue

        preds, future_index = _fit_predict_ar_xgb(
            y=s,
            anchor_date=anchor_date,
            horizon=horizon_bt,
            lags=lags,
            seed=0,
        )

        algo_params = {
            "lags": list(map(int, lags)),
            "n_obs": int(len(y_train)),
            "anchor_date": str(anchor_date.date()),
            "contracts": {
                "run_kind": "backtest",
                "anchor_date": str(anchor_date.date()),
                "data_asof_effective": str(data_asof_date),
                "target_metric_id": metric_id,
                "target_geo_id": geo_id,
                "target_property_type_id": property_type_id,
                "freq": "M",
                "train_start": str(y_train.index[0].date()),
                "train_end": str(anchor_date.date()),
                "horizon_max_months": int(horizon_bt),
            },
        }

        con = get_connection()
        run_id = insert_run(
            con=con,
            model_name="xgb_forecast",
            model_version="v1_ar_lags",
            target_metric_id=metric_id,
            target_geo_id=geo_id,
            target_property_type_id=property_type_id,
            freq="M",
            train_start=y_train.index[0].date(),
            train_end=anchor_date.date(),
            horizon_max_months=horizon_bt,
            algo_params=algo_params,
            notes=f"XGB forecast backtest anchor={anchor_date.date()}",
            is_active=False,
            run_kind="backtest",
            batch_id=batch_id,
            data_asof=data_asof_date,
        )

        target_dates = [d.date() for d in future_index]
        insert_predictions(
            con=con,
            run_id=run_id,
            target_dates=target_dates,
            y_hat=preds,
            y_hat_lo=None,
            y_hat_hi=None,
        )
        con.close()

        print(f"[xgb_forecast_backtest] Created backtest run_id={run_id} for anchor={anchor_date.date()}")
        results_summary.append({"anchor_date": anchor_date, "run_id": run_id})

    print("\n[xgb_forecast_backtest] Summary:")
    for r in results_summary:
        print(f"  anchor={r['anchor_date'].date()} -> run_id={r['run_id']}")


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Run XGB forecast backtests for a single target series.")
    p.add_argument("--metric_id", default="median_sale_price")
    p.add_argument("--geo_id", default="dc_city")
    p.add_argument("--property_type_id", default="-1")
    p.add_argument("--horizon", type=int, default=12)
    p.add_argument("--lags", type=str, default=None, help="Comma-separated lags, e.g. 1,2,3,6,12")
    p.add_argument("--batch_id", type=str, default=None)
    p.add_argument("--data_asof", type=str, default=None)
    p.add_argument("--anchors", type=str, default=None)

    args = p.parse_args(argv)

    lags = None
    if args.lags:
        lags = [int(x.strip()) for x in args.lags.split(",") if x.strip()]

    run_backtest_xgb_forecast(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
        lags=lags,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
        anchors_csv=args.anchors,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
