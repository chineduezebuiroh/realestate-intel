# forecast/backtest_sarimax_single.py

import os
from typing import List, Dict

import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from .db_forecast import (
    get_connection,
    new_batch_id,
    insert_run,
    insert_predictions,
)

from .backtest_utils import (
    choose_anchor_dates, 
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)


# ==========================================================
# Core backtest logic
# ==========================================================
def load_target_series(
    metric_id: str,
    geo_id: str,
    property_type_id: str,
) -> pd.Series:
    """
    Load the full target series from fact_timeseries as a pandas Series indexed by date.
    """
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

    if df.empty:
        raise ValueError(
            f"No data for metric={metric_id}, geo={geo_id}, pt={property_type_id}"
        )

    s = df.set_index("date")["value"].astype(float)
    return s


def choose_anchor_indices(s: pd.Series, horizon: int, min_train_len: int = 60) -> List[int]:
    """
    Choose a few anchor indices for backtesting.

    Strategy:
      - Work backwards from the end, about 1, 2, 3 years before the last date (12-month steps),
        as long as we have:
          * enough training data before the anchor (>= min_train_len)
          * enough future data (>= horizon months) between anchor and last date
    """
    n = len(s)
    if n < (min_train_len + horizon):
        # Not enough total history to do a meaningful backtest
        return []

    last_idx = n - 1

    # We want anchors so that: anchor_idx >= min_train_len-1
    # and anchor_idx + horizon <= last_idx
    # We'll try K anchors at 12-month steps from the last-trainable position.
    max_anchor_idx = last_idx - horizon  # last index we can use as anchor
    min_anchor_idx = min_train_len - 1

    anchors = []
    # Start from the latest possible anchor and step back 12 points (months)
    idx = max_anchor_idx
    while idx >= min_anchor_idx and len(anchors) < 3:
        anchors.append(idx)
        idx -= 12

    anchors.sort()
    return anchors


def run_backtest_sarimax_single(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
):
    """
    Run a few SARIMAX backtest folds for a single target series.

    For each anchor date:
      - Train on data <= anchor
      - Forecast up to horizon months ahead (but not beyond last observed date)
      - Store as backtest runs in forecast tables (is_active=FALSE)
    """
    s = load_target_series(metric_id, geo_id, property_type_id)

    s = s.copy()
    s.index = pd.PeriodIndex(s.index, freq="M").to_timestamp(how="end")

    batch_id = new_batch_id()
    data_asof = s.index.max().date()
    print(f"[backtest] batch_id={batch_id} data_asof={data_asof}")

    anchors = choose_anchor_dates(
        s,
        horizon=horizon,
        min_train_len=DEFAULT_MIN_TRAIN_LEN,
        step_months=args.anchor_step_months,
        max_anchors=args.max_anchors,
        latest_anchor_offset_months=args.latest_anchor_offset_months,
    )

    if not anchors:
        print("[backtest] Not enough history to run backtests.")
        return
    
    print(f"[backtest] Found {len(anchors)} anchors.")
    
    last_date = s.index[-1]
    results_summary = []

    for anchor_date in anchors:
        print(f"\n[backtest] Anchor at date={anchor_date.date()}")

        # Training series: all data up to and including anchor_date
        y_train = s.loc[:anchor_date]

        # Determine how many months we can forecast before we run out of actuals
        anchor_period = anchor_date.to_period("M")
        last_period = last_date.to_period("M")
        # number of months between anchor and last_date
        months_available = (last_period.year - anchor_period.year) * 12 + (last_period.month - anchor_period.month)

        horizon_bt = min(horizon, months_available)
        if horizon_bt <= 0:
            print("[backtest] No future months available for this anchor; skipping.")
            continue

        print(f"[backtest] Training length={len(y_train)}, backtest horizon={horizon_bt} months.")

        # Fit SARIMAX
        model = SARIMAX(
            endog=y_train,
            order=(1, 1, 1),
            seasonal_order=(1, 1, 1, 12),
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(disp=False)

        # Forecast horizon_bt steps
        fc = res.get_forecast(steps=horizon_bt)
        mean_fc = fc.predicted_mean.values
        ci = fc.conf_int().values  # shape (horizon_bt, 2)

        algo_params = {
            "order": (1, 1, 1),
            "seasonal_order": (1, 1, 1, 12),
            "n_obs": int(len(y_train)),
            "anchor_date": str(anchor_date.date()),
        }

        """
        run_id = insert_forecast_run_backtest(
            metric_id=metric_id,
            geo_id=geo_id,
            property_type_id=property_type_id,
            train_start=y_train.index[0],
            train_end=anchor_date,
            horizon_max_months=horizon_bt,
            algo_params=algo_params,
            anchor_date=anchor_date,
        )

        insert_predictions_backtest(
            run_id=run_id,
            forecast_values=mean_fc,
            conf_int=ci,
            last_date=anchor_date,
            horizon_max_months=horizon_bt,
        )
        """

        con = get_connection()
        
        algo_params = {
            "order": (1, 1, 1),
            "seasonal_order": (1, 1, 1, 12),
            "n_obs": int(len(y_train)),
            "anchor_date": str(anchor_date.date()),
        }
        
        run_id = insert_run(
            con=con,
            model_name="sarimax_backtest",
            model_version="v1",
            target_metric_id=metric_id,
            target_geo_id=geo_id,
            target_property_type_id=property_type_id,
            freq="M",
            train_start=y_train.index[0].date(),
            train_end=anchor_date.date(),
            horizon_max_months=horizon_bt,
            algo_params=algo_params,
            notes=f"SARIMAX backtest anchor={anchor_date.date()}",
            is_active=False,
            run_kind="backtest",
            batch_id=batch_id,
            data_asof=data_asof,
        )
        
        last_period = anchor_date.to_period("M")
        future_periods = [last_period + i for i in range(1, horizon_bt + 1)]
        target_dates = [p.to_timestamp(how="end").date() for p in future_periods]
        
        insert_predictions(
            con=con,
            run_id=run_id,
            target_dates=target_dates,
            y_hat=mean_fc,
            y_hat_lo=ci[:, 0] if ci is not None else None,
            y_hat_hi=ci[:, 1] if ci is not None else None,
        )
        
        con.close()


        print(f"[backtest] Created backtest run_id={run_id} for anchor={anchor_date.date()}")
        results_summary.append({"anchor_date": anchor_date, "run_id": run_id})

    print("\n[backtest] Summary:")
    for r in results_summary:
        print(f"  anchor={r['anchor_date'].date()} -> run_id={r['run_id']}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SARIMAX backtests for a single target series.")
    parser.add_argument("--metric_id", default="median_sale_price")
    parser.add_argument("--geo_id", default="dc_city")
    parser.add_argument("--property_type_id", default="-1")
    parser.add_argument("--horizon", type=int, default=12)

    parser.add_argument("--min_train_len", type=int, default=DEFAULT_MIN_TRAIN_LEN)
    parser.add_argument("--anchor_step_months", type=int, default=DEFAULT_ANCHOR_STEP_MONTHS)
    parser.add_argument("--max_anchors", type=int, default=DEFAULT_MAX_ANCHORS)
    parser.add_argument("--latest_anchor_offset_months", type=int, default=None)

    parser.add_argument("--batch_id", type=str, default=None)
    parser.add_argument("--data_asof", type=str, default=None)  # YYYY-MM-DD

    args = parser.parse_args()

    run_backtest_sarimax_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
    )
