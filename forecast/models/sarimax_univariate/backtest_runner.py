from __future__ import annotations
# forecast/models/sarimax_univariate/backtest_runner.py

import os
from typing import List, Dict, Optional

import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from forecast.core.db_forecast import (
    get_connection,
    new_batch_id,
    insert_run,
    insert_predictions,
)

from forecast.core.backtest_utils import (
    choose_anchor_dates,
    month_end_index,
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)

# ==========================================================
# Helper
# ==========================================================
def _parse_data_asof(x: Optional[str], default_date: pd.Timestamp):
    if x is None or str(x).strip() == "":
        return default_date.date()
    return pd.to_datetime(x).date()

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
        WITH t AS (
          SELECT
            (date_trunc('month', date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end,
            date,
            value,
            row_number() OVER (
              PARTITION BY (date_trunc('month', date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
              ORDER BY date DESC
            ) AS rn
          FROM fact_timeseries
          WHERE metric_id = ?
            AND geo_id = ?
            AND property_type_id = ?
        )
        SELECT month_end AS date, value
        FROM t
        WHERE rn = 1
        ORDER BY month_end
    """
    df = con.execute(sql, [metric_id, geo_id, property_type_id]).fetchdf()

    if df.empty:
        raise ValueError(
            f"No data for metric={metric_id}, geo={geo_id}, pt={property_type_id}"
        )

    s = df.set_index("date")["value"].astype(float)

    s.index = pd.DatetimeIndex(s.index)
    s = s.asfreq("ME")

    if s.index.freq is None:
        raise ValueError("Target series must have monthly-end frequency (ME).")
    if s.isna().any():
        raise ValueError("Target series has missing months after enforcing ME frequency.")
    
    return s


def run_backtest_sarimax_single(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
    *,
    min_train_len: int = DEFAULT_MIN_TRAIN_LEN,
    anchor_step_months: int = DEFAULT_ANCHOR_STEP_MONTHS,
    max_anchors: int = DEFAULT_MAX_ANCHORS,
    latest_anchor_offset_months: Optional[int] = None,
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,  # YYYY-MM-DD
    anchors_csv: Optional[str] = None,
    order: tuple[int, int, int] = (0, 1, 0),
    seasonal_order: tuple[int, int, int, int] = (1, 1, 1, 12),
    trend: str | None = None,
    model_version: str | None = None,
    enforce_stationarity: bool=False,
    enforce_invertibility: bool=False,
    maxiter: int = 200,
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
    #s.index = month_end_index(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()

    batch_id = batch_id or new_batch_id()
    data_asof = _parse_data_asof(data_asof, s.index.max())
    print(f"[backtest] batch_id={batch_id} data_asof={data_asof}")

    print(f"[backtest] target={metric_id} geo={geo_id} pt={property_type_id} horizon={horizon}")

    # IMPORTANT: clamp history to as-of (make as-of REAL, not cosmetic)
    data_asof_ts = pd.Timestamp(data_asof).to_period("M").to_timestamp(how="end")
    s = s.loc[:data_asof_ts].copy()
    if s.empty:
        raise ValueError(f"[backtest] no target data <= data_asof={data_asof}")
    
    # re-check max after clamp
    s = s[~s.index.duplicated(keep="last")].sort_index()


    if anchors_csv:
        anchors = [pd.Timestamp(s.strip()) for s in anchors_csv.split(",") if s.strip()]
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
        print("[backtest] Not enough history to run backtests.")
        return
    
    print(f"[backtest] Found {len(anchors)} anchors.")

    # \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/ \/
    # Eventually centralize this next block in forecast/artifacts/, but don’t block Phase C on refactor. Get the contract.
    from pathlib import Path
    import json
    
    #out_dir = Path(os.environ.get("ARTIFACT_ROOT", "runs")) / "runs" / batch_id / "sarimax_univariate"
    artifact_root = Path(os.environ.get("ARTIFACT_ROOT", "runs"))
    out_dir = artifact_root / "runs" / batch_id / "sarimax_univariate" / metric_id
    out_dir.mkdir(parents=True, exist_ok=True)

    anchors_payload = {
        "model_name": "sarimax_univariate",
        "run_kind": "backtest",
        "metric_id": metric_id,
        "geo_id": geo_id,
        "property_type_id": property_type_id,
        "horizon": int(horizon),
        "min_train_len": int(min_train_len),
        "anchor_step_months": int(anchor_step_months),
        "max_anchors": int(max_anchors),
        "latest_anchor_offset_months": int(latest_anchor_offset_months) if latest_anchor_offset_months is not None else None,
        "data_asof": str(data_asof),
        "anchors": [str(pd.Timestamp(a).date()) for a in anchors],
        "anchors_source": "cli" if anchors_csv else "policy",
    }
    anchors_path = out_dir / "anchors.json"
    anchors_path.write_text(json.dumps(anchors_payload, indent=2))
    print(f"[backtest] wrote anchors -> {anchors_path}")

    asof_payload = {
        "mode": "strict",  # later: strict vs relaxed_poisoned
        "data_asof": str(data_asof),
        "data_asof_ts": str(data_asof_ts.date()),
        "notes": "user-passed data_asof" if data_asof is not None else "defaulted to series max",
    }
    asof_path = out_dir / "asof_report.json"
    asof_path.write_text(json.dumps(asof_payload, indent=2))
    print(f"[backtest] wrote asof_report -> {asof_path}")
    # End of block to centralize in forecast/artifacts/
    # /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\ /\

    
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
        future_months_available = (last_period.year - anchor_period.year) * 12 + (last_period.month - anchor_period.month)

        horizon_bt = min(horizon, future_months_available)
        if horizon_bt <= 0:
            print("[backtest] No future months available for this anchor; skipping.")
            continue

        print(f"[backtest] Training length={len(y_train)}, backtest horizon={horizon_bt} months.")

        # Fit SARIMAX
        model = SARIMAX(
            endog=y_train.astype(float),
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=bool(enforce_stationarity),
            enforce_invertibility=bool(enforce_invertibility),
            trend=trend,
        )
        res = model.fit(disp=False)

        # Forecast horizon_bt steps
        fc = res.get_forecast(steps=horizon_bt)
        mean_fc = fc.predicted_mean.values
        ci = fc.conf_int().values  # shape (horizon_bt, 2)

        algo_params = {
            "order": list(order),
            "seasonal_order": list(seasonal_order),
            "trend": trend,  # None or "c"/"t"/"ct"
            "enforce_stationarity": bool(enforce_stationarity),
            "enforce_invertibility": bool(enforce_invertibility),
            "n_obs": int(len(y_train)),
            "anchor_date": str(anchor_date.date()),            
            "contracts": {
                "run_kind": "backtest",
                "anchor_date": str(anchor_date.date()),
                "data_asof_effective": str(data_asof),
                "target_metric_id": metric_id,
                "target_geo_id": geo_id,
                "target_property_type_id": property_type_id,
                "freq": "M",
                "train_start": str(y_train.index[0].date()),
                "train_end": str(anchor_date.date()),
                "horizon_max_months": int(horizon_bt),
            }
        }
        
        con = get_connection()

        model_version_effective = model_version or "v1"
        run_id = insert_run(
            con=con,
            model_name="sarimax_univariate",
            model_version=model_version_effective,
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
