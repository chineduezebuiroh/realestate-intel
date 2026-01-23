from __future__ import annotations

from typing import Optional, List, Dict, Any

import pandas as pd

from forecast.db_forecast import get_connection, insert_run, insert_predictions
from forecast.backtest_utils import month_end_index
from forecast.models.sarimax_exog.core import SarimaxExogSpec, fit_sarimax_exog, forecast_sarimax_exog


def _enforce_month_end_freq(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    # normalize to month-end timestamps and attach freq
    idx = pd.to_datetime(idx)
    idx = idx.to_period("M").to_timestamp(how="end")
    # set freq explicitly (ME = month end)
    return pd.DatetimeIndex(idx, freq="ME")


def _split_y_and_exog(df: pd.DataFrame) -> tuple[pd.Series, pd.DataFrame]:
    if "y" not in df.columns:
        raise ValueError("[sarimax_exog_bridge] design matrix artifact missing required 'y' column")
    y = df["y"].astype(float)
    X = df.drop(columns=["y"])
    return y, X


def _assert_exog_order(X: pd.DataFrame, feature_ids: List[str]) -> None:
    cols = list(map(str, X.columns))
    if cols != list(map(str, feature_ids)):
        raise ValueError(
            "[sarimax_exog_bridge] exog column order mismatch vs feature_ids\n"
            f"X_cols[:5]={cols[:5]}\n"
            f"feature_ids[:5]={feature_ids[:5]}"
        )


def run_bridge_from_design_matrix_artifact(
    *,
    # identity
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    freq: str,
    # artifact inputs
    design_matrix_parquet_path: str,
    design_matrix_audit_json_path: str,
    # run config
    anchor_date: str,  # YYYY-MM-DD
    horizon: int,
    batch_id: str,
    data_asof: str,
    run_kind: str,  # "backtest" or "live"
    is_active: bool,
    model_version: str = "v0_bridge_artifact",
    future_exog_parquet_path: Optional[str] = None,
) -> int:
    # ---- load artifacts ----

    df = pd.read_parquet(design_matrix_parquet_path)
    with open(design_matrix_audit_json_path, "r") as f:
        audit: Dict[str, Any] = __import__("json").load(f)

    max_h = audit.get("max_horizon_available")
    if max_h is not None and horizon > int(max_h):
        raise ValueError(
            f"[sarimax_exog_bridge] horizon={horizon} exceeds audit.max_horizon_available={max_h} "
            f"for this artifact."
        )

    feature_ids = audit.get("feature_ids")
    if not feature_ids:
        raise ValueError("[sarimax_exog_bridge] audit missing feature_ids")
    
    y_full, X_full = _split_y_and_exog(df)
    _assert_exog_order(X_full, feature_ids)
    
    anchor_ts = pd.Timestamp(anchor_date).to_period("M").to_timestamp(how="end")
    if anchor_ts not in y_full.index:
        raise ValueError(f"[sarimax_exog_bridge] anchor not in y index: {anchor_ts}")
    if anchor_ts not in X_full.index:
        raise ValueError(f"[sarimax_exog_bridge] anchor not in X index: {anchor_ts}")

    # How many future exog rows exist in the artifact after anchor?
    n_future_available = int((X_full.index > anchor_ts).sum())
    
    if horizon > n_future_available:
        raise ValueError(
            "[sarimax_exog_bridge] requested horizon exceeds future exog available in artifact.\n"
            f"anchor={anchor_ts.date()} horizon={horizon} available_future_rows={n_future_available}\n"
            "Pick a smaller horizon, or generate future exog rows via an exog-forecasting policy."
        )

    y_full.index = _enforce_month_end_freq(y_full.index)
    X_full.index = _enforce_month_end_freq(X_full.index)

    y_train = y_full.loc[:anchor_ts]
    X_train = X_full.loc[:anchor_ts]
    
    # Future exog rows come from artifact rows AFTER anchor
    if future_exog_parquet_path:
        X_future = pd.read_parquet(future_exog_parquet_path)
    
        # allow either:
        # 1) contains only exog columns, or
        # 2) contains y + exog (we ignore y)
        if "y" in X_future.columns:
            X_future = X_future.drop(columns=["y"])
    
        _assert_exog_order(X_future, feature_ids)
    
        # normalize to month-end + ensure we take rows after anchor
        X_future = X_future.loc[pd.Timestamp(anchor_date).to_period("M").to_timestamp(how="end"):]
        X_future = X_future.iloc[1 : horizon + 1].copy()
    else:
        # legacy behavior: try to use rows inside the design matrix artifact
        X_future = X_full.loc[anchor_ts:].iloc[1 : horizon + 1].copy()
    
    if len(X_future) != horizon:
        raise ValueError(
            f"[sarimax_exog_bridge] insufficient future exog rows for horizon={horizon}: got {len(X_future)}"
        )
    X_future.index = _enforce_month_end_freq(X_future.index)
    
    spec = SarimaxExogSpec()
    res = fit_sarimax_exog(y_train=y_train, X_train=X_train, spec=spec)
    mean_fc, ci = forecast_sarimax_exog(res=res, X_future=X_future, steps=horizon)
    
    target_dates = [d.date() for d in X_future.index]

    
    if len(X_future) != horizon:
        raise ValueError(
            f"[sarimax_exog_bridge] insufficient future exog rows for horizon={horizon}: "
            f"got {len(X_future)}"
        )

    spec = SarimaxExogSpec()
    res = fit_sarimax_exog(y_train=y_train, X_train=X_train, spec=spec)
    mean_fc, ci = forecast_sarimax_exog(res=res, X_future=X_future, steps=horizon)

    # build target_dates from X_future index
    target_dates = [d.date() for d in X_future.index]

    algo_params = {
        "model_version": model_version,
        "feature_ids": feature_ids,
        "design_matrix_sha256": audit.get("design_matrix_sha256"),
        "feature_set_sha256": audit.get("feature_set_sha256"),
        "anchor_date": anchor_date,
        "fit_diag": {
            "aic": getattr(res, "aic", None),
            "bic": getattr(res, "bic", None),
        },
        "contracts": {
            "run_kind": run_kind,
            "anchor_date": anchor_date,
            "data_asof_effective": data_asof,
            "target_metric_id": metric_id,
            "target_geo_id": geo_id,
            "target_property_type_id": property_type_id,
            "freq": freq,
            "train_start": str(y_train.index[0].date()),
            "train_end": str(anchor_ts.date()),
            "horizon_max_months": int(horizon),
        },
    }

    con = get_connection()
    run_id = insert_run(
        con=con,
        model_name="sarimax_exog",
        model_version=model_version,
        target_metric_id=metric_id,
        target_geo_id=geo_id,
        target_property_type_id=property_type_id,
        freq=freq,
        train_start=y_train.index[0].date(),
        train_end=anchor_ts.date(),
        horizon_max_months=horizon,
        algo_params=algo_params,
        notes=f"SARIMAX(exog) bridge run anchor={anchor_date}",
        is_active=is_active,
        run_kind=run_kind,
        batch_id=batch_id,
        data_asof=pd.to_datetime(data_asof).date(),
    )

    insert_predictions(
        con=con,
        run_id=run_id,
        target_dates=target_dates,
        y_hat=mean_fc,
        y_hat_lo=ci[:, 0] if ci is not None else None,
        y_hat_hi=ci[:, 1] if ci is not None else None,
    )
    con.close()
    return int(run_id)
