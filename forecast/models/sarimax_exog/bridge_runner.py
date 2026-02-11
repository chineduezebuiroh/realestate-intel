from __future__ import annotations
# forecast/models/sarimax_exog/bridge_runner.py

import os, json, traceback, hashlib
import duckdb

import pandas as pd

from datetime import date, datetime
from typing import Optional, List, Dict, Any

from forecast.core.backtest_utils import month_end_index
from forecast.core.db_forecast import get_connection, insert_run, insert_predictions

from forecast.features.fact_loader import load_series_from_fact

from forecast.models.sarimax_exog.core import SarimaxExogSpec, fit_sarimax_exog, forecast_sarimax_exog

# ====================================================
# Helpers
# ====================================================
def _to_monthly_period_index(idx: pd.DatetimeIndex) -> pd.PeriodIndex:
    idx = pd.to_datetime(idx)
    return idx.to_period("M")

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

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _parse_feature_id(feature_id: str) -> tuple[str, str, str, str, int]:
    """
    feature_id format used by selector: metric__geo__pt__source_lagN
      e.g. avg_sale_to_list__va_state__6__redfin_lag1
    Returns (metric_id, geo_id, pt_id, source_id, lag_int)
    """
    parts = feature_id.split("__")
    if len(parts) != 4:
        raise ValueError(f"[sarimax_exog_bridge] cannot parse feature_id: {feature_id}")
    metric_id, geo_id, pt_id, src_lag = parts
    if "_lag" not in src_lag:
        raise ValueError(f"[sarimax_exog_bridge] feature_id missing _lag: {feature_id}")
    source_id, lag_s = src_lag.rsplit("_lag", 1)
    try:
        lag = int(lag_s)
    except Exception:
        raise ValueError(f"[sarimax_exog_bridge] bad lag in feature_id: {feature_id}")
    return metric_id, geo_id, pt_id, source_id, lag

def _build_design_matrix_from_selected_features(
    *,
    con: "duckdb.DuckDBPyConnection",
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    data_asof: date,
    feature_ids: list[str],
    anchor_date: str,
    horizon: int,
    min_train_len: int,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Returns DataFrame with columns: y + each feature_id (already lagged).
    Index: month-end DatetimeIndex
    """
    y = load_series_from_fact(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=property_type_id,
        data_asof=data_asof,
        source_id=None,
        con=con,
    ).astype(float)

    y.index = month_end_index(pd.to_datetime(y.index))
    y = y[~y.index.duplicated(keep="last")].sort_index()
    y.name = "y"

    idx = y.index
    X = pd.DataFrame(index=idx)

    max_lag = 0
    for fid in feature_ids:
        m, g, pt, src, lag = _parse_feature_id(fid)
        max_lag = max(max_lag, lag)

        s = load_series_from_fact(
            metric_id=m,
            geo_id=g,
            property_type_id=pt,
            data_asof=data_asof,
            source_id=src,
            con=con,
        ).astype(float)

        s.index = month_end_index(pd.to_datetime(s.index))
        s = s[~s.index.duplicated(keep="last")].sort_index()

        X[fid] = s.reindex(idx).shift(lag)

    df = pd.concat([y, X], axis=1)

    # remove the unavoidable lag-induced NaNs
    if max_lag > 0 and len(df) > max_lag:
        df = df.iloc[max_lag:].copy()

    # --- Window to what we will actually use for this anchor ---
    anchor_ts = pd.Timestamp(anchor_date).to_period("M").to_timestamp(how="end")
    if anchor_ts not in df.index:
        raise ValueError(f"[sarimax_exog_bridge] anchor not in design matrix index: {anchor_date}")

    # We need:
    #  - at least min_train_len rows for training ending at anchor
    #  - plus horizon rows after anchor for X_future
    train_end = anchor_ts
    future_end = df.index[df.index > anchor_ts][:horizon]
    if len(future_end) < horizon:
        raise ValueError(
            f"[sarimax_exog_bridge] insufficient future rows after anchor={anchor_date}: "
            f"need {horizon}, have {len(future_end)}"
        )
    future_end_ts = future_end[-1]

    # Choose a conservative train start: last min_train_len rows before/including anchor
    train_block = df.loc[:train_end].dropna(subset=["y"])
    if len(train_block) < min_train_len:
        raise ValueError(
            f"[sarimax_exog_bridge] insufficient training rows at anchor={anchor_date}: "
            f"{len(train_block)} < min_train_len={min_train_len}"
        )
    train_start_ts = train_block.index[-min_train_len]

    df_win = df.loc[train_start_ts:future_end_ts].copy()


    # Now enforce no NaNs *in the window we actually use*
    # Contract B: drop exog columns that are incomplete in the anchor window.
    feature_cols = [c for c in df_win.columns if c != "y"]

    # If y has NaNs in-window, that's a real target issue (still fail)
    if df_win["y"].isna().any():
        bad_dates = df_win.index[df_win["y"].isna()].strftime("%Y-%m-%d").tolist()[:10]
        raise ValueError(
            "[sarimax_exog_bridge] target y has NaNs in anchor window.\n"
            f"anchor={anchor_date} train_start={train_start_ts.date()} future_end={future_end_ts.date()}\n"
            f"First bad dates: {bad_dates}"
        )

    dropped: list[str] = []
    # Iteratively drop columns that create NaNs anywhere in window
    while True:
        # rows that have any NaN among exogs
        exog_bad_rows = df_win[feature_cols].isna().any(axis=1) if feature_cols else pd.Series(False, index=df_win.index)

        if not exog_bad_rows.any():
            break  # all good

        # columns responsible for NaNs
        cols_with_nan = df_win.loc[exog_bad_rows, feature_cols].isna().any(axis=0)
        offenders = sorted(cols_with_nan[cols_with_nan].index.tolist())

        if not offenders:
            # Shouldn't happen, but don't infinite-loop
            break

        # Drop them
        df_win = df_win.drop(columns=offenders)
        dropped.extend(offenders)
        feature_cols = [c for c in df_win.columns if c != "y"]

        # Safety: if we drop everything, fail loudly
        if not feature_cols:
            bad_dates = df_win.index[exog_bad_rows].strftime("%Y-%m-%d").tolist()[:10]
            raise ValueError(
                "[sarimax_exog_bridge] all exogs dropped due to missingness in anchor window.\n"
                f"anchor={anchor_date} train_start={train_start_ts.date()} future_end={future_end_ts.date()}\n"
                f"First bad dates: {bad_dates}\n"
                f"dropped_n={len(dropped)}"
            )

    if dropped:
        # Keep this as a print for now; later promote to structured artifact/audit
        print(f"[sarimax_exog_bridge] dropped_exogs_due_to_nans n={len(dropped)} example={dropped[:5]}")

    effective_feature_ids = [c for c in df_win.columns if c != "y"]
    # deterministic order: keep the original requested order
    req_set = set(feature_ids)
    effective_feature_ids = [fid for fid in feature_ids if fid in req_set and fid in set(effective_feature_ids)]
    return df_win, effective_feature_ids


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
    data_asof: Optional[str] = None,
    run_kind: str,  # "backtest" or "live"
    is_active: bool,
    model_version: str = "v0_bridge_artifact",
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

    if data_asof is None:
        data_asof = str(audit.get("data_asof_effective") or "").strip()
    if not data_asof:
        raise ValueError("[sarimax_exog_bridge] data_asof missing and audit has no data_asof_effective")


    feature_ids_effective = audit.get("feature_ids")
    if not feature_ids_effective:
        raise ValueError("[sarimax_exog_bridge] audit missing feature_ids")
    
    feature_ids_requested = audit.get("feature_ids_requested") or feature_ids_effective

    
    y_full, X_full = _split_y_and_exog(df)
    _assert_exog_order(X_full, feature_ids_effective)
    
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

    y_train = y_full.loc[:anchor_ts]
    X_train = X_full.loc[:anchor_ts]
    
    # legacy behavior: try to use rows inside the design matrix artifact
    X_future = X_full.loc[anchor_ts:].iloc[1 : horizon + 1].copy()
    
    p = _to_monthly_period_index(X_future.index)
    if len(p) >= 2:
        expected = pd.period_range(p[0], periods=len(p), freq="M")
        if not p.equals(expected):
            raise ValueError("[sarimax_exog_bridge] X_future months are not consecutive; cannot forecast deterministically.")
    
    if len(X_future) != horizon:
        raise ValueError(
            f"[sarimax_exog_bridge] insufficient future exog rows for horizon={horizon}: got {len(X_future)}"
        )
    
    train_start_date = pd.to_datetime(y_train.index[0]).date()
    train_end_date = anchor_ts.date()

    # Statsmodels-friendly monthly index (handles missing months without freq pinning)
    y_train_sm = y_train.copy()
    X_train_sm = X_train.copy()
    X_future_sm = X_future.copy()

    
    y_train_sm.index = _to_monthly_period_index(y_train_sm.index)
    X_train_sm.index = _to_monthly_period_index(X_train_sm.index)
    X_future_sm.index = _to_monthly_period_index(X_future_sm.index)

    
    spec = SarimaxExogSpec()
    res = fit_sarimax_exog(y_train=y_train_sm, X_train=X_train_sm, spec=spec)
    mean_fc, ci = forecast_sarimax_exog(res=res, X_future=X_future_sm, steps=horizon)

    mle_retvals = getattr(res, "mle_retvals", None) or {}
    fit_converged = bool(mle_retvals.get("converged")) if isinstance(mle_retvals, dict) else None

    
    #target_dates = [d.date() for d in X_future.index]
    target_dates = [pd.to_datetime(d).date() for d in X_future.index]


    algo_params = {
        "model_version": model_version,
        "feature_ids_requested": feature_ids_requested,
        "feature_ids_effective": feature_ids_effective,
        "design_matrix_sha256": audit.get("design_matrix_sha256"),
        "feature_set_sha256": audit.get("feature_set_sha256"),
        "anchor_date": anchor_date,
        "fit_diag": {
            "aic": getattr(res, "aic", None),
            "bic": getattr(res, "bic", None),
            "fit_converged": fit_converged,
            "mle_retvals": mle_retvals,   # optional but useful for debugging
            "n_obs_train": len(y_train_sm),
            "n_exogs_effective": X_train_sm.shape[1],
            "dropped_exogs_n": len(dropped),
        },
        "contracts": {
            "run_kind": run_kind,
            "anchor_date": anchor_date,
            "data_asof_effective": data_asof,
            "target_metric_id": metric_id,
            "target_geo_id": geo_id,
            "target_property_type_id": property_type_id,
            "freq": freq,
            "train_start": train_start_date,
            "train_end": train_end_date,
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
        train_start=train_start_date,
        train_end=train_end_date,
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

def _cap_feature_ids_for_sarimax(
    df_sel: pd.DataFrame,
    *,
    max_exogs: int,
    min_non_redfin: int,
) -> list[str]:
    """
    Deterministically cap selector-picked feature_ids for SARIMAX.
    Uses selector rank (ascending) as the stable ordering.
    Enforces a minimum count of non-Redfin features if possible.
    """
    if max_exogs is None or max_exogs <= 0:
        return df_sel["feature_id"].astype(str).tolist()

    df = df_sel.copy()
    if "rank" in df.columns:
        df = df.sort_values(["rank", "feature_id"], ascending=[True, True])
    else:
        # fallback: higher importance first
        df = df.sort_values(["importance", "feature_id"], ascending=[False, True])

    fids = df["feature_id"].astype(str).tolist()
    if len(fids) <= max_exogs:
        return fids

    def _is_redfin(fid: str) -> bool:
        # feature_id format: metric__geo__pt__source_lagK
        # e.g. "...__redfin_lag1"
        # This is deliberately blunt and deterministic.
        return "__redfin_lag" in fid

    non_redfin = [fid for fid in fids if not _is_redfin(fid)]
    redfin = [fid for fid in fids if _is_redfin(fid)]

    # If we can't satisfy min_non_redfin, take as many as exist (Contract A).
    take_non = min(len(non_redfin), int(min_non_redfin))
    take_total = int(max_exogs)
    take_red = max(0, take_total - take_non)

    out = non_redfin[:take_non] + redfin[:take_red]

    # If non_redfin was scarce, we may be underfilled; top-up deterministically.
    if len(out) < take_total:
        used = set(out)
        for fid in fids:
            if fid in used:
                continue
            out.append(fid)
            used.add(fid)
            if len(out) >= take_total:
                break

    return out[:take_total]

# ====================================================
# Primary Function
# ====================================================
def run_backtest_sarimax_exog_bridge(
    *,
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    freq: str,
    horizon: int,
    min_train_len: int,
    selector_batch_id: str,
    anchors_csv: str,
    batch_id: str,
    data_asof: str,
    artifact_root: str,
    run_kind: str = "backtest",
    is_active: bool = False,
    max_exogs_for_sarimax: int = 30,
    min_non_redfin_for_sarimax: int = 10,
) -> None:
    """
    Phase C bridge backtest runner:
      - reads selector outputs per anchor
      - builds design matrix artifacts (y + feature_id columns)
      - calls run_bridge_from_design_matrix_artifact() which writes to DB
    """
    anchors = [a.strip() for a in (anchors_csv or "").split(",") if a.strip()]
    if not anchors:
        raise ValueError("[sarimax_exog_bridge] anchors_csv was empty")

    data_asof_date = pd.to_datetime(data_asof).date()
    artifact_root = artifact_root.rstrip("/")

    selector_metric_dir = f"{artifact_root}/runs/{selector_batch_id}/xgb/{metric_id}"
    bridge_dir = f"{artifact_root}/runs/{batch_id}/sarimax_exog_bridge/{metric_id}"
    os.makedirs(bridge_dir, exist_ok=True)

    con = get_connection()
    try:
        results = {
            "model": "sarimax_exog_bridge",
            "batch_id": batch_id,
            "selector_batch_id": selector_batch_id,
            "metric_id": metric_id,
            "geo_id": geo_id,
            "property_type_id": property_type_id,
            "freq": freq,
            "horizon": int(horizon),
            "min_train_len": int(min_train_len),
            "data_asof": str(data_asof_date),
            "requested_anchors": anchors,
            "success": [],
            "failed": [],
            "started_at_utc": datetime.utcnow().isoformat() + "Z",
        }
        
        for anchor in anchors:
            try:
                sel_path = f"{selector_metric_dir}/selected_features__anchor={anchor}.parquet"
                if not os.path.exists(sel_path):
                    raise FileNotFoundError(f"missing selector output: {sel_path}")
        
                df_sel = pd.read_parquet(sel_path)
                feature_ids = df_sel["feature_id"].astype(str).tolist()
                if not feature_ids:
                    raise ValueError("empty selector feature list")

                feature_set_sha256 = None
                if "feature_set_sha256" in df_sel.columns and df_sel["feature_set_sha256"].notna().any():
                    feature_set_sha256 = str(df_sel["feature_set_sha256"].dropna().iloc[0])
        
                dm_full, feature_ids_after_nan_drop = _build_design_matrix_from_selected_features(
                    con=con,
                    metric_id=metric_id,
                    geo_id=geo_id,
                    property_type_id=property_type_id,
                    data_asof=data_asof_date,
                    feature_ids=feature_ids,
                    anchor_date=anchor,
                    horizon=horizon,
                    min_train_len=min_train_len,
                )

                # cap AFTER NaN-drop (your requested provenance order)
                effective_feature_ids = _cap_feature_ids_for_sarimax(
                    feature_ids_effective=feature_ids_after_nan_drop,
                    max_exogs_for_sarimax=max_exogs_for_sarimax,
                    min_non_redfin_for_sarimax=min_non_redfin_for_sarimax,
                )
                
                # materialize capped DM (keep 'y' + capped exogs, preserve order)
                keep_cols = ["y"] + list(map(str, effective_feature_ids))
                missing = [c for c in keep_cols if c not in dm_full.columns]
                if missing:
                    raise ValueError(f"[sarimax_exog_bridge] capped columns missing from dm_full: {missing[:10]}")
                
                dm = dm_full.loc[:, keep_cols].copy()
        
                anchor_ts = pd.Timestamp(anchor).to_period("M").to_timestamp(how="end")
                n_future_available = int((dm.index > anchor_ts).sum())
                if horizon > n_future_available:
                    raise ValueError(f"horizon exceeds future rows: need {horizon}, have {n_future_available}")
        
                dm_path = f"{bridge_dir}/design_matrix__anchor={anchor}.parquet"
                audit_path = f"{bridge_dir}/design_matrix_audit__anchor={anchor}.json"
                dm.to_parquet(dm_path, index=True)
                dm_sha = _sha256_file(dm_path)

                audit = {
                    "data_asof_effective": str(data_asof_date),
                    "selector_batch_id": selector_batch_id,
                    "selector_selected_features_path": sel_path,
                    "feature_ids_requested": feature_ids,
                    "feature_ids": effective_feature_ids,  # canonical for artifact runner
                    "feature_set_sha256": feature_set_sha256,
                    "design_matrix_sha256": dm_sha,
                    "max_horizon_available": int(n_future_available),
                }
                with open(audit_path, "w") as f:
                    json.dump(audit, f, indent=2, sort_keys=True)

                run_id = run_bridge_from_design_matrix_artifact(
                    metric_id=metric_id,
                    geo_id=geo_id,
                    property_type_id=property_type_id,
                    freq=freq,
                    design_matrix_parquet_path=dm_path,
                    design_matrix_audit_json_path=audit_path,
                    anchor_date=anchor,
                    horizon=horizon,
                    batch_id=batch_id,
                    data_asof=str(data_asof_date),
                    run_kind=run_kind,
                    is_active=is_active,
                )
        
                results["success"].append({"anchor": anchor, "run_id": int(run_id)})

            except Exception as e:
                results["failed"].append({
                    "anchor": anchor,
                    "error_type": type(e).__name__,
                    "error": str(e),
                })
                # keep going

        results["finished_at_utc"] = datetime.utcnow().isoformat() + "Z"

        summary_path = f"{bridge_dir}/bridge_backtest_summary.json"
        with open(summary_path, "w") as f:
            json.dump(results, f, indent=2, sort_keys=True)
        
        print(f"[sarimax_exog_bridge] wrote summary -> {summary_path}")
        print(f"[sarimax_exog_bridge] success={len(results['success'])} failed={len(results['failed'])}")

    finally:
        try:
            con.close()
        except Exception:
            pass
