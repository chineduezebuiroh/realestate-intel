from __future__ import annotations
# forecast/models/sarimax_exog/bridge_runner.py

import os, json, traceback, hashlib
import duckdb

import pandas as pd
import numpy as np

from datetime import date, datetime
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path

from forecast.core.backtest_utils import month_end_index
from forecast.core.db_forecast import get_connection, insert_run, insert_predictions

from forecast.features.fact_loader import load_series_from_fact

from forecast.models.sarimax_exog.core import SarimaxExogSpec, fit_sarimax_exog, forecast_sarimax_exog

# ====================================================
# Helpers
# ====================================================
# Canonical lag policy (bridge must not invent lag0)
CANON_ALLOWED_LAGS = {1, 3, 6, 12}

def _extract_lag(fid: str) -> int:
    m = __import__("re").search(r"_lag(\d+)$", str(fid))
    if not m:
        raise ValueError(f"[sarimax_exog_bridge] canonical feature_id missing _lag suffix: {fid}")
    return int(m.group(1))

def _validate_canonical_feature_ids(feature_ids: list[str]) -> None:
    bad0 = [fid for fid in feature_ids if str(fid).endswith("_lag0")]
    if bad0:
        raise SystemExit(
            "[sarimax_exog_bridge] REFUSING: canonical feature_ids contain _lag0. "
            f"examples={bad0[:10]}"
        )
    bad = []
    for fid in feature_ids:
        lag = _extract_lag(fid)
        if lag not in CANON_ALLOWED_LAGS:
            bad.append((fid, lag))
    if bad:
        raise SystemExit(
            "[sarimax_exog_bridge] REFUSING: canonical feature_ids contain disallowed lags. "
            f"allowed={sorted(CANON_ALLOWED_LAGS)} bad_examples={bad[:10]}"
        )
        
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

def _resolve_canonical_exog_csv(
    *,
    artifact_root: str,
    stability_version: str,
    metric_id: str,
    n: int,
) -> Path:
    """
    Deterministic canonical exog path:
      {artifact_root}/canonical_exogs/{stability_version}/canonical_exog_set__metric={metric_id}__n={n}.csv
    """
    root = Path(artifact_root.rstrip("/"))
    return (
        root
        / "canonical_exogs"
        / stability_version
        / f"canonical_exog_set__metric={metric_id}__n={int(n)}.csv"
    )

def _load_canonical_exog_df(
    *,
    artifact_root: str,
    stability_version: str,
    metric_id: str,
    n: int,
    override_csv: Optional[str] = None,
) -> tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    """
    Returns:
      (df_canon, canon_csv_path_str, canon_csv_sha256)
    df_canon must include:
      - feature_id (base_feature_id + _lag{best_lead_mode})
      - rank (canonical_rank)
    """
    if override_csv:
        p = Path(override_csv)
    else:
        p = _resolve_canonical_exog_csv(
            artifact_root=artifact_root,
            stability_version=stability_version,
            metric_id=metric_id,
            n=n,
        )

    if not p.exists():
        return None, None, None

    df = pd.read_csv(p)

    required = {"base_feature_id", "best_lead_mode", "canonical_rank"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[sarimax_exog_bridge] canonical exog CSV missing columns: {sorted(missing)} in {p}")

    # Build selector-style feature_id with explicit lag (lag can be 0; _parse_feature_id accepts int)
    df = df.copy()

    if df["best_lead_mode"].isna().any():
        bad_n = int(df["best_lead_mode"].isna().sum())
        raise ValueError(f"[sarimax_exog_bridge] canonical exogs best_lead_mode has NaNs (n={bad_n}). Refusing.")
    df["best_lead_mode"] = df["best_lead_mode"].astype(int)

    
    df["feature_id"] = df["base_feature_id"].astype(str) + df["best_lead_mode"].astype(str).radd("_lag")
    # stable ordering = canonical_rank ascending
    df["rank"] = df["canonical_rank"].astype(int)

    # Defensive: cap to exactly N best ranks (if file has more)
    df = df.sort_values(["rank", "feature_id"], ascending=[True, True]).head(int(n)).reset_index(drop=True)

    sha = _sha256_file(str(p))
    return df[["feature_id", "rank"]].copy(), str(p), sha

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
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Returns:
      - df_win: DataFrame with columns: y + each feature_id (already lagged), windowed to [train_start..future_end]
      - effective_feature_ids: list of feature IDs that remain after NaN-drop (in the same order as columns in df_win)
      - dropped_feature_ids: list of feature IDs dropped due to missingness inside the anchor window
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
    dropped_feature_ids = list(map(str, dropped))  # normalize
    return df_win, effective_feature_ids, dropped_feature_ids


def _zscore_exogs_train_only(
    X_train: pd.DataFrame,
    X_future: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Z-score exogenous features using TRAIN-window mean/std only.
    - Applies same transform to X_future.
    - Any zero/NaN std is treated as 1.0 (no scaling) to avoid blowups.
    Returns transformed (X_train_z, X_future_z, diag_dict).
    """
    mu = X_train.mean(axis=0)
    sigma = X_train.std(axis=0, ddof=0)

    # guard rails
    sigma_safe = sigma.copy()
    sigma_safe = sigma_safe.fillna(1.0)
    sigma_safe[sigma_safe == 0] = 1.0

    X_train_z = (X_train - mu) / sigma_safe
    X_future_z = (X_future - mu) / sigma_safe

    diag = {
        "zscore": True,
        "n_features": int(X_train.shape[1]),
        "n_zero_std": int((sigma.fillna(0) == 0).sum()),
    }
    return X_train_z, X_future_z, diag


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

    
    # --- exog scaling (train-only z-score) ---
    X_train_scaled, X_future_scaled, zdiag = _zscore_exogs_train_only(X_train, X_future)
    X_train = X_train_scaled
    X_future = X_future_scaled

    
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

    

    # --- DEBUG: exog diagnostics right before fit ---
    import numpy as np
    
    def _exog_diag(X: "pd.DataFrame") -> dict:
        Xv = X.to_numpy(dtype=float)
        return {
            "mean_abs_mean": float(np.abs(np.nanmean(Xv, axis=0)).mean()),
            "mean_std": float(np.nanstd(Xv, axis=0).mean()),
            "max_abs": float(np.nanmax(np.abs(Xv))),
            "any_nan": bool(np.isnan(Xv).any()),
            "any_inf": bool(np.isinf(Xv).any()),
            "shape": [int(Xv.shape[0]), int(Xv.shape[1])],
        }
    
    exog_diag_pre_fit = _exog_diag(X_train_sm)

    Xv = X_train_sm.to_numpy(dtype=float)
    # numerical rank + condition number
    u,s,vt = np.linalg.svd(Xv, full_matrices=False)
    rank = int((s > 1e-10).sum())
    cond = float(s[0]/s[-1]) if s[-1] > 0 else float("inf")


    
    res = fit_sarimax_exog(y_train=y_train_sm, X_train=X_train_sm, spec=spec)
    mean_fc, ci = forecast_sarimax_exog(res=res, X_future=X_future_sm, steps=horizon)

    mle_retvals = getattr(res, "mle_retvals", None) or {}
    fit_converged = bool(mle_retvals.get("converged")) if isinstance(mle_retvals, dict) else None

    
    #target_dates = [d.date() for d in X_future.index]
    target_dates = [pd.to_datetime(d).date() for d in X_future.index]

    dropped_feature_ids = list(map(str, audit.get("dropped_feature_ids_due_to_nans") or []))

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
            "exog_zscore_train_only": zdiag,
            "fit_converged": fit_converged,
            "mle_retvals": mle_retvals,   # optional but useful for debugging
            "n_obs_train": len(y_train_sm),
            "n_exogs_effective": X_train_sm.shape[1],
            "dropped_exogs_n": int(len(dropped_feature_ids)),
            "dropped_exogs_sample": dropped_feature_ids[:10],
            # optional, if you want full provenance in algo_params too (audit already has it)
            # "dropped_exogs": dropped_feature_ids,
            "exog_diag_pre_fit": exog_diag_pre_fit,
            "iterations": (mle_retvals.get("iterations") if isinstance(mle_retvals, dict) else None),
            "warnflag": (mle_retvals.get("warnflag") if isinstance(mle_retvals, dict) else None),
            "fopt": (mle_retvals.get("fopt") if isinstance(mle_retvals, dict) else None),
            "exog_rank": rank,
            "exog_cond": cond,
            "exog_smin": float(s[-1]),
        },
        "spec": {
            "order": list(spec.order),
            "seasonal_order": list(spec.seasonal_order),
            "enforce_stationarity": bool(spec.enforce_stationarity),
            "enforce_invertibility": bool(spec.enforce_invertibility),
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
    # canonical exogs (deterministic defaults; override is debug-only)
    use_canonical_exogs: bool = True,
    canonical_stability_version: str = "v08.2",
    canonical_exog_n: int = 100,
    canonical_exog_csv: Optional[str] = None,
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
    

    def _refuse_long_component(label: str, s: str, *, max_len: int = 160) -> None:
        # macOS per-path-component limit is commonly 255 bytes.
        # Keep this well below that to leave room for future suffixes.
        if s is None:
            return
        if len(s.encode("utf-8")) > max_len:
            raise SystemExit(
                f"[sarimax_exog_bridge] REFUSING: {label} is too long for a safe path component "
                f"({len(s.encode('utf-8'))} bytes > {max_len}).\n"
                f"Shorten it. Do NOT embed other batch ids inside it.\n"
                f"{label}={s}"
            )
    
    _refuse_long_component("batch_id", batch_id)
    _refuse_long_component("selector_batch_id", selector_batch_id)
    _refuse_long_component("metric_id", metric_id, max_len=120)
    
    selector_metric_dir = Path(artifact_root) / "runs" / selector_batch_id / "xgb" / metric_id
    bridge_dir = Path(artifact_root) / "runs" / batch_id / "sarimax_exog_bridge" / metric_id
    bridge_dir.mkdir(parents=True, exist_ok=True)


    # ---- canonical exogs (load once; apply across anchors) ----
    canon_df = None
    canon_csv_path = None
    canon_csv_sha256 = None

    if use_canonical_exogs:
        canon_df, canon_csv_path, canon_csv_sha256 = _load_canonical_exog_df(
            artifact_root=artifact_root,
            stability_version=canonical_stability_version,
            metric_id=metric_id,
            n=int(canonical_exog_n),
            override_csv=canonical_exog_csv,
        )

        canon_feature_ids = None
        if canon_df is not None and len(canon_df) > 0:
            if "feature_id" in canon_df.columns:
                canon_feature_ids = canon_df["feature_id"].astype(str).tolist()
            else:
                # Backward-compat: if old canonical file exists without feature_id,
                # reconstruct deterministically (but we still refuse lag0 via validator)
                if "best_lead_mode" not in canon_df.columns:
                    raise ValueError("[sarimax_exog_bridge] canonical exogs missing feature_id and best_lead_mode")
                canon_feature_ids = (
                    canon_df["base_feature_id"].astype(str)
                    + "_lag"
                    + canon_df["best_lead_mode"].astype(int).astype(str)
                ).tolist()

            _validate_canonical_feature_ids(canon_feature_ids)

        
        if canon_df is not None:
            print(
                "[sarimax_exog_bridge] using canonical exogs "
                f"metric={metric_id} n={len(canon_df)} version={canonical_stability_version} "
                f"path={canon_csv_path} sha256={canon_csv_sha256[:12]}..."
            )
        else:
            print(
                "[sarimax_exog_bridge] canonical exogs not found; falling back to selector outputs "
                f"metric={metric_id} version={canonical_stability_version} n={canonical_exog_n}"
            )


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
                feature_set_sha256 = None

                if canon_df is not None:
                    # canonical feature set (stable across anchors)
                    df_sel = canon_df.copy()
                    feature_ids = df_sel["feature_id"].astype(str).tolist()
                    sel_path = str(canon_csv_path)  # provenance
                    feature_set_sha256 = str(canon_csv_sha256)
                    exog_source = "canonical_exogs"
                else:
                    # selector-derived feature set (anchor-specific)
                    sel_path = f"{selector_metric_dir}/selected_features__anchor={anchor}.parquet"

                    df_sel = None
                    feature_set_sha256 = None
    
                    if canon_df is not None and canon_feature_ids is not None:
                        # Canonical path: do NOT depend on per-anchor selector output
                        feature_ids = list(map(str, canon_feature_ids))
                        exog_source = "canonical_exogs"
                    else:
                        # Fallback: selector path (requires per-anchor parquet)
                        if not os.path.exists(sel_path):
                            raise FileNotFoundError(f"missing selector output: {sel_path}")
    
                        df_sel = pd.read_parquet(sel_path)
                        feature_ids = df_sel["feature_id"].astype(str).tolist()
                        if not feature_ids:
                            raise ValueError("empty selector feature list")
                        exog_source = "selector"


                    if df_sel is not None and "feature_set_sha256" in df_sel.columns and df_sel["feature_set_sha256"].notna().any():
                        feature_set_sha256 = str(df_sel["feature_set_sha256"].dropna().iloc[0])


                    exog_source = "selector_selected_features"

        
                dm_full, feature_ids_after_nan_drop, dropped_feature_ids = _build_design_matrix_from_selected_features(
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


                # --- NaN-drop already applied in dm_full; now cap exogs using selector ranking, restricted to NaN-safe features ---

                nan_ok = set(map(str, feature_ids_after_nan_drop))
                
                # keep selector ordering/ranking but only for features that survived NaN-drop
                df_sel_eff = df_sel.copy()
                df_sel_eff["feature_id"] = df_sel_eff["feature_id"].astype(str)
                df_sel_eff = df_sel_eff[df_sel_eff["feature_id"].isin(nan_ok)].copy()
                
                keep_ids = _cap_feature_ids_for_sarimax(
                    df_sel_eff,
                    max_exogs=int(max_exogs_for_sarimax),
                    min_non_redfin=int(min_non_redfin_for_sarimax),
                )
                
                # safety: ensure keep_ids exist in dm_full and preserve order
                keep_cols = ["y"] + keep_ids
                missing = [c for c in keep_cols if c not in dm_full.columns]
                if missing:
                    raise ValueError(f"[sarimax_exog_bridge] capped columns missing from dm_full: {missing[:10]}")
                
                dm = dm_full.loc[:, keep_cols].copy()
                effective_feature_ids = keep_ids

        
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
                    "selector_selected_features_path": (sel_path if exog_source == "selector" else None),
                    # canonical
                    "exog_source": exog_source,
                    "canonical_stability_version": (canonical_stability_version if canon_df is not None else None),
                    "canonical_exog_n": (int(canonical_exog_n) if canon_df is not None else None),
                    "canonical_exog_csv_path": (str(canon_csv_path) if exog_source == "canonical_exogs" else None),
                    "canonical_exog_csv_sha256": (canon_csv_sha256 if canon_df is not None else None),
                    "feature_ids_requested": feature_ids,
                    "feature_ids": effective_feature_ids,  # canonical for artifact runner
                    "feature_set_sha256": feature_set_sha256,
                    "design_matrix_sha256": dm_sha,
                    "max_horizon_available": int(n_future_available),
                    "dropped_feature_ids_due_to_nans": dropped_feature_ids,
                    "dropped_feature_count_due_to_nans": int(len(dropped_feature_ids)),
                    
                    #alternatives that use 'exog_source == "canonical_exogs' instead of 'canon_df'
                    """
                    "exog_source": exog_source,
                    "canonical_stability_version": (canonical_stability_version if exog_source == "canonical_exogs" else None),
                    "canonical_exog_n": (int(canonical_exog_n) if exog_source == "canonical_exogs" else None),
                    "canonical_exog_csv_path": (str(canon_csv_path) if exog_source == "canonical_exogs" else None),
                    "canonical_exog_csv_sha256": (str(canon_csv_sha256) if exog_source == "canonical_exogs" else None),
                    """
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
        
                results["success"].append({
                    "anchor": anchor,
                    "run_id": int(run_id),
                    "dropped_feature_count_due_to_nans": int(len(dropped_feature_ids)),
                    "dropped_feature_ids_due_to_nans_sample": dropped_feature_ids[:10],
                })

                # --- write per-anchor summary (refuse overwrite) ---
                anchor_summary_path = bridge_dir / f"bridge_backtest_summary__anchor={anchor}.json"
                if anchor_summary_path.exists():
                    raise SystemExit(f"[sarimax_exog_bridge] REFUSING to overwrite existing summary: {anchor_summary_path}")
                
                anchor_payload = {
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
                    "anchor": anchor,
                    "run_id": int(run_id),
                    "feature_set_sha256": feature_set_sha256,
                    "dropped_feature_count_due_to_nans": int(len(dropped_feature_ids)),
                    "dropped_feature_ids_due_to_nans_sample": dropped_feature_ids[:10],
                    "design_matrix_path": str(dm_path),
                    "design_matrix_audit_path": str(audit_path),
                    "selector_selected_features_path": str(sel_path),
                }
                anchor_summary_path.write_text(json.dumps(anchor_payload, indent=2, sort_keys=True))


            except Exception as e:
                msg = str(e)
                if "insufficient training rows" in msg or "insufficient future rows" in msg:
                    results.setdefault("ineligible", []).append({"anchor": anchor,
                                                                "error_type": type(e).__name__,
                                                                "error": str(e),
                                                            })
                else:
                    results["failed"].append({
                        "anchor": anchor,
                        "error_type": type(e).__name__,
                        "error": str(e),
                    })
                    # keep going

        results["finished_at_utc"] = datetime.utcnow().isoformat() + "Z"


        summary_path = bridge_dir / "bridge_backtest_summary.json"
        # optional strictness (recommended)
        if summary_path.exists():
            raise SystemExit(f"[sarimax_exog_bridge] REFUSING to overwrite existing summary: {summary_path}")
        summary_path.write_text(json.dumps(results, indent=2, sort_keys=True))

        
        print(f"[sarimax_exog_bridge] wrote summary -> {summary_path}")
        print(f"[sarimax_exog_bridge] success={len(results['success'])} failed={len(results['failed'])}")

    finally:
        try:
            con.close()
        except Exception:
            pass
