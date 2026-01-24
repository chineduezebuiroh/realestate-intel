from __future__ import annotations
# forecast/eval/core.py

from dataclasses import dataclass
from datetime import date
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd

from forecast.db_forecast import get_connection
from forecast.eval.metrics import rmse, mae, mape, smape, wape, interval_coverage, interval_width


@dataclass(frozen=True)
class EvalSpec:
    metric_id: str
    geo_id: str
    property_type_id: str
    freq: str = "M"
    # compare only these run kinds (typically backtest)
    run_kinds: Tuple[str, ...] = ("backtest",)
    # optional: restrict to a batch cohort
    batch_ids: Optional[Tuple[str, ...]] = None
    # optional: restrict to models
    model_names: Optional[Tuple[str, ...]] = None
    # optional: restrict anchors
    anchor_dates: Optional[Tuple[str, ...]] = None  # YYYY-MM-DD
    # evaluation trimming
    require_full_horizon: bool = True
    # in EvalSpec
    prefer_batch_ids: Optional[Tuple[str, ...]] = None
    dedupe_latest_per_model_anchor: bool = True
    require_models: Optional[Tuple[str, ...]] = None
    require_complete_cohort: bool = True


def _load_runs(spec: EvalSpec) -> pd.DataFrame:
    con = get_connection()
    where = [
        "target_metric_id = ?",
        "target_geo_id = ?",
        "target_property_type_id = ?",
        "freq = ?",
    ]
    params: List[Any] = [spec.metric_id, spec.geo_id, spec.property_type_id, spec.freq]

    if spec.run_kinds:
        where.append(f"run_kind IN ({','.join(['?']*len(spec.run_kinds))})")
        params.extend(list(spec.run_kinds))

    if spec.batch_ids:
        where.append(f"batch_id IN ({','.join(['?']*len(spec.batch_ids))})")
        params.extend(list(spec.batch_ids))

    if spec.model_names:
        where.append(f"model_name IN ({','.join(['?']*len(spec.model_names))})")
        params.extend(list(spec.model_names))

    sql = f"""
        SELECT
            run_id,
            model_name,
            model_version,
            run_kind,
            batch_id,
            data_asof,
            train_start,
            train_end,
            horizon_max_months,
            algo_params_json
        FROM forecast_runs
        WHERE {' AND '.join(where)}
        ORDER BY train_end, model_name, run_id
    """
    df = con.execute(sql, params).fetchdf()
    con.close()

    if df.empty:
        raise ValueError("[eval] no forecast_runs matched the eval spec")

    # optional anchor filter
    if spec.anchor_dates:
        anchors = set(pd.to_datetime(list(spec.anchor_dates)).date)
        df = df[df["train_end"].isin(anchors)].copy()
        if df.empty:
            raise ValueError("[eval] anchor_dates filter removed all runs")

    # --- prefer_batch_ids: if present, restrict to those batches first ---
    if getattr(spec, "prefer_batch_ids", None):
        pref = set(spec.prefer_batch_ids)
        df_pref = df[df["batch_id"].isin(pref)].copy()
        if df_pref.empty:
            raise ValueError("[eval] prefer_batch_ids filter removed all runs")
        df = df_pref

    # --- dedupe: keep exactly 1 run per (model_name, train_end) ---
    # deterministic: take max(run_id) within each group
    if getattr(spec, "dedupe_latest_per_model_anchor", True):
        df["train_end"] = pd.to_datetime(df["train_end"])
        df = (
            df.sort_values(["model_name", "train_end", "run_id"])
              .groupby(["model_name", "train_end"], as_index=False)
              .tail(1)
              .copy()
        )

    # --- require_models: restrict cohort to known set ---
    if getattr(spec, "require_models", None):
        req = set(spec.require_models)
        present = set(df["model_name"].astype(str).unique().tolist())
        missing = sorted(req - present)
        if missing:
            raise ValueError(f"[eval] missing required models in cohort: {missing}")
        df = df[df["model_name"].isin(req)].copy()

    # --- require_complete_cohort: keep only anchors where all models exist ---
    if getattr(spec, "require_complete_cohort", False):
        n_models = df["model_name"].nunique()
        counts = df.groupby("train_end")["model_name"].nunique()
        ok_anchors = counts[counts == n_models].index
        df = df[df["train_end"].isin(ok_anchors)].copy()
        if df.empty:
            raise ValueError("[eval] require_complete_cohort removed all runs (no anchor has full cohort)")

    return df


def _load_predictions(run_ids: List[int]) -> pd.DataFrame:
    con = get_connection()
    sql = f"""
        SELECT run_id, target_date, y_hat, y_hat_lo, y_hat_hi
        FROM forecast_predictions
        WHERE run_id IN ({','.join(['?']*len(run_ids))})
        ORDER BY run_id, target_date
    """
    df = con.execute(sql, run_ids).fetchdf()
    con.close()
    if df.empty:
        raise ValueError("[eval] no forecast_predictions found for selected runs")
    return df


def _load_actuals(metric_id: str, geo_id: str, property_type_id: str) -> pd.DataFrame:
    con = get_connection()
    sql = """
        SELECT date as target_date, value as y_true
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
        ORDER BY date
    """
    df = con.execute(sql, [metric_id, geo_id, property_type_id]).fetchdf()
    con.close()
    if df.empty:
        raise ValueError("[eval] no actuals found in fact_timeseries for target")
    return df


def _month_step(anchor: pd.Timestamp, target_date: pd.Timestamp) -> int:
    a = anchor.to_period("M")
    t = target_date.to_period("M")
    return int((t.year - a.year) * 12 + (t.month - a.month))


def build_eval_frame(spec: EvalSpec) -> pd.DataFrame:
    runs = _load_runs(spec)
    preds = _load_predictions(runs["run_id"].astype(int).tolist())
    actuals = _load_actuals(spec.metric_id, spec.geo_id, spec.property_type_id)

    # join predictions -> runs -> actuals
    df = preds.merge(runs, on="run_id", how="left")
    df["target_date"] = pd.to_datetime(df["target_date"])
    actuals["target_date"] = pd.to_datetime(actuals["target_date"])
    df = df.merge(actuals, on="target_date", how="left")

    # compute horizon step relative to train_end
    df["train_end"] = pd.to_datetime(df["train_end"])
    anchor_p = df["train_end"].dt.to_period("M")
    target_p = df["target_date"].dt.to_period("M")
    df["h_step"] = (target_p.dt.year - anchor_p.dt.year) * 12 + (target_p.dt.month - anchor_p.dt.month)
    df["h_step"] = df["h_step"].astype(int)


    # keep only true future horizons (1..H)
    df = df[(df["h_step"] >= 1) & (df["h_step"] <= df["horizon_max_months"])].copy()

    # enforce actual availability
    df = df[df["y_true"].notna()].copy()

    if df.empty:
        raise ValueError("[eval] after joining actuals + horizon filtering, no rows remain")

    # Optionally require full horizon per run (hard apples-to-apples)
    if spec.require_full_horizon:
        counts = df.groupby("run_id")["h_step"].nunique()
        ok = counts[counts == df.groupby("run_id")["horizon_max_months"].first()].index.tolist()
        df = df[df["run_id"].isin(ok)].copy()
        if df.empty:
            raise ValueError("[eval] require_full_horizon removed all runs (insufficient actuals)")

    # error columns
    df["err"] = df["y_hat"].astype(float) - df["y_true"].astype(float)
    df["abs_err"] = df["err"].abs()

    return df


def score_runs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces a run-level score table plus optional horizon-level breakdowns later.
    """
    out_rows: List[Dict[str, Any]] = []

    for run_id, g in df.groupby("run_id", sort=False):
        y_true = g["y_true"].astype(float)
        y_hat = g["y_hat"].astype(float)
        err = g["err"].astype(float)

        row: Dict[str, Any] = {
            "run_id": int(run_id),
            "model_name": str(g["model_name"].iloc[0]),
            "model_version": str(g["model_version"].iloc[0]),
            "run_kind": str(g["run_kind"].iloc[0]),
            "batch_id": str(g["batch_id"].iloc[0]),
            "data_asof": str(g["data_asof"].iloc[0]) if "data_asof" in g.columns else None,
            "anchor_date": str(pd.to_datetime(g["train_end"].iloc[0]).date()),
            "horizon": int(g["horizon_max_months"].iloc[0]),
            "n_points": int(len(g)),
            "rmse": rmse(err),
            "mae": mae(err),
            "wape": wape(y_true, y_hat),
            "mape": mape(y_true, y_hat),
            "smape": smape(y_true, y_hat),
        }

        # interval metrics only if present
        if g["y_hat_lo"].notna().any() and g["y_hat_hi"].notna().any():
            lo = g["y_hat_lo"].astype(float)
            hi = g["y_hat_hi"].astype(float)
            row["pi_coverage"] = interval_coverage(y_true, lo, hi)
            row["pi_width"] = interval_width(lo, hi)
        else:
            row["pi_coverage"] = None
            row["pi_width"] = None

        out_rows.append(row)

    scores = pd.DataFrame(out_rows)

    # Sort in a useful default order (lower is better)
    scores = scores.sort_values(["anchor_date", "wape", "rmse", "mae"], ascending=[True, True, True, True])
    return scores
