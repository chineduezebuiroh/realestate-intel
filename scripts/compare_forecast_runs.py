# scripts/compare_forecast_runs.py
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import duckdb
import numpy as np
import pandas as pd


# -----------------------------
# Helpers
# -----------------------------
def _month_end(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.to_period("M").to_timestamp("M")


def _add_months_me(anchor_me: pd.Timestamp, n_months: int) -> pd.Timestamp:
    # anchor_me is month-end timestamp
    return (anchor_me.to_period("M") + n_months).to_timestamp("M")


def _safe_sign(x: float, eps: float = 1e-12) -> int:
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def _bin_regime(delta_pct: float) -> str:
    # You can tweak these later, but don't overthink now.
    # These bins are deliberately coarse for capital timing.
    if delta_pct <= -0.05:
        return "down_big(<=-5%)"
    if delta_pct < 0.0:
        return "down_small(-5..0)"
    if delta_pct < 0.05:
        return "up_small(0..5)"
    return "up_big(>=5%)"


def _rmse(e: np.ndarray) -> float:
    return float(np.sqrt(np.mean(e * e))) if len(e) else float("nan")


def _mae(e: np.ndarray) -> float:
    return float(np.mean(np.abs(e))) if len(e) else float("nan")


def _mape(y_true: np.ndarray, y_hat: np.ndarray) -> float:
    denom = np.maximum(1e-9, np.abs(y_true))
    return float(np.mean(np.abs(y_hat - y_true) / denom) * 100.0) if len(y_true) else float("nan")


def _linreg_slope_intercept(x: np.ndarray, y: np.ndarray) -> Tuple[float, float, int]:
    # y ~ a + b*x
    m = np.isfinite(x) & np.isfinite(y)
    x = x[m]
    y = y[m]
    if len(x) < 2:
        return float("nan"), float("nan"), int(len(x))
    b, a = np.polyfit(x, y, 1)
    return float(b), float(a), int(len(x))


def compute_regime_thresholds_quantile(
    actual: pd.DataFrame,
    horizons: List[int],
    q: List[float] = [0.2, 0.4, 0.6, 0.8],
) -> Dict[int, List[float]]:
    """
    Returns {horizon_months: [q20, q40, q60, q80]} for delta_true (as fraction, not pct).
    Uses the actual series only.
    """
    a = actual[["target_date", "y"]].copy()
    a["target_date"] = pd.to_datetime(a["target_date"]).dt.to_period("M").dt.to_timestamp("M")
    s = a.set_index("target_date")["y"].astype(float).sort_index()

    out: Dict[int, List[float]] = {}
    for h in horizons:
        deltas = []
        for anchor_me in s.index:
            t_me = _add_months_me(anchor_me, h)
            if t_me not in s.index:
                continue
            y0 = float(s.loc[anchor_me])
            y1 = float(s.loc[t_me])
            if not np.isfinite(y0) or not np.isfinite(y1) or abs(y0) < 1e-12:
                continue
            deltas.append((y1 / y0) - 1.0)

        if len(deltas) < 30:
            # you can tighten this later; for now don’t pretend it’s robust
            out[h] = []
            continue

        qs = np.quantile(np.array(deltas, dtype=float), q).tolist()
        out[h] = [float(x) for x in qs]

    return out


def bin_regime_by_thresholds(delta: float, qs: List[float]) -> str:
    """
    delta is fraction (0.05 == +5%).
    qs is [q20, q40, q60, q80]
    """
    if not qs:
        return "regime_unknown"
    q20, q40, q60, q80 = qs
    if delta <= q20:
        return "R1_low"
    if delta <= q40:
        return "R2_midlow"
    if delta <= q60:
        return "R3_mid"
    if delta <= q80:
        return "R4_midhigh"
    return "R5_high"
    
# -----------------------------
# Core loads
# -----------------------------
def load_actual_series(con: duckdb.DuckDBPyConnection, metric: str, geo: str, pt: str) -> pd.DataFrame:
    q = """
    with t as (
      select
        (date_trunc('month', date) + interval '1 month' - interval '1 day')::date as month_end,
        date,
        value,
        row_number() over (
          partition by (date_trunc('month', date) + interval '1 month' - interval '1 day')::date
          order by date desc
        ) as rn
      from fact_timeseries
      where metric_id = ?
        and geo_id = ?
        and property_type_id = ?
    )
    select month_end::date as target_date, value::double as y
    from t
    where rn = 1
    order by target_date
    """
    df = con.execute(q, [metric, geo, pt]).df()
    if df.empty:
        raise SystemExit(f"No actual rows for metric={metric} geo={geo} pt={pt}")
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["target_date"] = df["target_date"].dt.to_period("M").dt.to_timestamp("M")
    return df


def load_runs(con: duckdb.DuckDBPyConnection, run_ids: List[int]) -> pd.DataFrame:
    q = f"""
    select
      run_id,
      model_name,
      model_version,
      target_metric_id,
      target_geo_id,
      target_property_type_id,
      train_end,
      horizon_max_months,
      batch_id,
      run_kind
    from forecast_runs
    where run_id in ({",".join(map(str, run_ids))})
    """
    df = con.execute(q).df()
    if df.empty:
        raise SystemExit("No forecast_runs rows for provided run_ids.")
    df["train_end"] = pd.to_datetime(df["train_end"])
    df["train_end"] = df["train_end"].dt.to_period("M").dt.to_timestamp("M")
    return df


def load_runs_by_batch(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    *,
    model_label: str,
    metric: str,
    geo: str,
    pt: str,
    horizon: Optional[int] = None,
    run_kind: Optional[str] = "backtest",
) -> pd.DataFrame:
    q = """
    select
      run_id,
      model_name,
      model_version,
      target_metric_id,
      target_geo_id,
      target_property_type_id,
      train_end,
      horizon_max_months,
      batch_id,
      run_kind
    from forecast_runs
    where batch_id = ?
      and target_metric_id = ?
      and target_geo_id = ?
      and cast(target_property_type_id as varchar) = ?
    """
    params = [batch_id, metric, geo, str(pt)]
    
    if horizon is not None:
        q += " and horizon_max_months = ?"
        params.append(int(horizon))
    
    if run_kind is not None:
        q += " and run_kind = ?"
        params.append(run_kind)
    
    df = con.execute(q, params).df()
    if df.empty:
        raise SystemExit(
            f"No forecast_runs rows for batch_id={batch_id!r} after filtering "
            f"metric={metric!r} geo={geo!r} pt={str(pt)!r} horizon={horizon!r} run_kind={run_kind!r}"
        )

    df["train_end"] = pd.to_datetime(df["train_end"])
    df["train_end"] = df["train_end"].dt.to_period("M").dt.to_timestamp("M")

    # attach label here so we can build anchors_df cleanly
    df["model"] = model_label
    return df


def load_predictions(con: duckdb.DuckDBPyConnection, run_ids: List[int]) -> pd.DataFrame:
    q = f"""
    select
      run_id,
      target_date::date as target_date,
      y_hat::double as y_hat
    from forecast_predictions
    where run_id in ({",".join(map(str, run_ids))})
    order by run_id, target_date
    """
    df = con.execute(q).df()
    if df.empty:
        raise SystemExit("No forecast_predictions rows for provided run_ids.")
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["target_date"] = df["target_date"].dt.to_period("M").dt.to_timestamp("M")
    return df


# -----------------------------
# Scoring
# -----------------------------
def score_levels(joined: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    joined columns expected: model, anchor, run_id, target_date, y_hat, y
    """
    def _score(g: pd.DataFrame) -> pd.Series:
        g = g.sort_values("target_date").head(n)
        y = g["y"].to_numpy(dtype=float)
        yhat = g["y_hat"].to_numpy(dtype=float)
        e = yhat - y
        return pd.Series(
            {
                "n": int(len(g)),
                "rmse": _rmse(e),
                "mae": _mae(e),
                "mape_pct": _mape(y, yhat),
                "start": str(g["target_date"].min().date()) if len(g) else None,
                "end": str(g["target_date"].max().date()) if len(g) else None,
            }
        )

    out = (
        joined.groupby(["model", "anchor", "run_id"], as_index=False, group_keys=False)
        .apply(_score, include_groups=False)
        .reset_index(drop=True)
    )
    return out


def score_directional(
    joined: pd.DataFrame,
    actual: pd.DataFrame,
    anchors: pd.DataFrame,
    horizons: List[int],
) -> pd.DataFrame:
    """
    Produce one row per (model, anchor, run_id, horizon_months) with:
      - delta_true_pct, delta_hat_pct
      - direction hit
      - regime hit
      - abs error on delta pct
    """
    thr = compute_regime_thresholds_quantile(actual, horizons=horizons)
    
    # Build quick lookup for actual y at any month-end
    a = actual[["target_date", "y"]].copy()
    a = a.set_index("target_date")["y"].astype(float)

    rows = []
    for r in anchors.itertuples(index=False):
        model = r.model
        run_id = int(r.run_id)
        anchor_me = pd.Timestamp(r.anchor)
        if anchor_me not in a.index:
            # If you can’t score anchor, you can’t use it.
            continue
        y0 = float(a.loc[anchor_me])

        for h in horizons:
            t_me = _add_months_me(anchor_me, h)
            if t_me not in a.index:
                continue
            y_true = float(a.loc[t_me])

            # predicted level at t_me
            g = joined[(joined["run_id"] == run_id) & (joined["target_date"] == t_me)]
            if g.empty:
                continue
            y_hat = float(g["y_hat"].iloc[0])

            
            # percent changes from anchor
            delta_true = (y_true / max(1e-12, y0)) - 1.0
            delta_hat = (y_hat / max(1e-12, y0)) - 1.0

            # magnitude / blowup penalties (pct-points)
            abs_delta_err_pctpts = abs(delta_hat - delta_true) * 100.0

            # penalize exaggerated confidence (only when forecast magnitude > true magnitude)
            overshoot_pctpts = max(0.0, (abs(delta_hat) - abs(delta_true)) * 100.0)

            # punish big misses harder than small ones
            squared_err_pctpts2 = ((delta_hat - delta_true) * 100.0) ** 2
            

            dir_true = _safe_sign(delta_true)
            dir_hat = _safe_sign(delta_hat)
            dir_hit = int(dir_true == dir_hat)

            qs = thr.get(int(h), [])
            reg_true = bin_regime_by_thresholds(delta_true, qs)
            reg_hat = bin_regime_by_thresholds(delta_hat, qs)

            reg_hit = int(reg_true == reg_hat)

            rows.append(
                {
                    "model": model,
                    "anchor": anchor_me,
                    "run_id": run_id,
                    "horizon_months": int(h),
                    "y0": y0,
                    "y_true": y_true,
                    "y_hat": y_hat,
                    "delta_true_pct": delta_true * 100.0,
                    "delta_hat_pct": delta_hat * 100.0,
                    "dir_true": dir_true,
                    "dir_hat": dir_hat,
                    "dir_hit": dir_hit,
                    "reg_true": reg_true,
                    "reg_hat": reg_hat,
                    "reg_hit": reg_hit,
                    "abs_delta_err_pctpts": abs_delta_err_pctpts,
                    "overshoot_pctpts": overshoot_pctpts,
                    "squared_err_pctpts2": squared_err_pctpts2,
                }
            )

    out = pd.DataFrame(rows)
    return out


def calibration_report(dir_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (model, horizon), compute y ~ a + b*x where:
      y = delta_true_pct, x = delta_hat_pct
    """
    rows = []
    for (model, h), g in dir_df.groupby(["model", "horizon_months"]):
        x = g["delta_hat_pct"].to_numpy(dtype=float)
        y = g["delta_true_pct"].to_numpy(dtype=float)
        b, a, n = _linreg_slope_intercept(x, y)
        rows.append(
            {
                "model": model,
                "horizon_months": int(h),
                "n": int(n),
                "cal_slope_b": b,
                "cal_intercept_a": a,
            }
        )
    return pd.DataFrame(rows).sort_values(["horizon_months", "model"])


def policy_eval_single_metric(dir_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (model, horizon): treat reg_true as the 'truth' regime and reg_hat as predicted.
    Returns confusion-style aggregates + a simple 'deploy/hold/defensive' action mapping (placeholder).
    """
    def action_from_regime(reg: str) -> str:
        if reg.startswith("up_big"):
            return "DEPLOY_AGGRESSIVE"
        if reg.startswith("up_small"):
            return "DEPLOY_NORMAL"
        if reg.startswith("down_small"):
            return "HOLD"
        return "DEFENSIVE"

    g = dir_df.copy()
    g["action_true"] = g["reg_true"].map(action_from_regime)
    g["action_hat"] = g["reg_hat"].map(action_from_regime)
    g["action_hit"] = (g["action_true"] == g["action_hat"]).astype(int)

    out = (
        g.groupby(["model", "horizon_months"])
        .agg(
            n=("run_id", "count"),
            action_hit_rate=("action_hit", "mean"),
            mean_abs_delta_err=("abs_delta_err_pctpts", "mean"),
        )
        .reset_index()
        .sort_values(["horizon_months", "action_hit_rate"], ascending=[True, False])
    )
    return out

# -----------------------------
# CLI
# -----------------------------
def parse_runs_json(s: str) -> Dict[str, Dict[str, int]]:
    """
    JSON format:
    {
      "sarimax_exog": {"2020-12-31": 1220, ...},
      "sarimax_univariate": {"2020-12-31": 1233, ...}
    }
    """
    obj = json.loads(s)
    if not isinstance(obj, dict):
        raise ValueError("--runs_json must be a JSON object")
    out: Dict[str, Dict[str, int]] = {}
    for model, amap in obj.items():
        if not isinstance(amap, dict):
            raise ValueError(f"runs_json[{model}] must be an object of anchor->run_id")
        out[model] = {}
        for anchor, rid in amap.items():
            out[model][str(anchor)] = int(rid)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare forecast runs: level + directional/regime + calibration.")
    ap.add_argument("--db", default="./data/market.duckdb")
    ap.add_argument("--metric", required=True)
    ap.add_argument("--geo", required=True)
    ap.add_argument("--pt", required=True)
    ap.add_argument(
        "--runs_json",
        required=False,
        help='JSON mapping model->(anchor_date->run_id). Example: \'{"sarimax_exog":{"2020-12-31":1220},"sarimax_univariate":{"2020-12-31":1233}}\'',
    )
    ap.add_argument(
        "--pick",
        action="append",
        default=[],
        help="Repeatable. Format: label=batch_id. Example: sarimax_univariate_old=phasec__...__v=02",
    )
    ap.add_argument("--horizons", default="6,12,18", help="Comma-separated horizons (months) for directional scoring.")
    ap.add_argument("--score_first_n", default="12,18", help="Comma-separated N for level scoring windows.")
    ap.add_argument(
        "--batch_horizon",
        type=int,
        default=None,
        help="Optional. When using --pick, restrict selected runs to horizon_max_months == this value. "
             "If omitted, do not filter by horizon.",
    )
    
    args = ap.parse_args()
    if not args.runs_json and not args.pick:
        raise SystemExit("Provide either --runs_json or at least one --pick label=batch_id")


    horizons = [int(x.strip()) for x in args.horizons.split(",") if x.strip()]
    score_ns = [int(x.strip()) for x in args.score_first_n.split(",") if x.strip()]
    
    con = duckdb.connect(args.db)
    
    # --- Build anchors_df and run_ids ---
    if args.pick:
        # Parse picks: label=batch_id
        picks: List[Tuple[str, str]] = []
        for s in args.pick:
            if "=" not in s:
                raise SystemExit(f"--pick must be label=batch_id, got: {s!r}")
            label, batch = s.split("=", 1)
            label = label.strip()
            batch = batch.strip()
            if not label or not batch:
                raise SystemExit(f"--pick must be label=batch_id, got: {s!r}")
            picks.append((label, batch))
    
        runs_parts = []
        for label, batch_id in picks:
            dfb = load_runs_by_batch(
                con,
                batch_id=batch_id,
                model_label=label,
                metric=args.metric,
                geo=args.geo,
                pt=args.pt,
                horizon=args.batch_horizon,
                run_kind="backtest",
            )
            runs_parts.append(dfb)
    
        runs_all = pd.concat(runs_parts, ignore_index=True)
    
        # Enforce target match early (don’t let mixed batches poison results)
        bad = runs_all[
            (runs_all["target_metric_id"] != args.metric)
            | (runs_all["target_geo_id"] != args.geo)
            | (runs_all["target_property_type_id"].astype(str) != str(args.pt))
        ]
        if len(bad):
            print("\nWARNING: Some runs in picked batches do not match metric/geo/pt you requested:")
            print(bad[["model","batch_id","run_id","target_metric_id","target_geo_id","target_property_type_id"]].to_string(index=False))
    
        # Build anchors_df: anchor := train_end (the contract)
        anchors_df = runs_all[["model", "run_id", "train_end"]].rename(columns={"train_end": "anchor"}).copy()

        dups = anchors_df.duplicated(subset=["model", "anchor"], keep=False)
        if dups.any():
            print("\nERROR: Duplicate (model, anchor) rows detected. You must have exactly one run per anchor per model.")
            print(anchors_df.loc[dups].sort_values(["model", "anchor", "run_id"]).to_string(index=False))
            raise SystemExit("Duplicate runs per anchor detected; fix batch selection or add disambiguation.")
    
        # Intersect anchors across all models (THIS is what makes comparisons legit)
        anchor_sets = {m: set(g["anchor"].tolist()) for m, g in anchors_df.groupby("model")}
        common = set.intersection(*anchor_sets.values()) if anchor_sets else set()
        if not common:
            raise SystemExit("No common anchors across picked batches. You are not comparing like-for-like.")
        before = len(anchors_df)
        anchors_df = anchors_df[anchors_df["anchor"].isin(common)].copy()
        after = len(anchors_df)
    
        # Optional: report shrink
        if after != before:
            print(f"[compare] anchor intersection kept {len(common)} anchors; rows {before} -> {after}")
    
        run_ids = sorted(anchors_df["run_id"].unique().tolist())
        runs = runs_all[runs_all["run_id"].isin(run_ids)].copy()
    
    else:
        # legacy mode: runs_json explicit mapping
        if not args.runs_json:
            raise SystemExit("Provide --runs_json or --pick")
        runs_map = parse_runs_json(args.runs_json)
    
        rows = []
        for model, amap in runs_map.items():
            for anchor, rid in amap.items():
                rows.append({"model": model, "anchor": _month_end(pd.Timestamp(anchor)), "run_id": int(rid)})
        anchors_df = pd.DataFrame(rows)
        if anchors_df.empty:
            raise SystemExit("Empty runs_json mapping.")
    
        run_ids = sorted(anchors_df["run_id"].unique().tolist())
        runs = load_runs(con, run_ids)
    
    # --- Load series & predictions ---
    actual = load_actual_series(con, args.metric, args.geo, args.pt)
    preds = load_predictions(con, run_ids)


    # Sanity: ensure runs match target
    bad = runs[
        (runs["target_metric_id"] != args.metric)
        | (runs["target_geo_id"] != args.geo)
        | (runs["target_property_type_id"].astype(str) != str(args.pt))
    ]
    if len(bad):
        print("\nWARNING: Some run_ids do not match metric/geo/pt you requested:")
        print(bad[["run_id", "target_metric_id", "target_geo_id", "target_property_type_id"]].to_string(index=False))

    # Join preds to actual
    df = preds.merge(actual, on="target_date", how="inner")
    if df.empty:
        raise SystemExit("No joined rows preds<->actual. Check fact coverage and target_date alignment.")

    df = df.merge(anchors_df, on="run_id", how="inner")
    if df.empty:
        raise SystemExit("No rows after mapping run_id -> model/anchor. Check run_ids.")

    # -----------------------------
    # LEVEL SCORING
    # -----------------------------
    for n in score_ns:
        out = score_levels(df, n=n)
        p = out.pivot_table(
            index="anchor",
            columns="model",
            values=["rmse", "mae", "mape_pct", "n", "run_id"],
            aggfunc="first",
        ).sort_index()

        print(f"\n=== LEVEL METRICS (first {n} months; per-anchor) ===")
        print(p.to_string())

        avg = out.groupby("model")[["rmse", "mae", "mape_pct"]].mean().sort_values("rmse")
        print(f"\n=== LEVEL METRICS (first {n} months; avg over anchors) ===")
        print(avg.to_string())

    # -----------------------------
    # DIRECTION / REGIME / CALIBRATION
    # -----------------------------
    dir_df = score_directional(df, actual, anchors_df, horizons=horizons)
    if dir_df.empty:
        raise SystemExit("No directional rows produced. Check anchors/horizons coverage.")

    # Aggregate per (model, horizon)
    agg = (
        dir_df.groupby(["model", "horizon_months"])
        .agg(
            n=("run_id", "count"),
            dir_hit_rate=("dir_hit", "mean"),
            reg_hit_rate=("reg_hit", "mean"),
            abs_delta_err_pctpts=("abs_delta_err_pctpts", "mean"),
        )
        .reset_index()
        .sort_values(["horizon_months", "reg_hit_rate", "dir_hit_rate"], ascending=[True, False, False])
    )

    print("\n=== DIRECTION / REGIME (avg over anchors) ===")
    print(agg.to_string(index=False))

    cal = calibration_report(dir_df)
    print("\n=== CALIBRATION (delta_true_pct ~ a + b*delta_hat_pct) ===")
    print(cal.to_string(index=False))

    pe = policy_eval_single_metric(dir_df)
    print("\n=== POLICY EVAL (single-metric action stub) ===")
    print(pe.to_string(index=False))

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
