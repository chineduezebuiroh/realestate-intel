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

    out = joined.groupby(["model", "anchor", "run_id"], as_index=False).apply(_score).reset_index(drop=True)
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

            dir_true = _safe_sign(delta_true)
            dir_hat = _safe_sign(delta_hat)
            dir_hit = int(dir_true == dir_hat)

            reg_true = _bin_regime(delta_true)
            reg_hat = _bin_regime(delta_hat)
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
                    "abs_delta_err_pctpts": abs(delta_hat - delta_true) * 100.0,
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
        required=True,
        help='JSON mapping model->(anchor_date->run_id). Example: \'{"sarimax_exog":{"2020-12-31":1220},"sarimax_univariate":{"2020-12-31":1233}}\'',
    )
    ap.add_argument("--horizons", default="6,12,18", help="Comma-separated horizons (months) for directional scoring.")
    ap.add_argument("--score_first_n", default="12,18", help="Comma-separated N for level scoring windows.")
    args = ap.parse_args()

    runs_map = parse_runs_json(args.runs_json)
    horizons = [int(x.strip()) for x in args.horizons.split(",") if x.strip()]
    score_ns = [int(x.strip()) for x in args.score_first_n.split(",") if x.strip()]

    # Build mapping table (model, anchor, run_id)
    rows = []
    for model, amap in runs_map.items():
        for anchor, rid in amap.items():
            rows.append({"model": model, "anchor": _month_end(pd.Timestamp(anchor)), "run_id": int(rid)})
    anchors_df = pd.DataFrame(rows)
    if anchors_df.empty:
        raise SystemExit("Empty runs_json mapping.")

    run_ids = sorted(anchors_df["run_id"].unique().tolist())

    con = duckdb.connect(args.db)

    # Load
    actual = load_actual_series(con, args.metric, args.geo, args.pt)
    preds = load_predictions(con, run_ids)
    runs = load_runs(con, run_ids)

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

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
