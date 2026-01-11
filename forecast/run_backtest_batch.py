# forecast/run_backtest_batch.py

import argparse
import subprocess
import os
import duckdb
import pandas as pd

from .db_forecast import new_batch_id, get_connection  # or wherever these live
from .feature_loader import TargetSpec  # if you want
# If you have a canonical "load_target_series" helper, use that instead.

BACKTEST_CMDS = [
    ("sarimax_backtest",     ["python", "-m", "forecast.backtest_sarimax_single"]),
    ("xgb_backtest",         ["python", "-m", "forecast.backtest_xgb_single"]),
    ("sarimax_exog_backtest",["python", "-m", "forecast.backtest_sarimax_exog_single"]),
]

def load_target_data_asof(metric_id: str, geo_id: str, property_type_id: str) -> str:
    con = get_connection()
    df = con.execute(
        """
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id=? AND geo_id=? AND property_type_id=?
        ORDER BY date
        """,
        [metric_id, geo_id, property_type_id],
    ).fetchdf()
    con.close()

    if df.empty:
        raise SystemExit("No target data found.")

    # Normalize to month-end timestamps
    idx = pd.to_datetime(df["date"])
    idx = pd.PeriodIndex(idx, freq="M").to_timestamp(how="end")
    return str(idx.max().date())  # YYYY-MM-DD

def run_one(cmd_base, args_common):
    cmd = cmd_base + args_common
    print("[batch] running:", " ".join(cmd))
    subprocess.check_call(cmd)

def sanity_check(metric_id, geo_id, property_type_id, batch_id, data_asof):
    con = get_connection()
    df = con.execute(
        """
        SELECT model_name, COUNT(*) n
        FROM forecast_runs
        WHERE target_metric_id=?
          AND target_geo_id=?
          AND target_property_type_id=?
          AND run_kind='backtest'
          AND batch_id=?
          AND data_asof=?
        GROUP BY 1
        ORDER BY 1
        """,
        [metric_id, geo_id, property_type_id, batch_id, data_asof],
    ).fetchdf()
    con.close()

    have = set(df["model_name"].tolist()) if not df.empty else set()
    need = set(name for name, _ in BACKTEST_CMDS)
    missing = sorted(list(need - have))
    if missing:
        raise SystemExit(f"[batch] FAIL: missing model families in batch: {missing}\n{df}")
    print("[batch] PASS sanity check:\n", df.to_string(index=False))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metric_id", required=True)
    ap.add_argument("--geo_id", required=True)
    ap.add_argument("--property_type_id", required=True)
    ap.add_argument("--horizon", type=int, default=12)

    # These should be centralized knobs
    ap.add_argument("--min_train_len", type=int, default=None)
    ap.add_argument("--anchor_step_months", type=int, default=None)
    ap.add_argument("--max_anchors", type=int, default=None)
    ap.add_argument("--latest_anchor_offset_months", type=int, default=None)

    # Optional: allow excluding families
    ap.add_argument("--skip_xgb", action="store_true")
    ap.add_argument("--skip_exog", action="store_true")

    args = ap.parse_args()

    batch_id = new_batch_id()
    data_asof = load_target_data_asof(args.metric_id, args.geo_id, args.property_type_id)

    print(f"[batch] batch_id={batch_id} data_asof={data_asof}")

    args_common = [
        "--metric_id", args.metric_id,
        "--geo_id", args.geo_id,
        "--property_type_id", args.property_type_id,
        "--horizon", str(args.horizon),
        "--batch_id", batch_id,
        "--data_asof", data_asof,
    ]
    
    # only pass overrides if set (and not None)
    if args.min_train_len is not None:
        args_common += ["--min_train_len", str(args.min_train_len)]
    if args.anchor_step_months is not None:
        args_common += ["--anchor_step_months", str(args.anchor_step_months)]
    if args.max_anchors is not None:
        args_common += ["--max_anchors", str(args.max_anchors)]
    if args.latest_anchor_offset_months is not None:
        args_common += ["--latest_anchor_offset_months", str(args.latest_anchor_offset_months)]    

    for model_name, cmd_base in BACKTEST_CMDS:
        if model_name == "xgb_backtest" and args.skip_xgb:
            continue
        if model_name == "sarimax_exog_backtest" and args.skip_exog:
            continue
        run_one(cmd_base, args_common)

    sanity_check(args.metric_id, args.geo_id, args.property_type_id, batch_id, data_asof)

if __name__ == "__main__":
    main()
