# forecast/run_backtest_batch.py

import argparse
import subprocess
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timezone

from .db_forecast import new_batch_id, get_connection
from .backtest_utils import month_end_index

BACKTEST_CMDS = [
    ("sarimax_backtest",      ["python", "-m", "forecast.backtest_sarimax_single"]),
    ("xgb_backtest",          ["python", "-m", "forecast.backtest_xgb_single"]),
    ("sarimax_exog_backtest", ["python", "-m", "forecast.backtest_sarimax_exog_single"]),
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

    idx = month_end_index(pd.to_datetime(df["date"]))
    return str(pd.DatetimeIndex(idx).max().date())  # YYYY-MM-DD


def write_manifest(batch_dir: Path, payload: dict) -> Path:
    batch_dir.mkdir(parents=True, exist_ok=True)
    p = batch_dir / "manifest.json"
    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    return p


def run_one(model_name, cmd_base, args_common):
    cmd = cmd_base + args_common
    print("[batch] running:", " ".join(cmd))
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        raise SystemExit(
            f"[batch] FAIL model={model_name} exit={e.returncode}\ncmd={' '.join(cmd)}"
        ) from e


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

    # determinism + artifacts
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--artifact_root", default="runs")
    ap.add_argument("--notes", default="")

    # centralized knobs
    ap.add_argument("--min_train_len", type=int, default=None)
    ap.add_argument("--anchor_step_months", type=int, default=None)
    ap.add_argument("--max_anchors", type=int, default=None)
    ap.add_argument("--latest_anchor_offset_months", type=int, default=None)

    # allow excluding families
    ap.add_argument("--skip_xgb", action="store_true")
    ap.add_argument("--skip_exog", action="store_true")

    # override dependency explicitly if needed
    ap.add_argument("--xgb_batch_id", default=None)

    args = ap.parse_args()

    batch_id = new_batch_id()
    data_asof = load_target_data_asof(args.metric_id, args.geo_id, args.property_type_id)
    xgb_batch_id = args.xgb_batch_id or batch_id

    batch_dir = Path(args.artifact_root) / batch_id
    manifest = {
        "batch_id": batch_id,
        "data_asof": data_asof,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "seed": args.seed,
        "artifact_root": str(Path(args.artifact_root).resolve()),
        "target": {
            "metric_id": args.metric_id,
            "geo_id": args.geo_id,
            "property_type_id": args.property_type_id,
        },
        "horizon": args.horizon,
        "knobs": {
            "min_train_len": args.min_train_len,
            "anchor_step_months": args.anchor_step_months,
            "max_anchors": args.max_anchors,
            "latest_anchor_offset_months": args.latest_anchor_offset_months,
        },
        "dependency": {
            "xgb_batch_id": xgb_batch_id,
        },
        "skips": {
            "skip_xgb": bool(args.skip_xgb),
            "skip_exog": bool(args.skip_exog),
        },
        "notes": args.notes,
    }
    manifest_path = write_manifest(batch_dir, manifest)

    print(f"[batch] batch_id={batch_id} data_asof={data_asof}")
    print(f"[batch] manifest={manifest_path}")

    args_common = [
        "--metric_id", args.metric_id,
        "--geo_id", args.geo_id,
        "--property_type_id", args.property_type_id,
        "--horizon", str(args.horizon),
        "--batch_id", batch_id,
        "--data_asof", data_asof,
    ]

    # pass overrides if set
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

        model_args = list(args_common)

        # only these scripts currently support artifacts + seed
        if model_name in ("xgb_backtest", "sarimax_exog_backtest"):
            model_args += ["--seed", str(args.seed)]
            model_args += ["--artifact_root", str(batch_dir)]

        if model_name == "sarimax_exog_backtest":
            model_args += ["--xgb_batch_id", xgb_batch_id]

        run_one(model_name, cmd_base, model_args)
    
    sanity_check(args.metric_id, args.geo_id, args.property_type_id, batch_id, data_asof)


if __name__ == "__main__":
    main()
