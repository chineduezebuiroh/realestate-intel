from __future__ import annotations
# forecast/cli/eval.py

import argparse
import json
import hashlib
from pathlib import Path
import pandas as pd

from forecast.eval.core import EvalSpec, build_eval_frame, score_runs


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate forecast runs apples-to-apples.")
    ap.add_argument("--metric_id", required=True)
    ap.add_argument("--geo_id", required=True)
    ap.add_argument("--property_type_id", required=True)
    ap.add_argument("--freq", default="M")

    ap.add_argument("--run_kind", action="append", default=["backtest"])
    ap.add_argument("--batch_id", action="append", default=None)
    ap.add_argument("--model_name", action="append", default=None)
    ap.add_argument("--anchor", action="append", default=None)

    ap.add_argument("--eval_batch_id", required=True)
    ap.add_argument("--artifact_root", default="runs")
    ap.add_argument("--allow_partial", action="store_true", help="Allow runs with incomplete horizons")

    args = ap.parse_args()

    spec = EvalSpec(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        freq=args.freq,
        run_kinds=tuple(args.run_kind),
        batch_ids=tuple(args.batch_id) if args.batch_id else None,
        model_names=tuple(args.model_name) if args.model_name else None,
        anchor_dates=tuple(args.anchor) if args.anchor else None,
        require_full_horizon=not bool(args.allow_partial),
    )

    df = build_eval_frame(spec)
    scores = score_runs(df)

    out_dir = Path(args.artifact_root) / args.eval_batch_id / "eval"
    out_dir.mkdir(parents=True, exist_ok=True)

    score_path = out_dir / "score_table.parquet"
    frame_path = out_dir / "eval_frame.parquet"
    audit_path = out_dir / "audit.json"

    # refuse overwrite
    for p in (score_path, frame_path, audit_path):
        if p.exists():
            raise SystemExit(f"[eval] REFUSING to overwrite existing evaluation artifact: {p}")

    scores.to_parquet(score_path, index=False)
    df.to_parquet(frame_path, index=False)

    audit = {
        "audit_version": "v1",
        "spec": {
            "metric_id": spec.metric_id,
            "geo_id": spec.geo_id,
            "property_type_id": spec.property_type_id,
            "freq": spec.freq,
            "run_kinds": list(spec.run_kinds),
            "batch_ids": list(spec.batch_ids) if spec.batch_ids else None,
            "model_names": list(spec.model_names) if spec.model_names else None,
            "anchor_dates": list(spec.anchor_dates) if spec.anchor_dates else None,
            "require_full_horizon": spec.require_full_horizon,
        },
        "artifacts": {
            "score_table": str(score_path),
            "eval_frame": str(frame_path),
        },
        "sha256": {
            "score_table": _sha256_file(score_path),
            "eval_frame": _sha256_file(frame_path),
        },
        "counts": {
            "n_rows_eval_frame": int(len(df)),
            "n_runs_scored": int(scores["run_id"].nunique()) if not scores.empty else 0,
        },
    }
    audit_path.write_text(json.dumps(audit, indent=2))
    print(f"[eval] wrote score_table: {score_path}")
    print(f"[eval] wrote eval_frame: {frame_path}")
    print(f"[eval] wrote audit: {audit_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
