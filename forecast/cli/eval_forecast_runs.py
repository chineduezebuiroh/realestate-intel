from __future__ import annotations
# forecast/cli/eval_forecast_runs.py

"""
LEGACY eval CLI for scoring rows in forecast_runs (DB-backed).

Not used by selector eval batching (see forecast/cli/eval_xgb_selector.py).
Keep until Phase C cleanup; do not extend.
"""

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

    ap.add_argument("--run_kind", action="append", default=None)
    ap.add_argument("--batch_id", action="append", default=None)
    ap.add_argument("--model_name", action="append", default=None)
    ap.add_argument("--anchor", action="append", default=None)

    ap.add_argument("--eval_batch_id", required=True)
    ap.add_argument("--artifact_root", default="runs")
    ap.add_argument("--allow_partial", action="store_true", help="Allow runs with incomplete horizons")
    
    ap.add_argument("--prefer_batch_id", action="append", default=None)
    ap.add_argument("--require_model", action="append", default=None)
    ap.add_argument("--no_complete_cohort", action="store_true")
    ap.add_argument("--no_dedupe", action="store_true")

    ap.add_argument("--horizon", type=int, default=None, help="Require exact horizon_max_months for scored runs")
    ap.add_argument("--data_asof", type=str, default=None, help="Require runs to have exact data_asof (YYYY-MM-DD)")
    ap.add_argument("--cohort", type=str, default=None, help="Cohort rule: latest_common")
    ap.add_argument("--cohort_model", action="append", default=None, help="Restrict cohort intersection to these model_name(s)")


    args = ap.parse_args()

    run_kinds = tuple(args.run_kind) if args.run_kind else ("backtest",)

    spec = EvalSpec(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        freq=args.freq,
        run_kinds=run_kinds,
        batch_ids=tuple(args.batch_id) if args.batch_id else None,
        model_names=tuple(args.model_name) if args.model_name else None,
        anchor_dates=tuple(args.anchor) if args.anchor else None,
        require_full_horizon=not bool(args.allow_partial),
        # New fields
        prefer_batch_ids=tuple(args.prefer_batch_id) if args.prefer_batch_id else None,
        dedupe_latest_per_model_anchor=not bool(args.no_dedupe),
        require_models=tuple(args.require_model) if args.require_model else None,
        require_complete_cohort=not bool(args.no_complete_cohort),

        horizon=args.horizon,
        data_asof_exact=args.data_asof,
        cohort=args.cohort,
        cohort_models=tuple(args.cohort_model) if args.cohort_model else None,
    )

    df = build_eval_frame(spec)

    cohort_info = None
    if spec.cohort:
        anchors = sorted(pd.to_datetime(df["train_end"]).dt.date.astype(str).unique().tolist())
        cohort_info = {
            "anchors": anchors,
            "models": sorted(df["model_name"].astype(str).unique().tolist()),
            "n_anchors": int(pd.to_datetime(df["train_end"]).dt.date.nunique()),
        }
    
    scores = score_runs(df)

    def _format_summary_md(scores: pd.DataFrame, audit: dict, cohort_info: dict | None) -> str:
        # aggregate table
        agg = (
            scores.groupby("model_name")
            .agg(
                n_runs=("run_id", "nunique"),
                median_wape=("wape", "median"),
                mean_wape=("wape", "mean"),
                median_rmse=("rmse", "median"),
                mean_rmse=("rmse", "mean"),
            )
            .sort_values(["median_wape", "median_rmse"])
        )
    
        # per-anchor wins on WAPE
        wins = {}
        ties = 0
        total = 0
        for anchor, g in scores.groupby("anchor_date"):
            g2 = g.sort_values("wape", ascending=True)
            if len(g2) < 2:
                continue
            total += 1
            if abs(float(g2.iloc[0]["wape"]) - float(g2.iloc[1]["wape"])) < 1e-12:
                ties += 1
            else:
                m = str(g2.iloc[0]["model_name"])
                wins[m] = wins.get(m, 0) + 1
    
        # spec section
        spec = audit.get("spec", {})
        lines = []
        lines.append("# Eval Summary")
        lines.append("")
        lines.append("## Spec")
        for k in ["metric_id","geo_id","property_type_id","freq","run_kinds","horizon","data_asof_exact","cohort","cohort_models","require_full_horizon"]:
            if k in spec:
                lines.append(f"- **{k}**: `{spec.get(k)}`")
        lines.append("")
        if cohort_info:
            lines.append("## Cohort")
            lines.append(f"- **n_anchors**: `{cohort_info.get('n_anchors')}`")
            lines.append(f"- **models**: `{cohort_info.get('models')}`")
            lines.append(f"- **anchors**: `{cohort_info.get('anchors')}`")
            lines.append("")
        lines.append("## Aggregate (lower is better)")
        lines.append("")
        lines.append(agg.reset_index().to_string(index=False))
        lines.append("")
        lines.append("## WAPE wins per anchor")
        lines.append(f"- wins: `{wins}`")
        lines.append(f"- ties: `{ties}`")
        lines.append(f"- total_anchors_scored: `{total}`")
        lines.append("")
        lines.append("## Artifacts")
        lines.append(f"- score_table: `{audit['artifacts']['score_table']}`")
        lines.append(f"- eval_frame: `{audit['artifacts']['eval_frame']}`")
        lines.append(f"- sha256.score_table: `{audit['sha256']['score_table']}`")
        lines.append(f"- sha256.eval_frame: `{audit['sha256']['eval_frame']}`")
        lines.append("")
        return "\n".join(lines)


    out_dir = Path(args.artifact_root) / args.eval_batch_id / "eval"
    out_dir.mkdir(parents=True, exist_ok=True)

    score_path = out_dir / "score_table.parquet"
    frame_path = out_dir / "eval_frame.parquet"
    audit_path = out_dir / "audit.json"
    summary_path = out_dir / "summary.md"

    # refuse overwrite
    for p in (score_path, frame_path, audit_path, summary_path):
        if p.exists():
            raise SystemExit(f"[eval] REFUSING to overwrite existing evaluation artifact: {p}")

    if args.cohort:
        scores["cohort"] = args.cohort
        scores["cohort_models"] = ",".join(args.cohort_model) if args.cohort_model else None
        scores["cohort_data_asof_exact"] = args.data_asof
        scores["cohort_horizon"] = args.horizon



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
            
            "horizon": spec.horizon,
            "data_asof_exact": spec.data_asof_exact,
            "cohort": spec.cohort,
            "cohort_models": list(spec.cohort_models) if spec.cohort_models else None,

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
    audit["cohort"] = cohort_info
    
    audit_path.write_text(json.dumps(audit, indent=2))
    summary_path.write_text(_format_summary_md(scores, audit, cohort_info))
    print(f"[eval] wrote summary: {summary_path}")

    print(f"[eval] wrote score_table: {score_path}")
    print(f"[eval] wrote eval_frame: {frame_path}")
    print(f"[eval] wrote audit: {audit_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
