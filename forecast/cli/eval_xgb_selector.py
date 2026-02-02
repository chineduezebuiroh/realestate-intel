from __future__ import annotations
# forecast/cli/eval_xgb_selector.py

import json
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from forecast.core.anchors import AnchorPolicy, choose_anchors
from forecast.core.eval_batch import (
    append_jsonl,
    eval_out_dir,
    make_run_batch_id,
    new_eval_batch_id,
)
from forecast.models.xgb.backtest_selector_runner import run_xgb_selector


def _parse_targets_csv(path: str) -> List[Tuple[str, str, str]]:
    """
    CSV columns required: metric_id, geo_id, property_type_id
    """
    p = Path(path)
    df = pd.read_csv(p)
    missing = {"metric_id", "geo_id", "property_type_id"} - set(df.columns)
    if missing:
        raise SystemExit(f"[eval_xgb_selector] targets_csv missing columns: {sorted(missing)}")

    out: List[Tuple[str, str, str]] = []
    for _, r in df.iterrows():
        out.append((str(r["metric_id"]), str(r["geo_id"]), str(r["property_type_id"])))
    return out


def _parse_targets_inline(s: str) -> List[Tuple[str, str, str]]:
    """
    Format:
      "metric_id,geo_id,property_type_id; metric_id,geo_id,property_type_id; ..."
    """
    out: List[Tuple[str, str, str]] = []
    for part in [x.strip() for x in s.split(";") if x.strip()]:
        bits = [b.strip() for b in part.split(",")]
        if len(bits) != 3:
            raise SystemExit(f"[eval_xgb_selector] bad --targets item: {part!r}")
        out.append((bits[0], bits[1], bits[2]))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Eval batch: run XGB selector across targets x anchors.")
    p.add_argument("--artifact_root", default="artifacts/eval")
    p.add_argument("--eval_batch_id", default=None)
    p.add_argument("--targets_csv", default=None)
    p.add_argument("--targets", default=None, help='Inline targets: "metric,geo,pt; metric,geo,pt"')

    # Selector inputs
    p.add_argument("--horizon", type=int, default=3)
    p.add_argument("--data_asof", type=str, default=None)  # YYYY-MM-DD
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--xgb_top_k", type=int, default=50)
    p.add_argument("--metric_pt_cap", type=int, default=10)
    p.add_argument("--min_non_redfin", type=int, default=25)

    # Anchor policy (general engine supports many anchors; eval uses many, selector still single-run per anchor)
    p.add_argument("--min_train_len", type=int, default=72)
    p.add_argument("--anchor_step_months", type=int, default=12)
    p.add_argument("--max_anchors", type=int, default=4)
    p.add_argument("--latest_anchor_offset_months", type=int, default=None)
    p.add_argument("--require_full_horizon", action="store_true", help="Strict: anchor must have full horizon available")

    args = p.parse_args(argv)

    if not args.targets_csv and not args.targets:
        raise SystemExit("[eval_xgb_selector] provide --targets_csv or --targets")

    if args.targets_csv:
        targets = _parse_targets_csv(args.targets_csv)
    else:
        targets = _parse_targets_inline(args.targets)

    eval_batch_id = args.eval_batch_id or new_eval_batch_id("evalxgbsel")
    out_dir = eval_out_dir(args.artifact_root, eval_batch_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "manifest__xgb_selector.jsonl"

    # Normalize data_asof for anchor engine
    data_asof_ts = None
    if args.data_asof:
        data_asof_ts = pd.Timestamp(args.data_asof).to_period("M").to_timestamp(how="end")

    policy = AnchorPolicy(
        horizon=int(args.horizon),
        min_train_len=int(args.min_train_len),
        step_months=int(args.anchor_step_months),
        max_anchors=int(args.max_anchors),
        latest_anchor_offset_months=(int(args.latest_anchor_offset_months) if args.latest_anchor_offset_months is not None else None),
    )

    # batch header row (first line in manifest)
    append_jsonl(
        manifest_path,
        {
            "kind": "eval_batch_header",
            "eval_batch_id": eval_batch_id,
            "artifact_root": str(args.artifact_root),
            "policy": asdict(policy),
            "data_asof": (str(data_asof_ts.date()) if data_asof_ts is not None else None),
            "xgb_top_k": int(args.xgb_top_k),
            "metric_pt_cap": int(args.metric_pt_cap),
            "min_non_redfin": int(args.min_non_redfin),
            "seed": int(args.seed),
            "require_full_horizon": bool(args.require_full_horizon),
        },
    )

    n_ok = 0
    n_fail = 0

    for (metric_id, geo_id, property_type_id) in targets:
        # We need y for anchors; the selector runner already loads y internally, but
        # choose_anchors() needs y passed in.
        # Easiest (and cleanest): import TargetSpec loader here? No.
        # Instead: call the selector runner once with anchors_csv *only* when we already have anchors.
        # So we compute anchors by asking selector runner to load y? That would regress modularity.
        #
        # Proper approach: reuse the existing target-series loader.
        from forecast.feature_loader import TargetSpec, load_target_series_for_spec

        target = TargetSpec(metric_id=metric_id, geo_id=geo_id, property_type_id=property_type_id)
        y = load_target_series_for_spec(target).copy()
        y.index = pd.DatetimeIndex(y.index)
        y = y.sort_index()

        # IMPORTANT: choose anchors inside the requested as-of window
        anchors = choose_anchors(
            y,
            policy=policy,
            data_asof=data_asof_ts,
            anchors_csv=None,
            require_full_horizon=bool(args.require_full_horizon),
        )

        if not anchors:
            n_fail += 1
            append_jsonl(
                manifest_path,
                {
                    "kind": "run_result",
                    "eval_batch_id": eval_batch_id,
                    "metric_id": metric_id,
                    "geo_id": geo_id,
                    "property_type_id": str(property_type_id),
                    "status": "fail",
                    "error": "no anchors returned",
                    "anchors_n": 0,
                },
            )
            continue

        for a in anchors:
            anchor_date = pd.Timestamp(a).to_period("M").to_timestamp(how="end").date().isoformat()
            run_batch_id = make_run_batch_id(
                eval_batch_id=eval_batch_id,
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=str(property_type_id),
                anchor_date=anchor_date,
                horizon=int(args.horizon),
                seed=int(args.seed),
            )

            try:
                # Run selector for exactly one anchor (explicit)
                run_xgb_selector(
                    metric_id=metric_id,
                    geo_id=geo_id,
                    property_type_id=str(property_type_id),
                    horizon=int(args.horizon),
                    min_train_len=int(args.min_train_len),
                    anchor_step_months=int(args.anchor_step_months),
                    max_anchors=1,  # selector contract: one per call
                    latest_anchor_offset_months=None,
                    batch_id=run_batch_id,
                    data_asof=args.data_asof,
                    seed=int(args.seed),
                    artifact_root=str(out_dir),  # nests runs under this eval batch
                    xgb_top_k=int(args.xgb_top_k),
                    anchors_csv=str(anchor_date),  # exactly one
                    metric_pt_cap=int(args.metric_pt_cap),
                    min_non_redfin=int(args.min_non_redfin),
                    debug=False,
                )

                # selector runner writes:
                #   <out_dir>/<run_batch_id>/xgb/selected_features__anchor=YYYY-MM-DD.parquet
                #   <out_dir>/<run_batch_id>/xgb/selector_summary__anchor=YYYY-MM-DD.json
                run_dir = out_dir / run_batch_id / "xgb"
                out_parquet = run_dir / f"selected_features__anchor={anchor_date}.parquet"
                out_json = run_dir / f"selector_summary__anchor={anchor_date}.json"

                # read feature_set_sha256 from summary for indexing/diffs
                feature_set_sha256 = None
                if out_json.exists():
                    j = json.loads(out_json.read_text(encoding="utf-8"))
                    feature_set_sha256 = j.get("feature_set_sha256")

                n_ok += 1
                append_jsonl(
                    manifest_path,
                    {
                        "kind": "run_result",
                        "eval_batch_id": eval_batch_id,
                        "batch_id": run_batch_id,
                        "metric_id": metric_id,
                        "geo_id": geo_id,
                        "property_type_id": str(property_type_id),
                        "horizon": int(args.horizon),
                        "anchor_date": anchor_date,
                        "status": "ok",
                        "out_parquet": str(out_parquet),
                        "out_json": str(out_json),
                        "feature_set_sha256": feature_set_sha256,
                    },
                )

            except SystemExit as e:
                # Selector runner uses SystemExit for "refuse overwrite" and some hard-fails
                n_fail += 1
                append_jsonl(
                    manifest_path,
                    {
                        "kind": "run_result",
                        "eval_batch_id": eval_batch_id,
                        "batch_id": run_batch_id,
                        "metric_id": metric_id,
                        "geo_id": geo_id,
                        "property_type_id": str(property_type_id),
                        "horizon": int(args.horizon),
                        "anchor_date": anchor_date,
                        "status": "fail",
                        "error": str(e),
                    },
                )
            except Exception as e:
                n_fail += 1
                append_jsonl(
                    manifest_path,
                    {
                        "kind": "run_result",
                        "eval_batch_id": eval_batch_id,
                        "batch_id": run_batch_id,
                        "metric_id": metric_id,
                        "geo_id": geo_id,
                        "property_type_id": str(property_type_id),
                        "horizon": int(args.horizon),
                        "anchor_date": anchor_date,
                        "status": "fail",
                        "error": repr(e),
                    },
                )

    print(f"[eval_xgb_selector] eval_batch_id={eval_batch_id} out_dir={out_dir}")
    print(f"[eval_xgb_selector] manifest={manifest_path}")
    print(f"[eval_xgb_selector] ok={n_ok} fail={n_fail}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
