from __future__ import annotations
# forecast/consume_selected_features.py

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import hashlib

from typing import List, Optional, Tuple, Dict, Set

import pandas as pd

from .feature_loader import TargetSpec, FeatureSpec, build_design_matrix
from .backtest_utils import month_end_index


def _parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def _parse_feature_id(fid: str) -> Tuple[str, int]:
    """
    feature_id format:
      <metric_id>__<geo_id>__<property_type_id>__<source_id>_lagK
    """
    if "_lag" not in fid:
        raise ValueError(f"feature_id missing _lag: {fid}")
    base, lag_s = fid.rsplit("_lag", 1)
    lag = int(lag_s)
    parts = base.split("__")
    if len(parts) != 4:
        raise ValueError(f"feature_id base not 4-part: {fid}")
    return base, lag


def _feature_specs_from_selected_features(df: pd.DataFrame) -> List[FeatureSpec]:
    """
    Convert selected_features parquet rows (feature_id like base_lagK) into FeatureSpec objects.

    Expected feature_id format:
      {metric_id}__{geo_id}__{property_type_id}__{source_id}_lag{K}
    """
    if "feature_id" not in df.columns:
        raise ValueError("selected_features df missing required column: feature_id")

    # base -> (metric_id, geo_id, property_type_id, source_id, lags)
    base_to_lags: Dict[str, Set[int]] = {}
    base_to_parts: Dict[str, Tuple[str, str, str, str]] = {}

    for fid in df["feature_id"].astype(str).tolist():
        if "_lag" not in fid:
            raise ValueError(f"Bad feature_id (missing _lag): {fid}")

        base, lag_str = fid.rsplit("_lag", 1)
        try:
            lag = int(lag_str)
        except Exception:
            raise ValueError(f"Bad feature_id lag suffix: {fid}")

        parts = base.split("__")
        if len(parts) != 4:
            raise ValueError(f"Bad feature_id base (expected 4-part '__' id): {fid}")

        metric_id, geo_id, pt_raw, source_id = parts

        base_to_lags.setdefault(base, set()).add(lag)
        base_to_parts.setdefault(base, (metric_id, geo_id, pt_raw, source_id))

    specs: List[FeatureSpec] = []
    for base, lags_set in sorted(base_to_lags.items()):
        metric_id, geo_id, pt_raw, source_id = base_to_parts[base]

        # Normalize property_type_id: "all" -> None (your loader treats None as 'all')
        property_type_id = None if pt_raw == "all" else pt_raw

        specs.append(
            FeatureSpec(
                name=base,                       # <-- REQUIRED, and must match base_id exactly
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=property_type_id,
                source_id=source_id,
                lags=tuple(sorted(lags_set)),
            )
        )

    if not specs:
        raise ValueError("No FeatureSpecs constructed from selected_features (empty after parsing).")

    return specs


def consume_selected_features(
    *,
    batch_id: str,
    anchor_date: str,
    target_metric_id: str,
    target_geo_id: str,
    target_property_type_id: str,
    top_k: int = 100,
    artifact_root: str = "runs",
    min_obs: int = 60,
    overwrite: bool = False,   # NEW
) -> Path:
    """
    Build a deterministic SARIMAX-exog design matrix from the selector artifact.

    Returns path to the emitted design matrix parquet.
    """
    anchor_dt = _parse_date(anchor_date)

    in_path = Path(artifact_root) / batch_id / "xgb" / f"selected_features__anchor={anchor_dt.isoformat()}.parquet"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing selector artifact: {in_path}")

    df = pd.read_parquet(in_path)
    if df.empty:
        raise ValueError(f"Selector artifact empty: {in_path}")

    # --- contract checks ---
    required_cols = {
        "feature_id",
        "rank",
        "data_asof",
        "data_asof_requested",
        "data_asof_effective",
        "feature_set_sha256",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Selector artifact missing columns: {sorted(missing)}")

    sha_vals = df["feature_set_sha256"].dropna().unique().tolist()
    if len(sha_vals) != 1:
        raise ValueError(f"Expected exactly 1 feature_set_sha256, got {len(sha_vals)}")

    # enforce top_k deterministically by rank
    df = df.sort_values("rank", ascending=True).head(int(top_k)).copy()

    # --- ordered feature_ids for audit (must match top_k selection) ---
    feature_ids = df["feature_id"].astype(str).tolist()
    if len(feature_ids) != int(top_k):
        raise SystemExit(f"[consume] REFUSING: expected {top_k} feature_ids, got {len(feature_ids)}")

    # use EFFECTIVE as-of
    data_asof_requested = _parse_date(str(df["data_asof_requested"].iloc[0]))
    data_asof_effective = _parse_date(str(df["data_asof_effective"].iloc[0]))


    # build target spec pinned to as-of
    target = TargetSpec(
        metric_id=target_metric_id,
        geo_id=target_geo_id,
        property_type_id=str(target_property_type_id),
        data_asof=data_asof_effective,
    )

    specs = _feature_specs_from_selected_features(df)
    if len(specs) == 0:
        raise SystemExit("[consume] 0 specs after parsing — refusing to run.")

    # Build design matrix (complete-case for SARIMAX-exog)
    y, X, base_series = build_design_matrix(
        target=target,
        feature_specs=specs,
        min_obs=min_obs,
        drop_feature_na=True,
    )
    if X.shape[1] != int(top_k):
        raise SystemExit(f"[consume] REFUSING: X has {X.shape[1]} features, expected top_k={top_k}")
    
    MIN_ROWS = 120  # pick a sane floor for now; later move to policy
    if len(X) < MIN_ROWS:
        raise SystemExit(f"[consume] REFUSING: n_rows={len(X)} < MIN_ROWS={MIN_ROWS}")


    # extra sanity: month-end index
    y.index = month_end_index(y.index)
    X.index = month_end_index(X.index)

    if len(X.columns) == 0:
        raise ValueError("Built empty X; refusing to emit design matrix.")

    out_dir = Path(artifact_root) / batch_id / "sarimax_exog"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"design_matrix__anchor={anchor_dt.isoformat()}__asof={data_asof_effective.isoformat()}.parquet"

    if out_path.exists() and not overwrite:
        raise SystemExit(
            f"REFUSING to overwrite existing design matrix: {out_path}\n"
            "Use --overwrite, a fresh --batch_id, or delete this file."
        )

    # store y + X together (simple)
    df_out = pd.concat([y.rename("y"), X], axis=1)
    df_out.to_parquet(out_path, index=True)
    design_matrix_sha256 = hashlib.sha256(out_path.read_bytes()).hexdigest()

    # audit sidecar
    audit = {
        "audit_version": "v1",
        "batch_id": batch_id,
        "anchor_date": anchor_dt.isoformat(),
        "data_asof_requested": data_asof_requested.isoformat(),
        "data_asof_effective": data_asof_effective.isoformat(),
        "feature_set_sha256": sha_vals[0],
        "feature_ids": feature_ids,                    # NEW (ordered)
        "design_matrix_sha256": design_matrix_sha256,  # NEW
        "top_k": int(top_k),
        "n_rows": int(df_out.shape[0]),
        "n_features": int(X.shape[1]),
        "target": {
            "metric_id": target_metric_id,
            "geo_id": target_geo_id,
            "property_type_id": str(target_property_type_id),
        },
        "selector_artifact": str(in_path),
        "design_matrix_artifact": str(out_path),
    }

    (out_path.with_suffix(".json")).write_text(json.dumps(audit, indent=2))

    print(f"[consume] wrote design matrix: {out_path}")
    print(f"[consume] wrote audit: {out_path.with_suffix('.json')}")
    print(f"[consume] n_rows={df_out.shape[0]} n_features={X.shape[1]} asof={data_asof_effective.isoformat()}")

    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch_id", required=True)
    ap.add_argument("--anchor_date", required=True)  # YYYY-MM-DD
    ap.add_argument("--metric_id", required=True)
    ap.add_argument("--geo_id", required=True)
    ap.add_argument("--property_type_id", required=True)
    ap.add_argument("--top_k", type=int, default=100)
    ap.add_argument("--artifact_root", type=str, default="runs")
    ap.add_argument("--min_obs", type=int, default=60)
    ap.add_argument("--overwrite", action="store_true", help="Allow overwriting existing design matrix artifacts")
    
    args = ap.parse_args()

    consume_selected_features(
        batch_id=args.batch_id,
        anchor_date=args.anchor_date,
        target_metric_id=args.metric_id,
        target_geo_id=args.geo_id,
        target_property_type_id=args.property_type_id,
        top_k=args.top_k,
        artifact_root=args.artifact_root,
        min_obs=args.min_obs,
        overwrite=args.overwrite,   # NEW
    )


if __name__ == "__main__":
    main()
