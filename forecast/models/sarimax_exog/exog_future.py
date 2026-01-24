from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from forecast.exog_forecast import forecast_exog_seasonal_naive


def _month_end(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.to_period("M").to_timestamp(how="end")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def build_exog_future(
    *,
    X_hist: pd.DataFrame,
    anchor_ts: pd.Timestamp,
    horizon: int,
    feature_ids: List[str],
    season: int = 12,
) -> pd.DataFrame:
    """
    Deterministically forecast each exog column using seasonal naive.
    Requires X_hist indexed by month-end timestamps and containing all feature_ids.
    Returns X_future indexed by the next `horizon` month-ends after anchor_ts.
    """
    anchor_ts = _month_end(pd.Timestamp(anchor_ts))

    # enforce column identity + order
    missing = [c for c in feature_ids if c not in X_hist.columns]
    if missing:
        raise ValueError(f"[sarimax_exog_live] X_hist missing exog cols: n={len(missing)} ex={missing[:5]}")
    X_hist = X_hist.loc[:, feature_ids].copy()

    # horizon month-ends after anchor
    start = _month_end(anchor_ts + pd.offsets.MonthEnd(1))
    end = _month_end(anchor_ts + pd.offsets.MonthEnd(horizon))
    horizon_idx = pd.date_range(start, end, freq="ME")

    out = {}
    for col in feature_ids:
        s = X_hist[col].astype(float)
        out[col] = forecast_exog_seasonal_naive(s_hist=s, horizon_idx=horizon_idx, season=season)

    X_future = pd.DataFrame(out, index=horizon_idx)
    # hard guard: no missing values allowed in future exog
    if X_future.isna().any().any():
        bad = X_future.isna().sum().sort_values(ascending=False)
        bad = bad[bad > 0].head(10)
        raise ValueError(f"[sarimax_exog_live] future exog has NaNs:\n{bad.to_string()}")

    return X_future


def write_exog_future_artifact(
    *,
    out_dir: Path,
    anchor_date: str,
    data_asof_effective: str,
    horizon: int,
    feature_ids: List[str],
    X_future: pd.DataFrame,
    design_matrix_audit: Dict[str, Any],
) -> Tuple[Path, Path]:
    """
    Writes:
      exog_future__anchor=YYYY-MM-DD__asof=YYYY-MM-DD__h=H.parquet
      exog_future__anchor=...json
    Returns (parquet_path, audit_path)
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = out_dir / f"exog_future__anchor={anchor_date}__asof={data_asof_effective}__h={int(horizon)}.parquet"
    audit_path = parquet_path.with_suffix(".json")

    # refuse overwrite (consistent with rest of artifacts)
    if parquet_path.exists() or audit_path.exists():
        raise SystemExit(f"[sarimax_exog_live] REFUSING to overwrite existing exog_future artifact: {parquet_path}")

    X_future.to_parquet(parquet_path, index=True)
    x_sha = _sha256_bytes(parquet_path.read_bytes())

    audit = {
        "audit_version": "v1",
        "kind": "sarimax_exog_exog_future",
        "anchor_date": anchor_date,
        "data_asof_effective": data_asof_effective,
        "horizon": int(horizon),
        "feature_ids": list(feature_ids),
        "exog_future_sha256": x_sha,
        "design_matrix_sha256": design_matrix_audit.get("design_matrix_sha256"),
        "feature_set_sha256": design_matrix_audit.get("feature_set_sha256"),
        "inputs": {
            "design_matrix_audit_json": design_matrix_audit.get("design_matrix_artifact", None) or "<unknown>",
        },
        "artifact": str(parquet_path),
    }
    audit_path.write_text(json.dumps(audit, indent=2))
    return parquet_path, audit_path
