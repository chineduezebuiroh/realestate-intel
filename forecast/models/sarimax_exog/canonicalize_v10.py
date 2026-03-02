from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

from forecast.models.sarimax_exog.policy_b import (
    PolicyBThresholds,
    compute_exog_diagnostics,
    enforce_policy_b,
    PolicyBViolation,
    estimate_arima_param_count,
)
from forecast.models.sarimax_exog.core import SarimaxExogSpec
from forecast.models.sarimax_exog.bridge_runner import (
    _load_canonical_exog_df,   # reuse your loader for v09.0 inputs
    _build_design_matrix_from_selected_features,
    _zscore_exogs_train_only,
)
from forecast.core.db_forecast import get_connection


@dataclass(frozen=True)
class CanonicalizeConfig:
    artifact_root: str
    input_stability_version: str  # e.g. v09.0
    output_stability_version: str  # e.g. v10.0
    metric_id: str
    geo_id: str
    property_type_id: str
    data_asof: str
    anchor_date: str
    horizon: int
    min_train_len: int

    # selection goals
    max_exogs_out: int = 30

    # Policy B thresholds
    min_obs_per_param: float = 5.0
    max_exog_cond: float = 1e12
    require_full_rank: bool = True
    svd_rtol: float = 1e-12


def _resolve_out_csv(cfg: CanonicalizeConfig, n: int) -> Path:
    root = Path(cfg.artifact_root.rstrip("/"))
    out_dir = root / "canonical_exogs" / cfg.output_stability_version
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"canonical_exog_set__metric={cfg.metric_id}__n={int(n)}.csv"


def _resolve_out_audit(cfg: CanonicalizeConfig, n: int) -> Path:
    root = Path(cfg.artifact_root.rstrip("/"))
    out_dir = root / "canonical_exogs" / cfg.output_stability_version
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"canonical_exog_audit__metric={cfg.metric_id}__n={int(n)}__anchor={cfg.anchor_date}__asof={cfg.data_asof}.json"


def build_canonical_exogs_v10(cfg: CanonicalizeConfig) -> tuple[Path, Path]:
    # Load ranked candidates from prior canonical file
    df_in, in_path, in_sha = _load_canonical_exog_df(
        artifact_root=cfg.artifact_root,
        stability_version=cfg.input_stability_version,
        metric_id=cfg.metric_id,
        n=1000,  # load plenty; we’ll stop at max_exogs_out
        override_csv=None,
    )
    if df_in is None or df_in.empty:
        raise ValueError("input canonical exogs not found / empty")

    ranked = df_in.sort_values(["rank", "feature_id"], ascending=[True, True])["feature_id"].astype(str).tolist()

    thresholds = PolicyBThresholds(
        min_obs_per_param=float(cfg.min_obs_per_param),
        max_exog_cond=float(cfg.max_exog_cond),
        require_full_rank=bool(cfg.require_full_rank),
        svd_rtol=float(cfg.svd_rtol),
    )
    spec = SarimaxExogSpec()
    arima_param_count = estimate_arima_param_count(
        order=tuple(spec.order),
        seasonal_order=tuple(spec.seasonal_order),
        trend=None,
    )

    accepted: list[str] = []
    rejected: list[dict] = []

    con = get_connection()
    try:
        for fid in ranked:
            if len(accepted) >= int(cfg.max_exogs_out):
                break

            trial = accepted + [fid]

            # Build design matrix window using bridge’s deterministic builder
            dm_win, effective_fids, dropped = _build_design_matrix_from_selected_features(
                con=con,
                metric_id=cfg.metric_id,
                geo_id=cfg.geo_id,
                property_type_id=cfg.property_type_id,
                data_asof=pd.to_datetime(cfg.data_asof).date(),
                feature_ids=trial,
                anchor_date=cfg.anchor_date,
                horizon=int(cfg.horizon),
                min_train_len=int(cfg.min_train_len),
            )

            # Split and slice train rows up to anchor
            y_full = dm_win["y"].astype(float)
            X_full = dm_win.drop(columns=["y"]).astype(float)

            anchor_ts = pd.Timestamp(cfg.anchor_date).to_period("M").to_timestamp(how="end")
            y_train = y_full.loc[:anchor_ts]
            X_train = X_full.loc[:anchor_ts]

            # Train-only z-score (same transform as bridge runner)
            X_train_z, _, _ = _zscore_exogs_train_only(X_train, X_train)  # future not needed for diag

            # Complete-case for diagnostics (same as fit expectation)
            train_mask = y_train.notna() & X_train_z.notna().all(axis=1)
            y_train2 = y_train.loc[train_mask]
            X_train2 = X_train_z.loc[train_mask]

            diag = compute_exog_diagnostics(X_train2.to_numpy(dtype=float), svd_rtol=thresholds.svd_rtol)

            context = {
                "metric_id": cfg.metric_id,
                "geo_id": cfg.geo_id,
                "property_type_id": cfg.property_type_id,
                "anchor_date": cfg.anchor_date,
                "data_asof": cfg.data_asof,
                "horizon": int(cfg.horizon),
                "spec": {"order": list(spec.order), "seasonal_order": list(spec.seasonal_order)},
            }

            try:
                enforce_policy_b(
                    n_obs_train=int(diag["n_obs"]),
                    n_exogs=int(diag["n_exogs"]),
                    arima_param_count=int(arima_param_count),
                    exog_rank=int(diag["exog_rank"]),
                    exog_cond=float(diag["exog_cond"]),
                    thresholds=thresholds,
                    context=context,
                )
                accepted.append(fid)
            except PolicyBViolation as e:
                rejected.append(
                    {
                        "feature_id": fid,
                        "failed_checks": e.report.get("failed_checks"),
                        "diagnostics": e.report.get("diagnostics"),
                    }
                )
                continue

    finally:
        try:
            con.close()
        except Exception:
            pass

    if not accepted:
        raise ValueError("no features accepted under Policy B thresholds")

    # Write output canonical file (same schema your bridge runner expects)
    out_rows = []
    for i, fid in enumerate(accepted, start=1):
        # parse base_feature_id + lag from fid to match expected schema
        base, lag = fid.rsplit("_lag", 1)
        out_rows.append(
            {
                "base_feature_id": base,
                "best_lead_mode": int(lag),
                "canonical_rank": int(i),
            }
        )

    out_df = pd.DataFrame(out_rows)

    out_csv = _resolve_out_csv(cfg, n=len(accepted))
    out_df.to_csv(out_csv, index=False)

    audit = {
        "input": {
            "stability_version": cfg.input_stability_version,
            "canonical_csv_path": in_path,
            "canonical_csv_sha256": in_sha,
        },
        "output": {
            "stability_version": cfg.output_stability_version,
            "canonical_csv_path": str(out_csv),
            "n_accepted": int(len(accepted)),
            "max_exogs_out": int(cfg.max_exogs_out),
        },
        "selection_context": {
            "metric_id": cfg.metric_id,
            "geo_id": cfg.geo_id,
            "property_type_id": cfg.property_type_id,
            "data_asof": cfg.data_asof,
            "anchor_date": cfg.anchor_date,
            "horizon": int(cfg.horizon),
            "min_train_len": int(cfg.min_train_len),
        },
        "thresholds": {
            "min_obs_per_param": float(cfg.min_obs_per_param),
            "max_exog_cond": float(cfg.max_exog_cond),
            "require_full_rank": bool(cfg.require_full_rank),
            "svd_rtol": float(cfg.svd_rtol),
        },
        "accepted_feature_ids": accepted,
        "rejected": rejected[:200],  # cap for sanity; expand later if needed
        "rejected_n": int(len(rejected)),
    }

    out_audit = _resolve_out_audit(cfg, n=len(accepted))
    out_audit.write_text(json.dumps(audit, indent=2, sort_keys=True))

    return out_csv, out_audit
