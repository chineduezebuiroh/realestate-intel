from __future__ import annotations
# forecast/models/sarimax_exog/live_runner.py

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from forecast.db_forecast import get_connection, insert_predictions, insert_run
from forecast.models.sarimax_exog.core import SarimaxExogSpec, fit_sarimax_exog, forecast_sarimax_exog
from forecast.models.sarimax_exog.exog_future import build_exog_future, write_exog_future_artifact


def _parse_asof_from_name(name: str) -> pd.Timestamp:
    # expects ...__asof=YYYY-MM-DD.json
    part = name.split("__asof=", 1)[1].replace(".json", "")
    return pd.Timestamp(part)


def _find_latest_design_matrix_artifact(
    *,
    runs_root: str,
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    prefer_batch_id: Optional[str] = None,
) -> Tuple[str, str, Dict[str, Any]]:
    """
    Find latest SARIMAX-exog design matrix artifact for a specific target.
    Selection:
      - filter by audit.target.{metric_id,geo_id,property_type_id}
      - sort by (data_asof_effective desc, anchor_date desc, mtime desc)
    """
    root = Path(runs_root)
    if not root.exists():
        raise FileNotFoundError(f"[sarimax_exog_live] runs root not found: {runs_root}")

    root = Path(runs_root)
    if prefer_batch_id:
        root = root / prefer_batch_id / "sarimax_exog"

    candidates = [
        p for p in root.rglob("*.json")
        if p.name.startswith("design_matrix__anchor=") and "__asof=" in p.name
    ]
    if not candidates:
        raise FileNotFoundError("[sarimax_exog_live] no design matrix audit jsons found under runs/")

    matched: list[tuple[pd.Timestamp, pd.Timestamp, float, Path, dict]] = []

    for audit_path in candidates:
        try:
            audit = json.loads(audit_path.read_text())
        except Exception:
            continue

        tgt = audit.get("target") or {}
        if str(tgt.get("metric_id")) != str(metric_id):
            continue
        if str(tgt.get("geo_id")) != str(geo_id):
            continue
        if str(tgt.get("property_type_id")) != str(property_type_id):
            continue

        asof = audit.get("data_asof_effective")
        anchor = audit.get("anchor_date")
        if not asof or not anchor:
            continue

        try:
            asof_ts = pd.Timestamp(asof)
            anchor_ts = pd.Timestamp(anchor)
        except Exception:
            continue

        matched.append((asof_ts, anchor_ts, audit_path.stat().st_mtime, audit_path, audit))

    if not matched:
        raise FileNotFoundError(
            "[sarimax_exog_live] no matching design matrix audits found for target "
            f"metric={metric_id} geo={geo_id} pt={property_type_id}"
        )

    matched.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    _, _, _, audit_path, audit = matched[0]
    parquet_path = audit_path.with_suffix(".parquet")
    if not parquet_path.exists():
        raise FileNotFoundError(f"[sarimax_exog_live] missing parquet for audit: {audit_path}")

    return str(parquet_path), str(audit_path), audit


def run_live_latest_artifact(
    *,
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    freq: str = "M",
    horizon: int = 12,
    batch_id: str,
    runs_root: str = "runs",
    artifact_root: str = "runs",
    is_active: bool = True,
    prefer_batch_id: Optional[str] = None,
) -> int:
    """
    Live SARIMAX(exog) run:
      1) find latest design_matrix artifact (+ audit)
      2) validate target identity vs args
      3) slice train rows up to anchor
      4) generate X_future via seasonal-naive per exog feature
      5) write X_future artifact (+ audit)
      6) fit + forecast
      7) persist run + predictions to DB

    This is NOT the bridge runner. Bridge is backtest/validation-only.
    """
    design_parquet_path, design_audit_path, audit = _find_latest_design_matrix_artifact(
        runs_root=runs_root,
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=str(property_type_id),
        prefer_batch_id=prefer_batch_id,
    )

    # --- audit contract ---
    feature_ids = audit.get("feature_ids")
    if not feature_ids:
        raise ValueError("[sarimax_exog_live] design matrix audit missing feature_ids")

    anchor_date = audit.get("anchor_date")
    if not anchor_date:
        raise ValueError("[sarimax_exog_live] design matrix audit missing anchor_date")

    data_asof_effective = audit.get("data_asof_effective")
    if not data_asof_effective:
        raise ValueError("[sarimax_exog_live] design matrix audit missing data_asof_effective")

    # --- enforce target identity (avoid accidental cross-target live runs) ---
    tgt = (audit.get("target") or {})
    audit_metric = str(tgt.get("metric_id", ""))
    audit_geo = str(tgt.get("geo_id", ""))
    audit_pt = str(tgt.get("property_type_id", ""))

    if audit_metric and audit_metric != metric_id:
        raise ValueError(f"[sarimax_exog_live] metric_id mismatch: args={metric_id} audit={audit_metric}")
    if audit_geo and audit_geo != geo_id:
        raise ValueError(f"[sarimax_exog_live] geo_id mismatch: args={geo_id} audit={audit_geo}")
    if audit_pt and audit_pt != str(property_type_id):
        raise ValueError(f"[sarimax_exog_live] property_type_id mismatch: args={property_type_id} audit={audit_pt}")

    # --- load design matrix ---
    df = pd.read_parquet(design_parquet_path)
    if "y" not in df.columns:
        raise ValueError("[sarimax_exog_live] design matrix parquet missing required 'y' column")

    y_full = df["y"].astype(float)
    X_full = df.drop(columns=["y"])

    # enforce exog order
    cols = list(map(str, X_full.columns))
    if cols != list(map(str, feature_ids)):
        raise ValueError("[sarimax_exog_live] exog column order mismatch vs audit.feature_ids")

    # anchor timestamp must exist
    anchor_ts = pd.Timestamp(anchor_date).to_period("M").to_timestamp(how="end")
    if anchor_ts not in y_full.index or anchor_ts not in X_full.index:
        raise ValueError(f"[sarimax_exog_live] anchor_ts not present in design matrix index: {anchor_ts}")

    # slice train
    y_train = y_full.loc[:anchor_ts]
    X_train = X_full.loc[:anchor_ts]

    if len(y_train) < 24:
        raise ValueError(f"[sarimax_exog_live] insufficient training rows: n={len(y_train)}")

    # --- generate future exog (policy: seasonal naive) ---
    X_future = build_exog_future(
        X_hist=X_train,
        anchor_ts=anchor_ts,
        horizon=horizon,
        feature_ids=list(map(str, feature_ids)),
        season=12,
    )

    # --- write future exog artifact ---
    out_dir = Path(artifact_root) / batch_id / "sarimax_exog"
    exog_parquet_path, exog_audit_path = write_exog_future_artifact(
        out_dir=out_dir,
        anchor_date=str(anchor_ts.date()),
        data_asof_effective=str(data_asof_effective),
        horizon=int(horizon),
        feature_ids=list(map(str, feature_ids)),
        X_future=X_future,
        design_matrix_audit=audit,
        overwrite=False,
    )
    exog_audit = json.loads(Path(exog_audit_path).read_text())
    print(f"[sarimax_exog_live] using design_matrix={design_parquet_path}")
    print(f"[sarimax_exog_live] wrote exog_future={exog_parquet_path}")

    # --- fit + forecast ---
    spec = SarimaxExogSpec()
    res = fit_sarimax_exog(y_train=y_train, X_train=X_train, spec=spec)
    mean_fc, ci = forecast_sarimax_exog(res=res, X_future=X_future, steps=int(horizon))

    target_dates = [pd.Timestamp(d).date() for d in X_future.index]

    # --- persist ---
    algo_params = {
        "model_version": "v0_live_exog_future",
        "feature_ids": list(map(str, feature_ids)),
        "design_matrix_artifact": design_parquet_path,
        "design_matrix_audit": design_audit_path,
        "design_matrix_sha256": audit.get("design_matrix_sha256"),
        "feature_set_sha256": audit.get("feature_set_sha256"),
        "exog_future_artifact": str(exog_parquet_path),
        "exog_future_audit": str(exog_audit_path),
        "exog_future_sha256": exog_audit.get("exog_future_sha256"),
        "anchor_date": str(anchor_ts.date()),
        "data_asof_effective": str(data_asof_effective),
        "fit_diag": {
            "aic": getattr(res, "aic", None),
            "bic": getattr(res, "bic", None),
        },
        "contracts": {
            "run_kind": "live",
            "anchor_date": str(anchor_ts.date()),
            "data_asof_effective": str(data_asof_effective),
            "target_metric_id": metric_id,
            "target_geo_id": geo_id,
            "target_property_type_id": str(property_type_id),
            "freq": freq,
            "train_start": str(pd.Timestamp(y_train.index[0]).date()),
            "train_end": str(anchor_ts.date()),
            "horizon_max_months": int(horizon),
        },
    }

    con = get_connection()
    run_id = insert_run(
        con=con,
        model_name="sarimax_exog",
        model_version="v0_live_exog_future",
        target_metric_id=metric_id,
        target_geo_id=geo_id,
        target_property_type_id=str(property_type_id),
        freq=freq,
        train_start=pd.Timestamp(y_train.index[0]).date(),
        train_end=anchor_ts.date(),
        horizon_max_months=int(horizon),
        algo_params=algo_params,
        notes=f"SARIMAX(exog) live run anchor={anchor_ts.date()}",
        is_active=bool(is_active),
        run_kind="live",
        batch_id=batch_id,
        data_asof=pd.to_datetime(str(data_asof_effective)).date(),
    )

    insert_predictions(
        con=con,
        run_id=run_id,
        target_dates=target_dates,
        y_hat=mean_fc,
        y_hat_lo=ci[:, 0] if ci is not None else None,
        y_hat_hi=ci[:, 1] if ci is not None else None,
    )
    con.close()

    return int(run_id)
