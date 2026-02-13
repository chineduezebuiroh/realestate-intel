from __future__ import annotations
# forecast/runners/live_univariate.py

import json
import hashlib
from dataclasses import asdict, is_dataclass
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import pandas as pd

from forecast.core.db_forecast import get_connection
from forecast.core.backtest_utils import month_end_index

# Adjust these imports to match your repo’s univariate core
from forecast.models.sarimax_univariate.core import (
    SarimaxUnivariateSpec,
    fit_sarimax_univariate,
    forecast_sarimax_univariate,
)


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _load_y_from_fact(metric_id: str, geo_id: str, property_type_id: str, data_asof: str) -> pd.Series:
    con = get_connection()
    df = con.execute(
        """
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
          AND CAST(date AS DATE) <= CAST(? AS DATE)
        ORDER BY date
        """,
        [metric_id, geo_id, property_type_id, data_asof],
    ).fetchdf()
    con.close()

    if df.empty:
        raise ValueError("no y data found for target")

    s = pd.Series(df["value"].astype(float).to_numpy(), index=pd.to_datetime(df["date"]))
    s.index = month_end_index(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def run_live_univariate(
    *,
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    freq: str,
    horizon: int,
    data_asof: str,
    batch_id: str,
    artifact_root: str,
    model_version: str,
    spec: SarimaxUnivariateSpec,
) -> Dict[str, Any]:
    """
    Runs a live univariate forecast and writes:
      - artifacts/phasec/live/<batch_id>/sarimax_univariate/<metric_id>/predictions.parquet
      - .../audit.json

    Also writes forecast_runs + forecast_predictions rows.
    """

    if freq != "M":
        raise ValueError(f"only freq=M supported for now, got {freq}")

    y = _load_y_from_fact(metric_id, geo_id, property_type_id, data_asof)
    if len(y) < 24:
        raise ValueError(f"refusing to fit: too few observations n={len(y)}")

    train_start = pd.to_datetime(y.index[0]).date()
    train_end = pd.to_datetime(y.index[-1]).date()

    res = fit_sarimax_univariate(y_train=y, spec=spec)
    mean_fc, ci = forecast_sarimax_univariate(res=res, steps=int(horizon))

    anchor_ts = pd.Timestamp(train_end).to_period("M").to_timestamp(how="end")
    target_idx = pd.date_range(anchor_ts, periods=int(horizon) + 1, freq="M")[1:]
    target_dates = [pd.to_datetime(d).date() for d in target_idx]

    pred_df = pd.DataFrame(
        {
            "target_date": pd.to_datetime(target_dates),
            "y_hat": pd.Series(mean_fc).astype(float).to_numpy(),
        }
    )
    if ci is not None and getattr(ci, "shape", None) is not None and ci.shape[1] >= 2:
        pred_df["y_hat_lo"] = ci[:, 0].astype(float)
        pred_df["y_hat_hi"] = ci[:, 1].astype(float)
    else:
        pred_df["y_hat_lo"] = None
        pred_df["y_hat_hi"] = None

    out_dir = Path(artifact_root) / "live" / batch_id / "sarimax_univariate" / metric_id
    out_dir.mkdir(parents=True, exist_ok=True)

    preds_path = out_dir / "predictions.parquet"
    audit_path = out_dir / "audit.json"
    if preds_path.exists() or audit_path.exists():
        raise ValueError(f"refusing to overwrite existing artifacts in {out_dir}")

    pred_df.to_parquet(preds_path, index=False)

    # --- DB writes ---
    algo_params = {
        "model_version": model_version,
        "spec": asdict(spec) if is_dataclass(spec) else spec,
        "live_contract": {
            "metric_id": metric_id,
            "geo_id": geo_id,
            "property_type_id": property_type_id,
            "freq": freq,
            "horizon": int(horizon),
            "data_asof": data_asof,
        },
        "artifacts": {"predictions": str(preds_path)},
        "sha256": {"predictions": _sha256_file(preds_path)},
    }

    con = get_connection()
    run_id = con.execute(
        """
        INSERT INTO forecast_runs (
            model_name, model_version, run_kind, batch_id, data_asof,
            target_metric_id, target_geo_id, target_property_type_id, freq,
            train_start, train_end, horizon_max_months, algo_params_json
        )
        VALUES (?, ?, ?, ?, CAST(? AS DATE),
                ?, ?, ?, ?,
                CAST(? AS DATE), CAST(? AS DATE), ?, ?)
        RETURNING run_id
        """,
        [
            "sarimax_univariate",
            model_version,
            "live",
            batch_id,
            data_asof,
            metric_id,
            geo_id,
            property_type_id,
            freq,
            str(train_start),
            str(train_end),
            int(horizon),
            json.dumps(algo_params),
        ],
    ).fetchone()[0]

    rows = []
    for _, r in pred_df.iterrows():
        rows.append(
            (
                int(run_id),
                pd.to_datetime(r["target_date"]).date(),
                float(r["y_hat"]),
                None if pd.isna(r["y_hat_lo"]) else float(r["y_hat_lo"]),
                None if pd.isna(r["y_hat_hi"]) else float(r["y_hat_hi"]),
            )
        )

    con.executemany(
        """
        INSERT INTO forecast_predictions (run_id, target_date, y_hat, y_hat_lo, y_hat_hi)
        VALUES (?, CAST(? AS DATE), ?, ?, ?)
        """,
        rows,
    )
    con.close()

    audit = {
        "batch_id": batch_id,
        "run_id": int(run_id),
        "model_name": "sarimax_univariate",
        "run_kind": "live",
        "train_start": str(train_start),
        "train_end": str(train_end),
        "artifacts": {"predictions": str(preds_path), "audit": str(audit_path)},
        "sha256": {"predictions": _sha256_file(preds_path)},
        "algo_params": algo_params,
    }
    audit_path.write_text(json.dumps(audit, indent=2))

    return audit
