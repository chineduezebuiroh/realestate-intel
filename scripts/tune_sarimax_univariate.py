from __future__ import annotations
# scripts/tune_sarimax_univariate.py

import warnings
import itertools
import duckdb

import numpy as np
import pandas as pd

from dataclasses import dataclass
from typing import Iterable, Optional

from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tools.sm_exceptions import ConvergenceWarning


@dataclass(frozen=True)
class TuneConfig:
    db_path: str = "./data/market.duckdb"
    metric_id: str = "median_ppsf"
    geo_id: str = "dc_city"
    property_type_id: str = "6"

    anchors_csv: str = "2020-12-31,2021-12-31,2022-12-31,2023-12-31,2024-06-30"
    horizon: int = 18
    min_train_len: int = 87

    # Keep this small. Bigger grid = wasted time.
    p_values: tuple[int, ...] = (0, 1, 2)
    d_values: tuple[int, ...] = (0, 1)
    q_values: tuple[int, ...] = (0, 1, 2)

    P_values: tuple[int, ...] = (0, 1)
    D_values: tuple[int, ...] = (0, 1)
    Q_values: tuple[int, ...] = (0, 1)

    seasonal_period: int = 12
    trend_grid: tuple[Optional[str], ...] = (None, "c")  # keep small; add "t","ct" only if you mean it

    enforce_stationarity: bool = False
    enforce_invertibility: bool = False
    maxiter: int = 250

    # scoring horizon slice (use first 12 months for apples-to-apples)
    score_first_n: int = 18


def _parse_anchors(anchors_csv: str) -> list[pd.Timestamp]:
    anchors = []
    for s in anchors_csv.split(","):
        s = s.strip()
        if not s:
            continue
        anchors.append(pd.Timestamp(s).to_period("M").to_timestamp("M"))
    return anchors


def load_series(cfg: TuneConfig) -> pd.Series:
    con = duckdb.connect(cfg.db_path)
    df = con.execute(
        """
        select date, value
        from fact_timeseries
        where metric_id = ?
          and geo_id = ?
          and property_type_id = ?
        order by date
        """,
        [cfg.metric_id, cfg.geo_id, cfg.property_type_id],
    ).df()
    con.close()

    if df.empty:
        raise ValueError("No rows returned from fact_timeseries for the given metric/geo/pt.")

    y = pd.Series(df["value"].astype(float).to_numpy(), index=pd.to_datetime(df["date"]))
    # force month-end timestamps
    y.index = y.index.to_period("M").to_timestamp("M")
    y = y.sort_index()

    return y


def forecast_dates(anchor: pd.Timestamp, h: int) -> list[pd.Timestamp]:
    # next month-end dates after anchor
    start = (anchor.to_period("M") + 1).to_timestamp("M")
    return [ (start.to_period("M") + i).to_timestamp("M") for i in range(h) ]


def score_forecast(y_true: np.ndarray, y_hat: np.ndarray) -> dict:
    e = y_hat - y_true
    mae = float(np.mean(np.abs(e)))
    rmse = float(np.sqrt(np.mean(e ** 2)))
    mape = float(np.mean(np.abs(e) / np.maximum(1e-9, np.abs(y_true))) * 100.0)
    return {"mae": mae, "rmse": rmse, "mape_pct": mape}


def iter_specs(cfg: TuneConfig) -> Iterable[
    tuple[tuple[int, int, int], tuple[int, int, int, int], Optional[str]]
]:
    for p, d, q in itertools.product(cfg.p_values, cfg.d_values, cfg.q_values):
        for P, D, Q in itertools.product(cfg.P_values, cfg.D_values, cfg.Q_values):
            for trend in cfg.trend_grid:
                order = (p, d, q)
                seasonal_order = (P, D, Q, cfg.seasonal_period)
                yield order, seasonal_order, trend


def fit_and_forecast(
    y_train: pd.Series,
    steps: int,
    order: tuple[int, int, int],
    seasonal_order: tuple[int, int, int, int],
    trend: Optional[str],
    enforce_stationarity: bool,
    enforce_invertibility: bool,
    maxiter: int,
) -> tuple[np.ndarray, dict]:
    """
    Returns:
      y_hat: np.ndarray of length `steps`
      fit_diag: dict with convergence/fit diagnostics
    """
    model = SARIMAX(
        endog=y_train.astype(float).to_numpy(),
        order=order,
        seasonal_order=seasonal_order,
        trend=trend,
        enforce_stationarity=enforce_stationarity,
        enforce_invertibility=enforce_invertibility,
    )

    saw_convergence_warning = False
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        res = model.fit(disp=False, maxiter=int(maxiter))
        saw_convergence_warning = any(
            issubclass(getattr(x, "category", Warning), ConvergenceWarning) for x in w
        )

    mle_retvals = getattr(res, "mle_retvals", None) or {}
    if isinstance(mle_retvals, dict):
        fit_converged = bool(mle_retvals.get("converged")) if "converged" in mle_retvals else None
        warnflag = mle_retvals.get("warnflag", None)
        iterations = mle_retvals.get("iterations", None)
        fopt = mle_retvals.get("fopt", None)
    else:
        fit_converged = None
        warnflag = None
        iterations = None
        fopt = None

    fc = res.get_forecast(steps=int(steps))
    mean = np.asarray(fc.predicted_mean, dtype=float)

    fit_diag = {
        "fit_converged": fit_converged,
        "warnflag": warnflag,
        "iterations": iterations,
        "fopt": fopt,
        "aic": float(getattr(res, "aic", np.nan)),
        "bic": float(getattr(res, "bic", np.nan)),
        "saw_convergence_warning": bool(saw_convergence_warning),
        "n_obs_train": int(len(y_train)),
    }
    return mean, fit_diag


if __name__ == "__main__":
    # --- config via env vars / quick edits ---
    # Edit these directly if you prefer.
    cfg = TuneConfig(
        db_path="./data/market.duckdb",
        metric_id="median_ppsf",
        geo_id="dc_city",
        property_type_id="6",
        anchors_csv="2020-12-31,2021-12-31,2022-12-31,2023-12-31,2024-06-30",
        horizon=18,
        min_train_len=87,
        trend_grid=(None, "c"),
    )

    y = load_series(cfg)
    anchors = _parse_anchors(cfg.anchors_csv)

    rows = []
    
    for order, seasonal_order, trend in iter_specs(cfg):
        print(f"[tune] trying order={order} seasonal_order={seasonal_order} trend={trend}", flush=True)
        spec_key = f"order={order} seas={seasonal_order} trend={trend}"
        per_anchor = []
        ok = True

        for anchor in anchors:
            if anchor not in y.index:
                per_anchor.append({"anchor": str(anchor.date()), "status": "missing_anchor"})
                ok = False
                continue

            y_train = y.loc[:anchor].dropna()
            if len(y_train) < cfg.min_train_len:
                per_anchor.append({"anchor": str(anchor.date()), "status": f"short_train n={len(y_train)}"})
                ok = False
                continue

            fdates = forecast_dates(anchor, cfg.horizon)
            y_future = y.reindex(fdates).astype(float)

            if y_future.isna().any():
                per_anchor.append({"anchor": str(anchor.date()), "status": "missing_future"})
                ok = False
                continue


            try:
                print(f"[tune]  anchor={anchor.date()} fitting...", flush=True)
                y_hat, fit_diag = fit_and_forecast(
                    y_train=y_train,
                    steps=cfg.horizon,
                    order=order,
                    seasonal_order=seasonal_order,
                    trend=trend,  # IMPORTANT: use the per-spec trend value
                    enforce_stationarity=cfg.enforce_stationarity,
                    enforce_invertibility=cfg.enforce_invertibility,
                    maxiter=cfg.maxiter,
                )
            except Exception as e:
                per_anchor.append({"anchor": str(anchor.date()), "status": f"fit_fail {type(e).__name__}: {e}"})
                ok = False
                continue
            
            # ---- CONVERGENCE GATE (MANDATORY) ----
            bad_convergence = (
                fit_diag.get("fit_converged") is not True
                or fit_diag.get("warnflag") not in (None, 0)
                or fit_diag.get("saw_convergence_warning") is True
            )
            if bad_convergence:
                per_anchor.append(
                    {
                        "anchor": str(anchor.date()),
                        "status": "not_converged",
                        "fit_diag": fit_diag,
                    }
                )
                ok = False
                continue


            # score first N months for apples-to-apples
            n = min(cfg.score_first_n, cfg.horizon)
            s = score_forecast(y_true=y_future.to_numpy()[:n], y_hat=y_hat[:n])
            per_anchor.append({"anchor": str(anchor.date()), "status": "ok", **s})

        # aggregate only ok anchors
        oks = [r for r in per_anchor if r.get("status") == "ok"]
        n_ok = len(oks)
        n_total = len(anchors)
        
        # STRICT: require success on every anchor
        if n_ok != n_total:
            continue

        avg_rmse = float(np.mean([r["rmse"] for r in oks]))
        avg_mae = float(np.mean([r["mae"] for r in oks]))
        avg_mape = float(np.mean([r["mape_pct"] for r in oks]))
        rows.append(
            {
                "spec": spec_key,
                "order": str(order),
                "seasonal_order": str(seasonal_order),
                "trend": str(trend),
                "n_anchors_ok": len(oks),
                "avg_rmse": avg_rmse,
                "avg_mae": avg_mae,
                "avg_mape_pct": avg_mape,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        raise SystemExit("No specs produced any valid scores. Check anchors/min_train_len/data coverage.")

    out = out.sort_values(["avg_rmse", "avg_mae", "avg_mape_pct"], ascending=True)
    print("\n=== TOP 15 SPECS BY avg_rmse ===")
    print(out.head(15).to_string(index=False))

    # Optional: write results
    out_path = (
        "artifacts/phasec/eval/"
        f"sarimax_univariate_tuning__metric={cfg.metric_id}__geo={cfg.geo_id}__pt={cfg.property_type_id}"
        f"__h={cfg.horizon}__scoreN={cfg.score_first_n}__nanchors={len(anchors)}.csv"
    )
    try:
        pd.Series([out_path]).to_csv  # no-op to silence linters
        out.to_csv(out_path, index=False)
        print(f"\nWROTE {out_path}")
    except Exception as e:
        print(f"\nNOTE: could not write CSV ({type(e).__name__}: {e})")
