# forecast/model_select_single.py

import argparse
import os
import subprocess
from dataclasses import dataclass
from typing import List, Tuple, Dict

import duckdb
import pandas as pd


BACKTEST_MODELS = ("sarimax_backtest", "sarimax_exog_backtest", "xgb_backtest")

# Map backtest family -> live module runner
PROMOTION_CMD = {
    "sarimax_backtest":     ["python", "-m", "forecast.sarimax_redfin"],
    "sarimax_exog_backtest":["python", "-m", "forecast.sarimax_exog"],
    "xgb_backtest":         ["python", "-m", "forecast.xgb_regressor"],
}


@dataclass
class Target:
    metric_id: str
    geo_id: str
    property_type_id: str


def connect() -> duckdb.DuckDBPyConnection:
    db_path = os.getenv("DUCKDB_PATH", "data/market.duckdb")
    return duckdb.connect(db_path)


def latest_batch_eval(
    con: duckdb.DuckDBPyConnection,
    target: Target,
    batch_hours: int,
) -> pd.DataFrame:
    """
    Returns a scoreboard dataframe for ONLY the latest batch per model_name.
    "Latest batch" is defined per model_name as runs with created_at >= (max_created_at - batch_hours).
    """

    q = f"""
    WITH candidate_runs AS (
      SELECT run_id, model_name, created_at
      FROM forecast_runs
      WHERE target_metric_id = ?
        AND target_geo_id = ?
        AND target_property_type_id = ?
        AND model_name IN {BACKTEST_MODELS}
    ),
    latest_per_model AS (
      SELECT model_name, MAX(created_at) AS max_created_at
      FROM candidate_runs
      GROUP BY model_name
    ),
    latest_batch_runs AS (
      SELECT r.run_id, r.model_name, r.created_at
      FROM candidate_runs r
      JOIN latest_per_model m USING (model_name)
      WHERE r.created_at >= m.max_created_at - INTERVAL '{batch_hours} hours'
    ),
    eval AS (
      SELECT r.model_name, r.created_at, e.*
      FROM latest_batch_runs r
      JOIN v_forecast_eval e USING (run_id)
    )
    SELECT
      model_name,
      COUNT(*) AS n_runs,
      AVG(mape_1m)  AS mape_1m_avg,
      AVG(mape_3m)  AS mape_3m_avg,
      AVG(mape_6m)  AS mape_6m_avg,
      AVG(mape_12m) AS mape_12m_avg,

      AVG(mae_1m)   AS mae_1m_avg,
      AVG(mae_3m)   AS mae_3m_avg,
      AVG(mae_6m)   AS mae_6m_avg,
      AVG(mae_12m)  AS mae_12m_avg,

      AVG(rmse_1m)  AS rmse_1m_avg,
      AVG(rmse_3m)  AS rmse_3m_avg,
      AVG(rmse_6m)  AS rmse_6m_avg,
      AVG(rmse_12m) AS rmse_12m_avg,

      MIN(created_at) AS batch_start,
      MAX(created_at) AS batch_end
    FROM eval
    GROUP BY model_name
    ORDER BY model_name;
    """

    df = con.execute(q, [target.metric_id, target.geo_id, target.property_type_id]).fetchdf()
    return df


def compute_weighted_score(
    df: pd.DataFrame,
    metric: str,
    horizons: List[int],
    weights: List[float],
) -> pd.DataFrame:
    """
    Adds `score` column to df using selected metric+ horizons.
    metric: "rmse" or "mae" or "mape"
    horizons: e.g. [3,6,12]
    weights:  same length, sums to 1 ideally
    """
    if len(horizons) != len(weights):
        raise ValueError("horizons and weights must be same length")

    metric = metric.lower()
    allowed = {"rmse", "mae", "mape"}
    if metric not in allowed:
        raise ValueError(f"metric must be one of {sorted(allowed)}")

    score = 0.0
    for h, w in zip(horizons, weights):
        col = f"{metric}_{h}m_avg"
        if col not in df.columns:
            raise ValueError(f"Missing required column in scoreboard: {col}")
        score = score + w * df[col]

    out = df.copy()
    out["score_metric"] = metric
    out["score_horizons"] = ",".join(map(str, horizons))
    out["score_weights"] = ",".join(map(str, weights))
    out["score"] = score
    out = out.sort_values("score", ascending=True).reset_index(drop=True)
    return out


def deactivate_live_runs(con: duckdb.DuckDBPyConnection, target: Target) -> int:
    """
    Marks all existing active (live) runs for the target as inactive.
    """
    q = """
    UPDATE forecast_runs
    SET is_active = FALSE
    WHERE target_metric_id = ?
      AND target_geo_id = ?
      AND target_property_type_id = ?
      AND is_active = TRUE
      AND model_name NOT LIKE '%backtest%';
    """
    con.execute(q, [target.metric_id, target.geo_id, target.property_type_id])
    # DuckDB doesn't reliably return affected rows in python API; just re-count:
    cnt = con.execute(
        """
        SELECT COUNT(*) FROM forecast_runs
        WHERE target_metric_id=? AND target_geo_id=? AND target_property_type_id=?
          AND is_active=TRUE AND model_name NOT LIKE '%backtest%';
        """,
        [target.metric_id, target.geo_id, target.property_type_id],
    ).fetchone()[0]
    return int(cnt)


def promote_winner(winner_model_name: str, target: Target, horizon: int) -> None:
    """
    Runs the appropriate live forecaster CLI for the winning family.
    """
    if winner_model_name not in PROMOTION_CMD:
        raise ValueError(f"No promotion command mapping for winner model: {winner_model_name}")

    cmd = PROMOTION_CMD[winner_model_name] + [
        "--metric_id", target.metric_id,
        "--geo_id", target.geo_id,
        "--property_type_id", target.property_type_id,
        "--horizon", str(horizon),
    ]

    print(f"[select] Promoting winner by running:\n  {' '.join(cmd)}")
    subprocess.check_call(cmd)


def main():
    ap = argparse.ArgumentParser(description="Select best model family for a single target from latest backtest batch.")
    ap.add_argument("--metric_id", required=True)
    ap.add_argument("--geo_id", required=True)
    ap.add_argument("--property_type_id", required=True)
    ap.add_argument("--metric", default="rmse", choices=["rmse", "mae", "mape"])
    ap.add_argument("--horizons", default="3,6,12", help="Comma-separated horizons, e.g. 1,3,6,12")
    ap.add_argument("--weights", default="0.2,0.5,0.3", help="Comma-separated weights matching horizons")
    ap.add_argument("--batch_hours", type=int, default=6, help="How far back from latest created_at counts as same batch (per model).")
    ap.add_argument("--promote", action="store_true", help="Deactivate existing live models for this target and run the winning live forecaster.")
    ap.add_argument("--live_horizon", type=int, default=12, help="Horizon for the promoted live forecast.")

    args = ap.parse_args()

    target = Target(args.metric_id, args.geo_id, args.property_type_id)
    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]
    weights = [float(x) for x in args.weights.split(",") if x.strip()]

    s = sum(weights)
    if s <= 0:
        raise SystemExit("[select] Weights must sum to a positive number.")
    weights = [w / s for w in weights]

    allowed = {1, 3, 6, 12}
    bad = [h for h in horizons if h not in allowed]
    if bad:
        raise SystemExit(
            f"[select] Horizons not supported yet: {bad}. "
            f"Available horizons in v_forecast_eval: {sorted(allowed)}"
        )
        
    con = connect()

    df = latest_batch_eval(con, target, batch_hours=args.batch_hours)
    if df.empty:
        raise SystemExit("[select] No backtest runs found for this target in the DB.")

    scored = compute_weighted_score(df, metric=args.metric, horizons=horizons, weights=weights)

    print("\n[select] Latest-batch scoreboard (per model family):")
    show_cols = [
        "model_name", "n_runs",
        "rmse_3m_avg", "rmse_6m_avg", "rmse_12m_avg",
        "mae_3m_avg", "mae_6m_avg", "mae_12m_avg",
        "batch_start", "batch_end",
        "score_metric", "score_horizons", "score_weights", "score",
    ]
    keep = [c for c in show_cols if c in scored.columns]
    print(scored[keep].to_string(index=False))

    winner = scored.iloc[0]["model_name"]
    print(f"\n[select] WINNER = {winner} (score={scored.iloc[0]['score']:.4f} using {args.metric} @ horizons={horizons})")

    if args.promote:
        # flip existing live runs off, then run live forecast for winner
        print("[select] Deactivating existing live runs for this target...")
        deactivate_live_runs(con, target)
        promote_winner(winner, target, horizon=args.live_horizon)
        print("[select] Promotion complete.")

    con.close()


if __name__ == "__main__":
    main()
