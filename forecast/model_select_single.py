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


def latest_batch_eval_long(
    con: duckdb.DuckDBPyConnection,
    target: Target,
) -> pd.DataFrame:
    """
    Returns long-form eval rows for ONLY the latest batch per model_name:
      model_name, horizon_months, mae_avg, rmse_avg, mape_avg, n_runs, batch_start, batch_end
    """

    models_sql = "(" + ",".join([f"'{m}'" for m in BACKTEST_MODELS]) + ")"

    q = f"""
    WITH candidate_runs AS (
      SELECT *
      FROM forecast_runs
      WHERE target_metric_id = ?
        AND target_geo_id = ?
        AND target_property_type_id = ?
        AND run_kind = 'backtest'
        AND batch_id IS NOT NULL
        AND data_asof IS NOT NULL
        AND model_name IN {models_sql}
    ),
    
    latest_common_data_asof AS (
      -- choose the most recent data_asof that exists for ALL model families
      SELECT data_asof
      FROM candidate_runs
      GROUP BY data_asof
      HAVING COUNT(DISTINCT model_name) = {len(BACKTEST_MODELS)}
      ORDER BY data_asof DESC
      LIMIT 1
    ),
    
    runs_same_asof AS (
      SELECT *
      FROM candidate_runs
      WHERE data_asof = (SELECT data_asof FROM latest_common_data_asof)
    ),
    
    latest_batch_per_model AS (
      SELECT
        model_name,
        arg_max(batch_id, created_at) AS batch_id
      FROM runs_same_asof
      GROUP BY model_name
    ),
    
    batch_runs AS (
      SELECT r.run_id, r.model_name, r.created_at, r.batch_id, r.data_asof
      FROM runs_same_asof r
      JOIN latest_batch_per_model lb
        ON r.model_name = lb.model_name AND r.batch_id = lb.batch_id
    ),
    
    eval AS (
      SELECT
        r.model_name,
        r.created_at,
        r.batch_id,
        e.run_id,
        e.horizon_months,
        e.mae,
        e.rmse,
        e.mape
      FROM batch_runs r
      JOIN v_forecast_eval_long e USING (run_id)
    )
    
    SELECT
      model_name,
      batch_id,
      horizon_months,
      COUNT(DISTINCT run_id) AS n_runs,
      AVG(mae)  AS mae_avg,
      AVG(rmse) AS rmse_avg,
      AVG(mape) AS mape_avg,
      MIN(created_at) AS batch_start,
      MAX(created_at) AS batch_end
    FROM eval
    GROUP BY model_name, batch_id, horizon_months
    ORDER BY model_name, horizon_months;
    """

    return con.execute(q, [target.metric_id, target.geo_id, target.property_type_id]).fetchdf()


def compute_weighted_score_long(
    df_long: pd.DataFrame,
    metric: str,
    horizons: List[int],
    weights: List[float],
    strict: bool = True,
) -> pd.DataFrame:
    """
    df_long columns expected:
      model_name, horizon_months, <metric>_avg, n_runs, batch_start, batch_end

    Returns one row per model_name with a weighted score across requested horizons.
    If strict=True, model must have ALL requested horizons or it is excluded.
    """
    metric = metric.lower()
    allowed = {"rmse", "mae", "mape"}
    if metric not in allowed:
        raise ValueError(f"metric must be one of {sorted(allowed)}")

    if len(horizons) != len(weights):
        raise ValueError("horizons and weights must be same length")

    # normalize weights (so caller can pass 2,5,3)
    s = sum(weights)
    if s <= 0:
        raise ValueError("weights must sum to > 0")
    weights = [w / s for w in weights]

    value_col = f"{metric}_avg"

    out_rows = []
    for model_name, g in df_long.groupby("model_name"):
        g2 = g.set_index("horizon_months")

        missing = [h for h in horizons if h not in g2.index]
        if missing and strict:
            continue
        
        # Only score on horizons that exist for this model
        used_pairs = [(h, w) for h, w in zip(horizons, weights) if h in g2.index]
        if not used_pairs:
            continue
        
        # Renormalize weights over the used horizons (strict=False behavior)
        wsum = sum(w for _, w in used_pairs)
        if wsum <= 0:
            continue
        
        score = 0.0
        for h, w in used_pairs:
            score += (w / wsum) * float(g2.loc[h, value_col])

        out_rows.append({
            "model_name": model_name,
            "batch_id": str(g["batch_id"].iloc[0]) if "batch_id" in g.columns else "",
            "n_runs": int(g["n_runs"].max()),  # per horizon it's same; take max
            "batch_start": g["batch_start"].min(),
            "batch_end": g["batch_end"].max(),
            "score_metric": metric,
            "score_horizons": ",".join(map(str, horizons)),
            "score_weights": ",".join(map(lambda x: f"{x:.4g}", weights)),
            "score": float(score),
            "missing_horizons": ",".join(map(str, missing)) if missing else "",
        })

    scored = pd.DataFrame(out_rows)
    if scored.empty:
        return scored
    return scored.sort_values("score", ascending=True).reset_index(drop=True)


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

    """
    allowed = {1, 3, 6, 12}
    bad = [h for h in horizons if h not in allowed]
    if bad:
        raise SystemExit(
            f"[select] Horizons not supported yet: {bad}. "
            f"Available horizons in v_forecast_eval: {sorted(allowed)}"
        )
    """
        
    con = connect()

    """
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
    """

    df_long = latest_batch_eval_long(con, target, batch_hours=args.batch_hours)
    if df_long.empty:
        raise SystemExit("[select] No backtest eval rows found for this target in the DB.")
    
    scored = compute_weighted_score_long(
        df_long,
        metric=args.metric,
        horizons=horizons,
        weights=weights,
        strict=False,
    )
    
    if scored.empty:
        # Useful diagnostics: show what horizons exist
        have = (
            df_long.groupby("model_name")["horizon_months"]
            .apply(lambda s: ",".join(map(str, sorted(set(s)))))
            .reset_index(name="available_horizons")
        )
        print("[select] No model had all requested horizons. Available horizons by model:")
        print(have.to_string(index=False))
        raise SystemExit(1)
    
    print("\n[select] Latest-batch scoreboard (per model family):")
    print(scored.to_string(index=False))
    
    winner = scored.iloc[0]["model_name"]
    print(
        f"\n[select] WINNER = {winner} "
        f"(score={scored.iloc[0]['score']:.4f} using {args.metric} @ horizons={horizons})"
    )

    """
    if args.promote:
        # flip existing live runs off, then run live forecast for winner
        print("[select] Deactivating existing live runs for this target...")
        deactivate_live_runs(con, target)
        promote_winner(winner, target, horizon=args.live_horizon)
        print("[select] Promotion complete.")

    con.close()
    """

    if args.promote:
        print("[select] Deactivating existing live runs for this target...")
        deactivate_live_runs(con, target)
    
        # IMPORTANT: release DuckDB lock before spawning subprocess
        con.close()
    
        promote_winner(winner, target, horizon=args.live_horizon)
        print("[select] Promotion complete.")
        return
    
    con.close()

if __name__ == "__main__":
    main()
