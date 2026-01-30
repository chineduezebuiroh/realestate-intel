# forecast/cli/model_select.py

import argparse
import subprocess
from dataclasses import dataclass
from typing import List

import pandas as pd
import duckdb

from core.db import connect


# Compare “families” (stable intent) while tolerating historical model_name drift.
FAMILIES = ("sarimax_univariate", "sarimax_exog", "xgb")

# Map family -> the model_name values that currently exist in forecast_runs for backtests
MODEL_NAME_BY_FAMILY = {
    "sarimax_univariate": "sarimax_univariate",
    "sarimax_exog": "sarimax_exog_backtest",
    "xgb": "xgb_forecast",
}

BACKTEST_MODELS = tuple(MODEL_NAME_BY_FAMILY.values())


# Promotion should run LIVE runners/CLIs, not backtests.
# Choose the thinnest stable entrypoints you have today.
PROMOTION_CMD = {}


@dataclass
class Target:
    metric_id: str
    geo_id: str
    property_type_id: str


def latest_batch_eval_long(con: duckdb.DuckDBPyConnection, target: Target) -> pd.DataFrame:
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
    latest_data_asof AS (
      SELECT MAX(data_asof) AS data_asof
      FROM candidate_runs
    ),
    runs_same_asof AS (
      SELECT *
      FROM candidate_runs
      WHERE data_asof = (SELECT data_asof FROM latest_data_asof)
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
    eval_preds AS (
      SELECT
        r.model_name,
        r.created_at,
        r.batch_id,
        e.run_id,
        e.horizon_months,
        e.abs_err,
        e.sq_err,
        e.ape
      FROM batch_runs r
      JOIN v_forecast_eval_long e USING (run_id)
    )
    SELECT
      model_name,
      batch_id,
      horizon_months,
      COUNT(DISTINCT run_id) AS n_runs,
      AVG(abs_err) AS mae_avg,
      SQRT(AVG(sq_err)) AS rmse_avg,
      AVG(ape) AS mape_avg,
      MIN(created_at) AS batch_start,
      MAX(created_at) AS batch_end
    FROM eval_preds
    GROUP BY model_name, batch_id, horizon_months
    ORDER BY model_name, horizon_months;
    """
    return con.execute(q, [target.metric_id, target.geo_id, target.property_type_id]).fetchdf()


def compute_weighted_score_long(
    df_long: pd.DataFrame,
    metric: str,
    horizons: List[int],
    weights: List[float],
    strict: bool = False,
) -> pd.DataFrame:
    metric = metric.lower()
    allowed = {"rmse", "mae", "mape"}
    if metric not in allowed:
        raise ValueError(f"metric must be one of {sorted(allowed)}")

    if len(horizons) != len(weights):
        raise ValueError("horizons and weights must be same length")

    s = sum(weights)
    if s <= 0:
        raise ValueError("weights must sum to > 0")
    weights = [w / s for w in weights]

    value_col = f"{metric}_avg"

    out_rows = []
    for model_name, g in df_long.groupby("model_name"):
        g = (
            g.groupby("horizon_months", as_index=False)
             .agg({
                 value_col: "mean",
                 "n_runs": "max",
                 "batch_start": "min",
                 "batch_end": "max",
                 "batch_id": "max",
             })
        )

        g2 = g.set_index("horizon_months")

        missing = [h for h in horizons if h not in g2.index]
        if missing and strict:
            continue

        used_pairs = [(h, w) for h, w in zip(horizons, weights) if h in g2.index]
        if not used_pairs:
            continue

        wsum = sum(w for _, w in used_pairs)
        if wsum <= 0:
            continue

        score = 0.0
        for h, w in used_pairs:
            score += (w / wsum) * float(g2.loc[h, value_col])

        out_rows.append({
            "model_name": model_name,
            "batch_id": str(g["batch_id"].iloc[0]),
            "n_runs": int(g["n_runs"].max()),
            "batch_start": g["batch_start"].min(),
            "batch_end": g["batch_end"].max(),
            "score_metric": metric,
            "requested_horizons": ",".join(map(str, horizons)),
            "requested_weights": ",".join(f"{w:.4g}" for w in weights),
            "score": float(score),
            "missing_horizons": ",".join(map(str, missing)) if missing else "",
        })

    scored = pd.DataFrame(out_rows)
    if scored.empty:
        return scored
    return scored.sort_values("score", ascending=True).reset_index(drop=True)


def deactivate_live_runs_by_kind(con: duckdb.DuckDBPyConnection, target: Target, run_kind: str) -> None:
    q = """
    UPDATE forecast_runs
    SET is_active = FALSE
    WHERE target_metric_id = ?
      AND target_geo_id = ?
      AND target_property_type_id = ?
      AND run_kind = ?
      AND is_active = TRUE
      AND model_name NOT LIKE '%backtest%';
    """
    con.execute(q, [target.metric_id, target.geo_id, target.property_type_id, run_kind])

"""
def promote_winner(
    winner_model_name: str,
    target: Target,
    horizon: int,
    run_kind: str,
    data_asof: str,
    xgb_batch_id: str | None = None,
    label: str | None = None,
) -> None:
    if winner_model_name not in PROMOTION_CMD:
        raise ValueError(f"No promotion command mapping for winner model: {winner_model_name}")

    cmd = PROMOTION_CMD[winner_model_name] + [
        "--metric_id", target.metric_id,
        "--geo_id", target.geo_id,
        "--property_type_id", target.property_type_id,
        "--horizon", str(horizon),
        "--run_kind", run_kind,
        "--data_asof", data_asof,
    ]

    if label:
        cmd += ["--label", label]

    if winner_model_name == "sarimax_exog_backtest":
        if not xgb_batch_id:
            raise ValueError("--xgb_batch_id is required to promote sarimax_exog.")
        cmd += ["--xgb_batch_id", xgb_batch_id]

    print(f"[select] Promoting winner by running:\n  {' '.join(cmd)}")
    subprocess.check_call(cmd)
"""

def promote_winner(*args, **kwargs) -> None:
    raise SystemExit(
        "[select] Promotion is disabled in Phase C re-org. "
        "Implement canonical live entrypoints first, then wire PROMOTION_CMD."
    )


def main():
    ap = argparse.ArgumentParser(description="Select best model family for a single target from latest backtest batch.")
    ap.add_argument("--metric_id", required=True)
    ap.add_argument("--geo_id", required=True)
    ap.add_argument("--property_type_id", required=True)
    ap.add_argument("--metric", default="rmse", choices=["rmse", "mae", "mape"])
    ap.add_argument("--horizons", default="3,6,12", help="Comma-separated horizons, e.g. 1,3,6,12")
    ap.add_argument("--weights", default="0.2,0.5,0.3", help="Comma-separated weights matching horizons")
    ap.add_argument("--promote", action="store_true")
    ap.add_argument("--live_horizon", type=int, default=12)
    ap.add_argument("--run_kind", default=None, help="Required for promotion: live_near or live_outlook.")
    ap.add_argument("--data_asof", default=None, help="Required for promotion; freezes data reads (YYYY-MM-DD).")
    ap.add_argument("--xgb_batch_id", default=None, help="Required for sarimax_exog promotion.")
    ap.add_argument("--label", default=None)

    args = ap.parse_args()

    target = Target(args.metric_id, args.geo_id, args.property_type_id)
    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]
    weights = [float(x) for x in args.weights.split(",") if x.strip()]

    con = connect()

    df_long = latest_batch_eval_long(con, target)
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
        have = (
            df_long.groupby("model_name")["horizon_months"]
            .apply(lambda s: ",".join(map(str, sorted(set(s)))))
            .reset_index(name="available_horizons")
        )
        print("[select] No model could be scored. Available horizons by model:")
        print(have.to_string(index=False))
        raise SystemExit(1)

    print("\n[select] Latest-batch scoreboard (per model family):")
    print(scored.to_string(index=False))

    winner = str(scored.iloc[0]["model_name"])
    print(f"\n[select] WINNER = {winner} (score={scored.iloc[0]['score']:.4f})")

    if args.promote:
        if args.run_kind not in ("live_near", "live_outlook"):
            raise SystemExit("[select] --run_kind must be live_near or live_outlook when promoting.")
        if not args.data_asof:
            raise SystemExit("[select] --data_asof is required when promoting.")

        print("[select] Deactivating existing live runs for this target...")
        deactivate_live_runs_by_kind(con, target, args.run_kind)

        # IMPORTANT: release DuckDB lock before spawning subprocess
        con.close()

        promote_winner(
            winner,
            target,
            horizon=args.live_horizon,
            run_kind=args.run_kind,
            data_asof=args.data_asof,
            xgb_batch_id=args.xgb_batch_id,
            label=args.label,
        )
        print("[select] Promotion complete.")
        return

    con.close()


if __name__ == "__main__":
    main()
