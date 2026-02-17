from __future__ import annotations
# forecast/selection/stability_rank.py

from pathlib import Path
import re
import pandas as pd


ANCHORS = ["2022-06-30","2022-09-30","2022-12-31","2023-03-31","2023-06-30","2023-09-30","2023-12-31","2024-03-31","2024-06-30"]

def strip_lag(fid: str) -> str:
    return re.sub(r"_lag\d+$", "", str(fid))

def load_metric_runs(metric_dir: Path) -> pd.DataFrame:
    rows = []
    for a in ANCHORS:
        p = metric_dir / f"candidate_scores__anchor={a}.parquet"
        df = pd.read_parquet(p)

        # base feature id
        df["base_feature_id"] = df["feature_id"].astype(str).map(strip_lag)
        df["anchor"] = a

        # guardrails
        if "lift_vs_baseline" not in df.columns:
            raise ValueError(f"missing lift_vs_baseline in {p}")
        if "selected" not in df.columns:
            raise ValueError(f"missing selected in {p}")

        rows.append(df[[
            "anchor",
            "base_feature_id",
            "lift_vs_baseline",
            "selected",
            "best_lead",
            "n_eff",
        ]].copy())
    out = pd.concat(rows, ignore_index=True)

    def _lift_pick(df: pd.DataFrame) -> float:
        # If any lag was selected at this anchor, use the best selected lag’s lift.
        # Otherwise use the best available lift (keeps behavior similar to before).
        sel = df[df["selected"].astype(bool)]
        if len(sel) > 0:
            return float(sel["lift_vs_baseline"].max())
        return float(df["lift_vs_baseline"].max())
    
    g = (
        out.groupby(["base_feature_id", "anchor"], as_index=False)
           .apply(lambda df: pd.Series({
               "lift": _lift_pick(df),
               "selected": int(df["selected"].max()),
               "best_lead": int(df["best_lead"].mode().iloc[0]) if len(df["best_lead"].mode()) else int(df["best_lead"].iloc[0]),
               "n_eff": int(df["n_eff"].max()),
           }))
           .reset_index(drop=True)
    )
    
    return g


def summarize(g: pd.DataFrame, *, win_eps: float = 0.0) -> pd.DataFrame:
    # win = lift strictly above baseline (eps lets you ignore numerical noise)
    g["win"] = g["lift"] > float(win_eps)

    s = g.groupby("base_feature_id", as_index=False).agg(
        anchors=("anchor", "nunique"),
        mean_lift=("lift", "mean"),
        median_lift=("lift", "median"),
        std_lift=("lift", lambda x: float(x.std(ddof=0))),  # ddof=0 = population std, stable for small N
        p25_lift=("lift", lambda x: float(x.quantile(0.25))),
        p75_lift=("lift", lambda x: float(x.quantile(0.75))),
        min_lift=("lift", "min"),
        max_lift=("lift", "max"),
        win_rate=("win", "mean"),
        selected_freq=("selected", "mean"),
        best_lead_mode=("best_lead", lambda x: int(x.mode().iloc[0]) if len(x.mode()) else int(x.iloc[0])),
    )

    # Guard: avoid division by zero; also keeps score scale sensible
    s["stability_score"] = (
        s["median_lift"]
        * s["win_rate"]
        * s["selected_freq"]
        / (1.0 + s["std_lift"].fillna(0.0))
    )

    s = s.sort_values(
        ["stability_score", "median_lift", "win_rate", "selected_freq"],
        ascending=False
    ).reset_index(drop=True)

    s["rank"] = range(1, len(s) + 1)
    return s


def main() -> int:
    batch = "phasec__selector__geo=dc_city__m={metric}__h=3__asof=2025-12-31__s1=cheap_lift__v=08"
  
    root = Path("artifacts/phasec/runs")

    metrics = ["median_sale_price", "median_ppsf", "median_dom"]
    out_root = Path("artifacts/phasec/selector_stability") / "v08"
    out_root.mkdir(parents=True, exist_ok=True)

    for m in metrics:
        metric_dir = root / (batch.format(metric=m)) / "xgb" / m
        if not metric_dir.exists():
            raise SystemExit(f"missing metric_dir: {metric_dir}")

        g = load_metric_runs(metric_dir)
        s = summarize(g)

        out_parq = out_root / f"stability_summary__metric={m}.parquet"
        out_csv  = out_root / f"stability_summary__metric={m}.csv"
        s.to_parquet(out_parq, index=False)
        s.to_csv(out_csv, index=False)

        print(f"[ok] {m} rows={len(s)} -> {out_csv}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
