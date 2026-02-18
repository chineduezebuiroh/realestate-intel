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

        df["base_feature_id"] = df["feature_id"].astype(str).map(strip_lag)
        df["anchor"] = a

        # guardrails
        for col in ["lift_vs_baseline", "selected", "best_lead", "n_eff"]:
            if col not in df.columns:
                raise ValueError(f"missing {col} in {p}")

        df["selected"] = df["selected"].astype(int)

        rows.append(df[[
            "anchor",
            "base_feature_id",
            "lift_vs_baseline",
            "selected",
            "best_lead",
            "n_eff",
        ]].copy())

    out = pd.concat(rows, ignore_index=True)

    # If you ever end up with multiple rows per base_feature_id per anchor (e.g. if lags sneak in),
    # collapse deterministically:
    g = (
        out.groupby(["base_feature_id", "anchor"], as_index=False)
           .agg(
               # any selected row makes it selected for the anchor
               selected=("selected", "max"),
               # "any" lift (diagnostic): best lift available among rows that exist
               lift_any=("lift_vs_baseline", "max"),
               # lift when selected: only consider rows that were selected; otherwise NaN
               lift_selected=("lift_vs_baseline", lambda s: float(s[out.loc[s.index, "selected"].astype(bool)].max())
                             if bool(out.loc[s.index, "selected"].any()) else float("nan")),
               best_lead=("best_lead", lambda s: int(s.mode().iloc[0]) if len(s.mode()) else int(s.iloc[0])),
               n_eff=("n_eff", "max"),
           )
    )

    return g


def summarize(g: pd.DataFrame, *, win_eps: float = 0.0, min_selected_anchors: int = 2) -> pd.DataFrame:
    """
    Promotion-aligned stability:
      - Primary lift stats computed on anchors where the feature was actually selected
      - selected_freq used as a gate / tie-breaker, NOT multiplied into the score
    """

    # win only defined where lift_selected exists (i.e. selected anchors)
    g["win_selected"] = g["lift_selected"] > float(win_eps)

    # ---- selected-only stats ----
    sel = g[g["selected"].eq(1)].copy()

    s_sel = sel.groupby("base_feature_id", as_index=False).agg(
        selected_anchors=("anchor", "nunique"),
        mean_lift_selected=("lift_selected", "mean"),
        median_lift_selected=("lift_selected", "median"),
        std_lift_selected=("lift_selected", lambda x: float(x.std(ddof=0))),
        p25_lift_selected=("lift_selected", lambda x: float(x.quantile(0.25))),
        p75_lift_selected=("lift_selected", lambda x: float(x.quantile(0.75))),
        min_lift_selected=("lift_selected", "min"),
        max_lift_selected=("lift_selected", "max"),
        win_rate_selected=("win_selected", "mean"),
        best_lead_mode=("best_lead", lambda x: int(x.mode().iloc[0]) if len(x.mode()) else int(x.iloc[0])),
    )

    # ---- all-anchors diagnostics (optional but very useful) ----
    s_all = g.groupby("base_feature_id", as_index=False).agg(
        anchors=("anchor", "nunique"),
        selected_freq=("selected", "mean"),
    
        mean_lift_any=("lift_any", "mean"),
        median_lift_any=("lift_any", "median"),
        std_lift_any=("lift_any", lambda x: float(x.std(ddof=0))),
        p10_lift_any=("lift_any", lambda x: float(x.quantile(0.10))),
        neg_rate_any=("lift_any", lambda x: float((x < 0).mean())),
    
        min_lift_any=("lift_any", "min"),
        max_lift_any=("lift_any", "max"),
    )

    s = s_all.merge(s_sel, on="base_feature_id", how="left")

    # Gate: if it was selected too few times, its "selected-lift stability" isn't meaningful.
    # Keep it, but force score to the bottom.
    s["eligible"] = (
        (s["selected_anchors"].fillna(0).astype(int) >= int(min_selected_anchors)) &
        (s["p10_lift_any"].fillna(float("-inf")) >= -0.05) &
        (s["neg_rate_any"].fillna(1.0) <= 0.25)
    )

    # stability score (NO selected_freq multiplier)
    # This is the formula you said you want the script to match.
    denom = 1.0 + s["std_lift_selected"].fillna(0.0)
    s["stability_score"] = (
        s["median_lift_selected"].fillna(float("-inf"))
        * s["win_rate_selected"].fillna(0.0)
        / denom
    )

    # push ineligible to bottom deterministically
    s.loc[~s["eligible"], "stability_score"] = float("-inf")

    s = s.sort_values(
        ["stability_score", "median_lift_selected", "win_rate_selected", "selected_freq"],
        ascending=False
    ).reset_index(drop=True)

    s["rank"] = range(1, len(s) + 1)
    return s


def main() -> int:
    batch = "phasec__selector__geo=dc_city__m={metric}__h=3__asof=2025-12-31__s1=cheap_lift__v=08"
  
    root = Path("artifacts/phasec/runs")

    metrics = ["median_sale_price", "median_ppsf", "median_dom"]
    out_root = Path("artifacts/phasec/selector_stability") / "v08.1"
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
