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


    # Deterministic collapse to 1 row per (base_feature_id, anchor)
    # - selected = any row selected
    # - lift_any = best lift among all rows (diagnostic)
    # - lift_selected = best lift among selected rows, else NaN
    out["_lift_sel"] = out["lift_vs_baseline"].where(out["selected"].eq(1), float("nan"))

    g = (
        out.groupby(["base_feature_id", "anchor"], as_index=False)
           .agg(
               selected=("selected", "max"),
               lift_any=("lift_vs_baseline", "max"),
               lift_selected=("_lift_sel", "max"),
               best_lead=("best_lead", lambda s: int(s.mode().iloc[0]) if len(s.mode()) else int(s.iloc[0])),
               n_eff=("n_eff", "max"),
           )
    )

    return g


def summarize(
    g: pd.DataFrame,
    *,
    win_eps: float = 0.0,
    min_selected_anchors: int = 2,
    p10_any_min: float = -0.05,
    neg_rate_any_max: float = 0.25,
) -> pd.DataFrame:
    """
    Produce ONE merged table (Option B) with:
      - promotion-aligned ranking (selected-only lift stats)
      - intrinsic ranking (any-anchor lift stats)
      - origin flag: both | promotion | intrinsic | neither

    Gates:
      - downside gates apply to both tracks (p10_any_min, neg_rate_any_max)
      - min_selected_anchors applies only to promotion track
    """

    # -------------------------
    # Per-row win flags
    # -------------------------
    g = g.copy()
    g["win_selected"] = g["lift_selected"] > float(win_eps)
    g["win_any"] = g["lift_any"] > float(win_eps)

    # -------------------------
    # Selected-only stats (promotion-aligned)
    # -------------------------
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

    # -------------------------
    # All-anchors stats (intrinsic)
    # -------------------------
    s_any = g.groupby("base_feature_id", as_index=False).agg(
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

    # merge
    s = s_any.merge(s_sel, on="base_feature_id", how="left")

    # -------------------------
    # Eligibility gates
    # -------------------------
    downside_ok = (
        (s["p10_lift_any"].fillna(float("-inf")) >= float(p10_any_min)) &
        (s["neg_rate_any"].fillna(1.0) <= float(neg_rate_any_max))
    )

    s["eligible_intrinsic"] = downside_ok

    s["eligible_promotion"] = (
        downside_ok &
        (s["selected_anchors"].fillna(0).astype(int) >= int(min_selected_anchors))
    )

    # -------------------------
    # Scores (keep them explicit + separate)
    # -------------------------
    # Promotion score: selected-only stability
    denom_p = 1.0 + s["std_lift_selected"].fillna(0.0)
    s["promotion_score"] = (
        s["median_lift_selected"].fillna(float("-inf"))
        * s["win_rate_selected"].fillna(0.0)
        / denom_p
    )
    s.loc[~s["eligible_promotion"], "promotion_score"] = float("-inf")

    # Intrinsic score: “physics” on any anchors (downside-gated)
    denom_i = 1.0 + s["std_lift_any"].fillna(0.0)
    s["intrinsic_score"] = (s["median_lift_any"].fillna(float("-inf")) / denom_i)
    s.loc[~s["eligible_intrinsic"], "intrinsic_score"] = float("-inf")

    # -------------------------
    # Ranks (separate)
    # -------------------------
    # Deterministic tie-breakers matter.
    s = s.sort_values(
        ["promotion_score", "median_lift_selected", "win_rate_selected", "selected_freq", "base_feature_id"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    s["promotion_rank"] = range(1, len(s) + 1)

    s = s.sort_values(
        ["intrinsic_score", "median_lift_any", "p10_lift_any", "neg_rate_any", "base_feature_id"],
        ascending=[False, False, False, True, True],
    ).reset_index(drop=True)
    s["intrinsic_rank"] = range(1, len(s) + 1)

    # -------------------------
    # Origin flag (your “overlap then prioritize” plan)
    # -------------------------
    s["origin"] = "neither"
    s.loc[s["eligible_intrinsic"] & ~s["eligible_promotion"], "origin"] = "intrinsic"
    s.loc[s["eligible_promotion"] & ~s["eligible_intrinsic"], "origin"] = "promotion"  # should be rare given gates
    s.loc[s["eligible_promotion"] & s["eligible_intrinsic"], "origin"] = "both"

    # A default “final sort” for human review:
    # both first, then promotion, then intrinsic, then neither
    origin_order = {"both": 0, "promotion": 1, "intrinsic": 2, "neither": 3}
    s["origin_order"] = s["origin"].map(origin_order).fillna(9).astype(int)

    s = s.sort_values(
        ["origin_order", "promotion_rank", "intrinsic_rank", "base_feature_id"],
        ascending=[True, True, True, True],
    ).reset_index(drop=True)

    # nice-to-have review rank for the merged view
    s["rank_merged"] = range(1, len(s) + 1)
    s = s.drop(columns=["origin_order"])

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

        out_parq = out_root / f"stability_merged__metric={m}.parquet"
        out_csv  = out_root / f"stability_merged__metric={m}.csv"
        s.to_parquet(out_parq, index=False)
        s.to_csv(out_csv, index=False)

        print(f"[ok] {m} rows={len(s)} -> {out_csv}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
