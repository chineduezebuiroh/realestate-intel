from __future__ import annotations

from pathlib import Path
import pandas as pd

# Config
METRICS = ["median_sale_price", "median_ppsf", "median_dom"]
N = 100
ALLOWED_LAGS = [1, 3, 6, 12]

# Priority order for freezing
ORIGIN_PRIORITY = {"both": 0, "promotion": 1, "intrinsic": 2, "neither": 9}

def build_one(merged_csv: Path, *, n: int = N) -> pd.DataFrame:
    df = pd.read_csv(merged_csv)

    required = [
        "base_feature_id",
        "origin",
        "promotion_rank",
        "intrinsic_rank",
        "rank_merged",
        "selected_anchors",
        "selected_freq",
        "median_lift_selected",
        "std_lift_selected",
        "p10_lift_any",
        "neg_rate_any",
        "best_lead_mode",
        "eligible_promotion",
        "eligible_intrinsic",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {merged_csv}: {missing}")

    # Enforce the freeze rule:
    # 1) origin in (both, promotion, intrinsic) only
    # 2) rank by origin priority then rank_merged
    df = df.copy()
    df["origin_priority"] = df["origin"].map(ORIGIN_PRIORITY).fillna(9).astype(int)

    # If you truly want “intrinsic lowest priority and ideally unused”:
    # keep intrinsic, but it comes after promotion.
    df = df[df["origin"].isin(["both", "promotion", "intrinsic"])]

    df = df.sort_values(
        ["origin_priority", "rank_merged", "promotion_rank", "intrinsic_rank"],
        ascending=[True, True, True, True],
    )

    out = df.head(int(n)).copy()

    # ---- enforce allowed lag contract + emit lagged feature_id for bridge ----
    out["best_lead_mode"] = out["best_lead_mode"].astype(int)

    bad = out.loc[~out["best_lead_mode"].isin(ALLOWED_LAGS), ["base_feature_id", "best_lead_mode"]]
    if len(bad) > 0:
        raise SystemExit(
            "[canonical_exogs] REFUSING: best_lead_mode contains disallowed lags. "
            f"allowed={ALLOWED_LAGS} bad_examples={bad.head(10).to_dict('records')}"
        )

    out["feature_id"] = out["base_feature_id"].astype(str) + "_lag" + out["best_lead_mode"].astype(str)
    

    # Minimal, stable schema for downstream consumption
    keep = [
        "base_feature_id",
        "feature_id",
        "origin",
        "promotion_rank",
        "intrinsic_rank",
        "rank_merged",
        "eligible_promotion",
        "eligible_intrinsic",
        "selected_anchors",
        "selected_freq",
        "best_lead_mode",
        "median_lift_selected",
        "std_lift_selected",
        "p10_lift_any",
        "neg_rate_any",
    ]

    out = out[keep].reset_index(drop=True)
    out["canonical_rank"] = range(1, len(out) + 1)
    return out

def main() -> int:
    merged_root = Path("artifacts/phasec/selector_stability/v08.3")
    out_root = Path("artifacts/phasec/canonical_exogs/v08.3")
    out_root.mkdir(parents=True, exist_ok=True)

    for m in METRICS:
        merged_csv = merged_root / f"stability_merged__metric={m}.csv"
        if not merged_csv.exists():
            raise SystemExit(f"Missing merged stability CSV: {merged_csv}")

        out = build_one(merged_csv, n=N)

        out_csv = out_root / f"canonical_exog_set__metric={m}__n={N}.csv"
        out.to_csv(out_csv, index=False)
        print(f"[ok] {m} -> {out_csv} rows={len(out)} origin_counts={out['origin'].value_counts().to_dict()}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
