# sources/census/expand_spec.py
import os
import csv
from pathlib import Path
import datetime as _dt
import pandas as pd

GEO_MANIFEST = Path("config/geo_manifest.csv")

OUT_PLAN = Path("data/census/census_acs5_query_plan.generated.csv")

CENSUS_DATASET = "acs/acs5"

# Keep minimal for now; expand later.
VARS = [
    ("census_pop_total", "B01003_001E"),
    ("census_median_household_income", "B19013_001E"),
]


def _normalize_include_flag(val: str) -> bool:
    v = (val or "").strip().upper()
    return v in {"1", "Y", "YES", "TRUE", "T"}


def main():
    if not GEO_MANIFEST.exists():
        raise SystemExit("[census:gen] missing config/geo_manifest.csv")

    gm = pd.read_csv(GEO_MANIFEST, dtype=str)

    needed = {"geo_id", "census_code", "include_census"}
    missing = needed - set(gm.columns)
    if missing:
        raise SystemExit(f"[census:gen] geo_manifest.csv missing columns: {sorted(missing)}")

    # support either 'level' or 'geo_kind'
    if "level" in gm.columns:
        level_col = "level"
    elif "geo_kind" in gm.columns:
        level_col = "geo_kind"
    else:
        raise SystemExit("[census:gen] geo_manifest.csv must have 'level' or 'geo_kind'")

    for col in ["geo_id", "census_code", "include_census", level_col]:
        gm[col] = gm[col].fillna("").astype(str).str.strip()

    # default vintage = last full year (stable enough for now)
    vintage = _dt.date.today().year - 1

    variables_csv = ",".join([v for _, v in VARS])
    metric_ids_csv = ",".join([m for m, _ in VARS])

    rows = []
    for r in gm.itertuples(index=False):
        geo_id = getattr(r, "geo_id")
        census_code = getattr(r, "census_code")
        include = _normalize_include_flag(getattr(r, "include_census"))
        level = getattr(r, level_col).strip().lower()

        if not include or not census_code:
            continue

        rows.append({
            "geo_id": geo_id,
            "geo_level": level,
            "census_code": census_code,
            "dataset": CENSUS_DATASET,
            "vintage": vintage,
            "variables_csv": variables_csv,
            "metric_ids_csv": metric_ids_csv,
        })

    if not rows:
        print("[census:gen] no Census geos enabled (include_census=1).")
        return

    OUT_PLAN.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PLAN.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(
            f,
            fieldnames=["geo_id","geo_level","census_code","dataset","vintage","variables_csv","metric_ids_csv"]
        )
        wr.writeheader()
        wr.writerows(sorted(rows, key=lambda d: d["geo_id"]))

    print(f"[census:gen] wrote {len(rows)} plan rows → {OUT_PLAN}")
    print(f"[census:gen] vintage={vintage} variables={variables_csv}")


if __name__ == "__main__":
    main()
