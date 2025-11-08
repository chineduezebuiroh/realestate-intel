# ingest/laus_expand_spec.py
import yaml, csv, sys
from pathlib import Path

SPEC_PATH = Path("config/laus_spec.yml")
OUT_CSV   = Path("config/laus_series.generated.csv")

def main():
    spec = yaml.safe_load(open(SPEC_PATH))
    prefix   = spec["series"]["prefix"]
    seasonals= spec["series"]["seasonal"]         # ["S","U"]
    measures = spec["series"]["measures"]         # {"003": {...}, ...}
    areas    = spec["areas"]

    rows = []
    for ar in areas:
        geo_id    = ar["geo_id"]
        area_stem = ar["area_stem"]
        for seas in seasonals:
            for suf, meta in measures.items():
                series_id   = f"{prefix}{seas}{area_stem}{suf}"
                seasonal_hr = "SA" if seas == "S" else "NSA"
                rows.append({
                    "geo_id": geo_id,
                    "series_id": series_id,
                    "metric_base": meta["base"],  # e.g., "laus_employment"
                    "seasonal": seasonal_hr,      # "SA"/"NSA"
                    "name": f"{meta['name']} ({ar.get('level','area').title()}, {seasonal_hr})",
                    "notes": ar.get("name") or geo_id,
                })

    # write the generated CSV in the same schema your bulk ingestor already supports
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["geo_id","series_id","metric_base","seasonal","name","notes"])
        w.writeheader()
        w.writerows(rows)

    print(f"[laus] wrote {len(rows)} series rows → {OUT_CSV}")

if __name__ == "__main__":
    main()
