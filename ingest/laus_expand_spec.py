# ingest/laus_expand_spec.py
import os
import yaml
import csv
from pathlib import Path

# Allow overrides via env; otherwise use defaults
SPEC_PATH = os.getenv("LAUS_SPEC_PATH", "config/laus_spec.yml")
OUT_CSV   = os.getenv("LAUS_OUT_CSV",   "config/laus_series.generated.csv")

def main():
    # Load spec (ensure YAML has spaces, not tabs)
    with open(SPEC_PATH, "r") as f:
        spec = yaml.safe_load(f)

    # Minimal validation / helpful errors
    try:
        series    = spec["series"]
        prefix    = series["prefix"]                 # e.g., "LA"
        seasonals = series["seasonal"]               # e.g., ["S","U"]
        measures  = series["measures"]               # e.g., {"03": {"base":"laus_unemployment_rate","name":"Unemployment Rate"}, ...}
        areas     = spec["areas"]                    # list of {geo_id, area_stem, name?, level?}
    except KeyError as e:
        raise SystemExit(f"[laus:gen] Missing required key in {SPEC_PATH}: {e}")

    rows = []
    # inside main(), after you read spec and before looping measures:
    for ar in areas:
        geo_id    = ar["geo_id"]
        area_stem = ar["area_stem"]
        level     = (ar.get("level") or "").lower()
    
        # SA is generally available for state/nation; sub-state is NSA-only
        if level in {"state", "nation"}:
            seasonals_for_area = ["S", "U"]
        else:
            seasonals_for_area = ["U"]
    
        for seas in seasonals_for_area:
            for suf, meta in measures.items():
                tail = str(suf).zfill(3)
                series_id   = f"{prefix}{seas}{area_stem}{tail}"
                seasonal_hr = "SA" if seas == "S" else "NSA"
                rows.append({
                    "geo_id": geo_id,
                    "series_id": series_id,
                    "metric_base": meta["base"],
                    "seasonal": seasonal_hr,
                    "name": f"{meta['name']} ({ar.get('level','area').title()}, {seasonal_hr})",
                    "notes": ar.get("name") or geo_id,
                })


    # Ensure destination directory exists
    out_path = Path(OUT_CSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write CSV that the bulk ingestor already supports
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["geo_id","series_id","metric_base","seasonal","name","notes"])
        w.writeheader()
        w.writerows(rows)

    print(f"[laus:gen] wrote {len(rows)} series rows → {out_path}")

if __name__ == "__main__":
    main()
