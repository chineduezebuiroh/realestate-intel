# ingest/census_geo_map.py
from pathlib import Path
import pandas as pd

GEO_MANIFEST = Path("config/geo_manifest.csv")

def _normalize_include_flag(val: str) -> bool:
    v = (val or "").strip().upper()
    return v in {"1", "Y", "YES", "TRUE", "T"}

def load_census_geo_map() -> dict[str, dict[str, str]]:
    """
    Build mapping:
      geo_id -> {
        "level":        <manifest level>,
        "census_code":  <string>,
        "include":      <bool>,
      }

    Uses:
      - 'geo_id'
      - 'census_code'
      - 'include_census'
      - 'level' (or 'geo_kind' fallback)
    from config/geo_manifest.csv
    """
    if not GEO_MANIFEST.exists():
        raise SystemExit("[census:geo] missing config/geo_manifest.csv")

    gm = pd.read_csv(GEO_MANIFEST, dtype=str)

    needed = {"geo_id", "census_code", "include_census"}
    missing = needed - set(gm.columns)
    if missing:
        raise SystemExit(f"[census:geo] geo_manifest.csv missing columns: {sorted(missing)}")

    # support either 'level' or 'geo_kind'
    if "level" in gm.columns:
        level_col = "level"
    elif "geo_kind" in gm.columns:
        level_col = "geo_kind"
    else:
        raise SystemExit("[census:geo] geo_manifest.csv must have 'level' or 'geo_kind' column")

    for col in ["geo_id", "census_code", "include_census", level_col]:
        gm[col] = gm[col].fillna("").astype(str).str.strip()

    mapping: dict[str, dict[str, str]] = {}

    for _, row in gm.iterrows():
        geo_id = row["geo_id"]
        level = row[level_col].lower()
        code  = row["census_code"]
        include = _normalize_include_flag(row["include_census"])

        if not include or not code:
            # quietly skip rows that either opt-out or have no code
            continue

        mapping[geo_id] = {
            "level": level,
            "census_code": code,
            "include": include,
        }

    print(f"[census:geo] loaded {len(mapping)} Census geos from geo_manifest.csv")
    return mapping

if __name__ == "__main__":
    m = load_census_geo_map()
    for k, v in list(m.items())[:10]:
        print(k, "→", v)
