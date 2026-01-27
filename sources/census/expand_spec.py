from __future__ import annotations
# sources/census/expand_spec.py

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

GEO_MANIFEST = Path("config/geo_manifest.csv")


def _normalize_include_flag(val: str) -> bool:
    v = (val or "").strip().upper()
    return v in {"1", "Y", "YES", "TRUE", "T"}


def load_census_geo_map(manifest_path: Path = GEO_MANIFEST) -> dict[str, dict[str, str]]:
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
    if not manifest_path.exists():
        raise SystemExit("[census:geo] missing config/geo_manifest.csv")

    gm = pd.read_csv(manifest_path, dtype=str)

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
        code = row["census_code"]
        include = _normalize_include_flag(row["include_census"])

        if not include or not code:
            continue

        mapping[geo_id] = {
            "level": level,
            "census_code": code,
            "include": include,
        }

    print(f"[census:geo] loaded {len(mapping)} Census geos from geo_manifest.csv")
    return mapping


def build_census_query_plan(
    *,
    dataset: str,
    vintage: int,
    variables: list[str],
    geo_map: Optional[dict[str, dict[str, str]]] = None,
) -> pd.DataFrame:
    """
    Expand step: produce a deterministic query plan that ingest.py can execute without guessing.

    Returns a DataFrame with one row per (geo_id, variable-set) query:
      - source_id
      - dataset
      - vintage
      - geo_id
      - geo_level
      - census_code
      - variables_csv   (kept stable/deterministic)
      - query_kind      ("for_in" for now; can add "ucgid" later)
      - for_clause
      - in_clause

    NOTE:
    - We are not implementing UC-GID here yet. This keeps Phase C entry simple.
    - geo_level/census_code must be interpreted consistently by ingest.py for URL construction.
    """
    if geo_map is None:
        geo_map = load_census_geo_map()

    if not variables:
        raise ValueError("[census:expand] variables must be non-empty")
    variables = [v.strip() for v in variables if (v or "").strip()]
    if not variables:
        raise ValueError("[census:expand] variables must be non-empty after stripping")

    rows = []
    for geo_id, meta in sorted(geo_map.items(), key=lambda kv: kv[0]):
        level = meta["level"]
        code = meta["census_code"]

        # Minimal contract: ingest.py decides how to convert (level, code) -> for/in params.
        # We keep the plan explicit so it's debuggable.
        rows.append(
            {
                "source_id": "census",
                "dataset": dataset,
                "vintage": int(vintage),
                "geo_id": geo_id,
                "geo_level": level,
                "census_code": code,
                "variables_csv": ",".join(sorted(set(variables))),
                "query_kind": "for_in",
                "for_clause": "",  # populated by ingest.py based on geo_level + census_code
                "in_clause": "",   # populated by ingest.py based on geo_level + census_code
            }
        )

    df = pd.DataFrame(rows)
    print(f"[census:expand] built query plan: {len(df)} geo queries, {len(variables)} variables")
    return df


if __name__ == "__main__":
    # Smoke test
    gm = load_census_geo_map()
    plan = build_census_query_plan(dataset="acs/acs5", vintage=2023, variables=["B01001_001E"], geo_map=gm)
    print(plan.head(10).to_string(index=False))
