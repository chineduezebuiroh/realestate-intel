# ingest/census_api_bulk.py
"""
Bulk fetch ACS 5-year Census data for the geos in config/geo_manifest.csv.

Design:
- geo_manifest.csv drives which geos we pull:
    - geo_id
    - level            (state/county/place/msa/csa)
    - include_census   (Y/N, 1/0, true/false)
    - census_code      (meaning depends on level)
- We infer Census API "for" / "in" params from (level, census_code).

Output:
- data/census_acs5_timeseries.csv with columns:
    geo_id, level, census_code, year, date, metric_id, value
"""

from __future__ import annotations

import csv
import os
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests
import pandas as pd
import datetime as _dt  # add this near the other imports

GEO_MANIFEST = Path("config/geo_manifest.csv")
OUT_CSV = Path("data/census_acs5_timeseries.csv")

# ---- BASIC CONFIG (you can move this into a YAML later if you want) ----


CENSUS_DATASET = "acs/acs5"

CURRENT_YEAR = _dt.date.today().year
YEAR_END = CURRENT_YEAR - 1  # use last full year as “max likely available”
YEAR_START = CURRENT_YEAR - 16  

YEARS = list(range(YEAR_START, YEAR_END + 1))


# metric_id -> Census variable name
ACS_VARS: Dict[str, str] = {
    # total population
    "census_pop_total": "B01003_001E",
    # median household income in the past 12 months (inflation-adjusted)
    "census_median_household_income": "B19013_001E",
}


# ---------------------------------------------------------------------


def _normalize_bool(val: Any) -> bool:
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    return s in {"y", "yes", "1", "true", "t"}


def load_geo_manifest_for_census() -> pd.DataFrame:
    if not GEO_MANIFEST.exists():
        raise SystemExit(f"[census] missing {GEO_MANIFEST}")

    gm = pd.read_csv(GEO_MANIFEST, dtype=str)

    required_cols = {"geo_id", "level", "include_census", "census_code"}
    missing = required_cols - set(gm.columns)
    if missing:
        raise SystemExit(f"[census] geo_manifest.csv missing columns: {sorted(missing)}")

    gm["include_census"] = gm["include_census"].apply(_normalize_bool)
    gm["level"] = gm["level"].astype(str).str.strip().str.lower()
    gm["census_code"] = gm["census_code"].astype(str).str.strip()

    # Only rows explicitly marked as include_census
    gm = gm[gm["include_census"]]
    gm = gm[gm["census_code"] != ""]
    gm = gm[gm["level"] != ""]

    print(f"[census] using {len(gm)} rows from geo_manifest with include_census=true")
    return gm


def build_census_geo_params(level: str, code: str) -> dict[str, str] | None:
    """
    Map our geo_manifest level + census_code to Census API `for`/`in` params.
    """
    level = (level or "").strip().lower()
    code = (code or "").strip()

    if not code:
        return None

    # Top-level states: only `for=state:XX`, NO `in` parameter.
    if level in ("state", "state_equiv"):
        return {"for": f"state:{code}"}

    # Counties (and county-equivalent independent cities):
    # expect 5-digit state+county FIPS: SSCCC (e.g. 24031).
    if level in ("county", "county_equiv", "independent_city"):
        if len(code) != 5:
            return None
        state_fips = code[:2]
        county_fips = code[2:]
        return {
            "for": f"county:{county_fips}",
            "in": f"state:{state_fips}",
        }

    # Places (cities, towns, etc.): 7-digit state+place: SSPPPPP (e.g. 1150000).
    if level in ("city", "place"):
        if len(code) != 7:
            return None
        state_fips = code[:2]
        place_fips = code[2:]
        return {
            "for": f"place:{place_fips}",
            "in": f"state:{state_fips}",
        }

    # Metro/micro areas (MSA): 5-digit code
    if level in ("msa", "metro_area", "metro"):
        return {
            "for": f"metropolitan statistical area/micropolitan statistical area:{code}"
        }
    """
    # Combined statistical areas (CSA): 5-digit code
    if level in ("csa", "combined_area"):
        return {
            "for": f"combined statistical area:{code}"
        }
    """
    
    # Combined statistical areas — your `census_code` values (like 47764)
    # are BLS-style and don’t work against ACS directly. Skip for now.
    if level in ("csa", "combined_area"):
        return None
    
    """
    # Metro divisions
    if level in ("metro_division", "msd"):
        return {
            "for": f"metropolitan division:{code}"
        }
    """

    # Metro divisions — ACS 5-year doesn’t expose these as a direct `for=` geo
    # with these codes (they’re BLS/OMB-oriented codes).
    # For now, we skip them; you still have MSA + counties for DMV coverage.
    if level in ("metro_division", "msd"):
        return None
    
    # If we don’t know how to map this level yet, skip it.
    return None


def census_request(
    year: int,
    dataset: str,
    var_codes: List[str],
    for_param: str,
    in_param: Optional[str] = None,
    api_key: Optional[str] = None,
    retry: int = 3,
    backoff: float = 0.5,
) -> Optional[Dict[str, Any]]:
    base = f"https://api.census.gov/data/{year}/{dataset}"
    params: Dict[str, str] = {
        "get": "NAME," + ",".join(var_codes),
        "for": for_param,
    }
    if in_param:
        params["in"] = in_param
    if api_key:
        params["key"] = api_key

    last_err = None
    for attempt in range(1, retry + 1):
        try:
            r = requests.get(base, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code} from Census")
                time.sleep(backoff * attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if not data or len(data) < 2:
                return None
            headers = data[0]
            row = data[1]
            return dict(zip(headers, row))
        except Exception as e:
            last_err = e
            time.sleep(backoff * attempt)

    print(f"[census] ERROR for year={year}, for={for_param}, in={in_param}: {last_err}")
    return None


# ---------- main ----------


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Bulk ACS Census ingestion based on geo_manifest."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch only a few rows and print sample, no file output.",
    )
    args = parser.parse_args(argv)

    gm = load_geo_manifest_for_census()
    if gm.empty:
        print("[census] nothing to do (no include_census rows).")
        return

    api_key = os.getenv("CENSUS_API_KEY")
    if not api_key:
        print(
            "[census] WARNING: CENSUS_API_KEY not set — small volumes may still work, "
            "but key is recommended."
        )

    rows: List[Dict[str, Any]] = []
    var_codes = list(ACS_VARS.values())
    var_by_code = {v: k for k, v in ACS_VARS.items()}

    total_calls = 0
    skipped = 0

    for row in gm.itertuples():
        geo_id = row.geo_id
        level = (row.level or "").strip().lower()
        census_code = (row.census_code or "").strip()

        geo_params = build_census_geo_params(level, census_code)

        if geo_params is None:
            print(
                f"[census] skipping geo_id={geo_id}, level={level} — "
                f"no valid Census params for census_code={census_code!r}"
            )
            skipped += 1
            continue

        for year in YEARS:
            resp = census_request(
                year=year,
                dataset=CENSUS_DATASET,
                var_codes=var_codes,
                for_param=geo_params["for"],
                in_param=geo_params.get("in"),
                api_key=api_key,
            )
            total_calls += 1

            if not resp:
                print(
                    f"[census] no data for geo_id={geo_id}, "
                    f"level={level}, year={year}"
                )
                continue

            # Convert each requested variable into a metric row
            for var_code in var_codes:
                metric_id = var_by_code[var_code]
                raw_val = resp.get(var_code)
                try:
                    val = (
                        float(raw_val)
                        if raw_val not in (None, "", "null")
                        else None
                    )
                except ValueError:
                    val = None

                rows.append(
                    {
                        "geo_id": geo_id,
                        "level": level,
                        "census_code": census_code,
                        "year": year,
                        "date": f"{year}-12-31",
                        "metric_id": metric_id,
                        "value": val,
                    }
                )

        # For dry-run, bail after a few API calls so you get a quick preview
        if args.dry_run and total_calls >= 5:
            break

    if not rows:
        print("[census] no rows fetched.")
        print(f"[census] skipped geo rows (no valid params): {skipped}")
        return

    df = pd.DataFrame(rows)

    if args.dry_run:
        print("[census] DRY RUN — sample of fetched data:")
        print(df.head(10))
        print(f"[census] total rows (sample): {len(df)}")
        print(f"[census] skipped geo rows (no valid params): {skipped}")
        return

    print(f"[census] total rows: {len(rows)}")
    print(f"[census] skipped geo rows (no valid params): {skipped}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"[census] wrote {len(df)} rows → {OUT_CSV}")
    print("[census] sample:")
    print(df.head(10))

if __name__ == "__main__":
    main()
