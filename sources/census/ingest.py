from __future__ import annotations
# sources/census/ingest.py

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Iterable, Tuple

import pandas as pd
import requests


# ---------- Geo mapping: (geo_level, census_code) -> for/in clauses ----------

def build_census_for_in(geo_level: str, census_code: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Convert our manifest geo identifiers into Census API `for` / `in` parameters.
    Returns (for_clause, in_clause_or_None).
    """
    level = (geo_level or "").strip().lower()
    code = (census_code or "").strip()

    if not code:
        return None

    # National total
    if level in ("nation", "national", "us"):
        return ("us:1", None)

    # States: code = 2-digit FIPS
    if level in ("state", "state_equiv"):
        return (f"state:{code}", None)

    # Counties: code = 5-digit SSCCC
    if level in ("county", "county_equiv", "independent_city"):
        if len(code) != 5:
            return None
        state_fips = code[:2]
        county_fips = code[2:]
        return (f"county:{county_fips}", f"state:{state_fips}")

    # Places: code = 7-digit SSPPPPP
    if level in ("city", "place"):
        if len(code) != 7:
            return None
        state_fips = code[:2]
        place_fips = code[2:]
        return (f"place:{place_fips}", f"state:{state_fips}")

    # MSA/micro: 5-digit code
    if level in ("msa", "metro_area", "metro"):
        return (f"metropolitan statistical area/micropolitan statistical area:{code}", None)

    # CSA/MSD: intentionally unsupported for now
    if level in ("csa", "combined_area", "metro_division", "msd"):
        return None

    return None


# ---------- HTTP ----------

def census_request(
    *,
    year: int,
    dataset: str,
    var_codes: List[str],
    for_clause: str,
    in_clause: Optional[str] = None,
    api_key: Optional[str] = None,
    retry: int = 3,
    backoff: float = 0.5,
    timeout_s: int = 30,
    session: Optional[requests.Session] = None,
) -> Optional[Dict[str, Any]]:
    base = f"https://api.census.gov/data/{year}/{dataset}"
    params: Dict[str, str] = {
        "get": "NAME," + ",".join(var_codes),
        "for": for_clause,
    }
    if in_clause:
        params["in"] = in_clause
    if api_key:
        params["key"] = api_key

    sess = session or requests.Session()

    last_err: Optional[Exception] = None
    for attempt in range(1, retry + 1):
        try:
            r = sess.get(base, params=params, timeout=timeout_s)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code} from Census")
                time.sleep(backoff * attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if not data or len(data) < 2:
                return None

            headers = data[0]
            row = data[1]  # single-geo query => single row expected
            return dict(zip(headers, row))
        except Exception as e:
            last_err = e
            time.sleep(backoff * attempt)

    print(f"[census] ERROR year={year} for={for_clause} in={in_clause}: {last_err}")
    return None


# ---------- Main ingestion entrypoint ----------

def ingest_from_query_plan(
    *,
    plan_df: pd.DataFrame,
    years: List[int],
    api_key: Optional[str] = None,
    retry: int = 3,
    backoff: float = 0.5,
) -> pd.DataFrame:
    """
    Execute the expanded query plan and return raw/staged rows.

    Required columns in plan_df:
      - dataset
      - vintage
      - geo_id
      - geo_level
      - census_code
      - variables_csv

    Returns raw df with:
      - source_id, dataset, vintage
      - geo_id, geo_level, census_code
      - year, date (YYYY-12-31)
      - census_name
      - variable_code
      - value (float or None)
    """
    needed = {"dataset", "vintage", "geo_id", "geo_level", "census_code", "variables_csv"}
    missing = needed - set(plan_df.columns)
    if missing:
        raise ValueError(f"[census:ingest] plan_df missing columns: {sorted(missing)}")

    if api_key is None:
        api_key = os.getenv("CENSUS_API_KEY")
        if not api_key:
            print("[census] WARNING: CENSUS_API_KEY not set (may hit limits).")

    rows: List[Dict[str, Any]] = []
    sess = requests.Session()

    skipped = 0
    total_calls = 0

    for rec in plan_df.to_dict(orient="records"):
        geo_id = rec["geo_id"]
        geo_level = rec["geo_level"]
        census_code = rec["census_code"]
        dataset = rec["dataset"]
        vintage = int(rec["vintage"])
        var_codes = [v for v in (rec["variables_csv"] or "").split(",") if v.strip()]

        for_in = build_census_for_in(geo_level, census_code)
        if for_in is None:
            print(f"[census] skipping geo_id={geo_id} level={geo_level} code={census_code!r} (no for/in mapping)")
            skipped += 1
            continue

        for_clause, in_clause = for_in

        for year in years:
            resp = census_request(
                year=year,
                dataset=dataset,
                var_codes=var_codes,
                for_clause=for_clause,
                in_clause=in_clause,
                api_key=api_key,
                retry=retry,
                backoff=backoff,
                session=sess,
            )
            total_calls += 1

            if not resp:
                continue

            census_name = resp.get("NAME")

            for var_code in var_codes:
                raw_val = resp.get(var_code)
                try:
                    val = float(raw_val) if raw_val not in (None, "", "null") else None
                except Exception:
                    val = None

                rows.append(
                    {
                        "source_id": "census",
                        "dataset": dataset,
                        "vintage": vintage,
                        "geo_id": geo_id,
                        "geo_level": geo_level,
                        "census_code": census_code,
                        "year": int(year),
                        "date": f"{year}-12-31",
                        "census_name": census_name,
                        "variable_code": var_code,
                        "value": val,
                    }
                )

    df = pd.DataFrame(rows)
    print(f"[census:ingest] calls={total_calls} rows={len(df)} skipped_geos={skipped}")
    return df
