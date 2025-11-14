# ingest/ces_api_bulk.py
import os, csv, time, json
from pathlib import Path
from datetime import date
import csv

import requests
import pandas as pd
import duckdb

from typing import Dict, Optional
from ingest.census_geo_map import load_census_geo_map

GEN_PATH = Path("config/ces_series.generated.csv")
DB_PATH  = os.getenv("DUCKDB_PATH", "./data/market.duckdb")

CENSUS_BASE = "https://api.census.gov/data"
OUT_CSV = Path("data/census_raw.csv")

BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_KEY = (os.getenv("BLS_API_KEY") or "").strip()

# Optional: handy filter while debugging, comma-sep geo_id list
FILTER_GEOS = set(
    g.strip().lower()
    for g in (os.getenv("CES_FILTER_GEOS", "").split(",")
              if os.getenv("CES_FILTER_GEOS") else [])
)



def fetch_census_acs(
    dataset: str,
    year: int,
    variables: list[str],
    for_param: str,
    in_param: str | None,
    api_key: str | None = None,
) -> list[list[str]]:
    """
    Fetch one Census ACS endpoint for a single geography.
    Returns list-of-rows (including header).
    """
    params = {
        "get": ",".join(variables + ["NAME"]),
        "for": for_param,
    }
    if in_param:
        params["in"] = in_param
    if api_key:
        params["key"] = api_key

    url = f"{CENSUS_BASE}/{year}/{dataset}"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()



def build_census_geo_query(geo_info: Dict[str, str]) -> Dict[str, Optional[str]]:
    """
    Given one row from load_census_geo_map(), return the 'for' and 'in'
    parameter pieces for Census API.

    Assumes:
      - 'level'     is one of: state, county, place, msa, cbsa, metro, csa
      - 'census_code' is:
          state:  SS
          county: SSCCC
          place:  SSPPPPP
          msa/cbsa/metro: CCCCC (CBSA)
          csa:   CCCCC (CSA)

    Returns dict with keys:
      - "for_": e.g. "state:11" or "county:001"
      - "in":   e.g. "state:11" or None
      - "geo_type": Census geo type string (for debugging/logging)
    """
    level = geo_info["level"].lower()
    code  = geo_info["census_code"]

    if level == "state":
        return {
            "for_": f"state:{code}",
            "in": None,
            "geo_type": "state",
        }

    if level == "county":
        if len(code) < 5:
            raise ValueError(f"[census:geo] county code '{code}' must be SSCCC")
        st, ct = code[:2], code[2:]
        return {
            "for_": f"county:{ct}",
            "in": f"state:{st}",
            "geo_type": "county",
        }

    if level in {"place", "city"}:
        # Assume SSPPPPP: 2-digit state + 5-digit place
        if len(code) < 7:
            raise ValueError(f"[census:geo] place code '{code}' must be SSPPPPP")
        st, pl = code[:2], code[2:]
        return {
            "for_": f"place:{pl}",
            "in": f"state:{st}",
            "geo_type": "place",
        }

    if level in {"msa", "cbsa", "metro"}:
        # ACS uses "metropolitan statistical area/micropolitan statistical area"
        return {
            "for_": f"metropolitan statistical area/micropolitan statistical area:{code}",
            "in": None,
            "geo_type": "cbsa",
        }

    if level == "csa":
        return {
            "for_": f"combined statistical area:{code}",
            "in": None,
            "geo_type": "csa",
        }

    raise ValueError(f"[census:geo] unsupported level '{level}' for code '{code}'")


# ---------- helpers ----------

def seasonal_suffix_from_sid(series_id: str) -> str:
    """SMS* => SA, SMU* => NSA."""
    s = (series_id or "").upper().strip()
    if s.startswith("SMS"):
        return "sa"
    if s.startswith("SMU"):
        return "nsa"
    # Default conservative
    return "nsa"

def metric_id_from_row(seasonal_tag: str) -> str:
    """Only one CES base metric in this phase: total nonfarm, all employees."""
    base = "ces_total_nonfarm"
    sfx  = "_sa" if (seasonal_tag or "").lower() == "sa" else "_nsa"
    return base + sfx

def ensure_dims(con: duckdb.DuckDBPyConnection, metric_ids: list[str]):
    # Source (idempotent)
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_source(
      source_id TEXT PRIMARY KEY, name TEXT, url TEXT, cadence TEXT, license TEXT
    );
    INSERT INTO dim_source(source_id, name, url, cadence, license)
    SELECT 'ces','BLS Current Employment Statistics',
           'https://www.bls.gov/ces/','monthly','public'
    WHERE NOT EXISTS (SELECT 1 FROM dim_source WHERE source_id='ces');
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_metric(
      metric_id TEXT PRIMARY KEY, name TEXT, frequency TEXT, unit TEXT, category TEXT
    );
    """)

    # CES metrics we expect
    meta = {
        "ces_total_nonfarm_sa":  ("Total Nonfarm Employment", "monthly", "persons", "labor"),
        "ces_total_nonfarm_nsa": ("Total Nonfarm Employment", "monthly", "persons", "labor"),
    }

    for mid in set(metric_ids):
        name, freq, unit, cat = meta.get(mid, ("CES Series", "monthly", "value", "labor"))
        con.execute("""
        INSERT INTO dim_metric(metric_id, name, frequency, unit, category)
        SELECT ?,?,?,?,?
        WHERE NOT EXISTS (SELECT 1 FROM dim_metric WHERE metric_id=?)
        """, [mid, name, freq, unit, cat, mid])

def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    if df.empty:
        return

    # ensure fact table
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_timeseries(
      geo_id TEXT NOT NULL,
      metric_id TEXT NOT NULL,
      date DATE NOT NULL,
      property_type_id TEXT NOT NULL DEFAULT 'all',
      value DOUBLE,
      source_id TEXT,
      PRIMARY KEY (geo_id, metric_id, date, property_type_id)
    );
    """)
    # dedupe
    df = (df.sort_values(["geo_id","metric_id","date","property_type_id"])
            .drop_duplicates(subset=["geo_id","metric_id","date","property_type_id"], keep="last"))

    con.register("df_stage", df[["geo_id","metric_id","date","property_type_id","value","source_id"]])
    con.execute("""
    DELETE FROM fact_timeseries AS f
    WHERE EXISTS (
      SELECT 1 FROM df_stage s
      WHERE s.geo_id=f.geo_id AND s.metric_id=f.metric_id
        AND s.date=f.date AND s.property_type_id=f.property_type_id
    )
    """)
    con.execute("""
    INSERT INTO fact_timeseries(geo_id,metric_id,date,property_type_id,value,source_id)
    SELECT geo_id,metric_id,date,property_type_id,CAST(value AS DOUBLE),source_id
    FROM df_stage
    """)



def fetch_series_window(series_ids: list[str], y1: int, y2: int) -> list[dict]:
    """Fetch a single ≤20-year window for the given series IDs."""
    payload = {
        "seriesid": series_ids,
        "startyear": str(y1),
        "endyear": str(y2),
        "annualaverage": True,
    }
    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY
        print(f"[ces] using BLS key: yes (len={len(BLS_KEY)})")
    else:
        print("[ces] using BLS key: no (public quota)")

    r = requests.post(BLS_API, json=payload, timeout=60)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS error: {j}")
    return j["Results"]["series"]


def year_windows(start_year: int, end_year: int, span: int = 20):
    y = start_year
    while y <= end_year:
        yield (y, min(y + span - 1, end_year))
        y += span



def to_df(series_block: list[dict], sid_to_meta: dict) -> pd.DataFrame:
    rows = []
    for s in series_block:
        sid = s.get("seriesID")
        meta = sid_to_meta.get(sid, {})
        # count real monthly rows per year to decide on keeping/dropping M13
        months_by_year = {}
        for d in s.get("data", []):
            p = str(d.get("period",""))
            if p.startswith("M") and p != "M13":
                y = int(d["year"])
                months_by_year[y] = months_by_year.get(y, 0) + 1

        for d in s.get("data", []):
            p = str(d.get("period",""))
            if not p.startswith("M"):
                continue

            y = int(d["year"])

            # drop annual M13 if monthly exists that year
            if p == "M13":
                if months_by_year.get(y, 0) > 0:
                    continue
                dt = pd.Timestamp(year=y, month=12, day=31).date()
            else:
                m = int(p[1:])
                if not (1 <= m <= 12):
                    continue
                dt = (pd.Timestamp(year=y, month=m, day=1)
                        .to_period("M").to_timestamp("M").date())

            try:
                val = float(d["value"])
            except Exception:
                continue

            rows.append({
                "geo_id":           meta.get("geo_id"),
                "metric_id":        meta.get("metric_id"),
                "date":             dt,
                "value":            val,
                "source_id":        "ces",
                "property_type_id": "all",
                "series_id":        sid,
            })
    return pd.DataFrame(rows)

# ---------- main ----------


def main():
    api_key = os.getenv("CENSUS_API_KEY")
    census_geos = load_census_geo_map()

    # Example: ACS 5-year, total population + median household income
    dataset = "acs/acs5"
    years = [2010, 2020, 2023]
    variables = ["B01003_001E", "B19013_001E"]  # pop total, median hh income

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        header_written = False

        for geo_id, info in census_geos.items():
            q = build_census_geo_query(info)

            for year in years:
                rows = fetch_census_acs(
                    dataset=dataset,
                    year=year,
                    variables=variables,
                    for_param=q["for_"],
                    in_param=q["in"],
                    api_key=api_key,
                )

                if not header_written:
                    writer.writerow(["geo_id", "year"] + rows[0])
                    header_written = True

                for row in rows[1:]:
                    writer.writerow([geo_id, year] + row)

                print(f"[census] {year} {geo_id} ({q['geo_type']}) OK")

    print(f"[census] wrote raw ACS rows → {OUT_CSV}")

if __name__ == "__main__":
    main()
