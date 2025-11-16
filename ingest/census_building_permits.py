# ingest/census_building_permits.py
from __future__ import annotations

import argparse
import io
import zipfile
from pathlib import Path
from typing import Optional, List

import pandas as pd
import requests

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

GEO_MANIFEST = Path("config/geo_manifest.csv")

BPS_MASTER_URL_DEFAULT = (
    "https://www2.census.gov/econ/bps/Master%20Data%20Set/BPS%20Compiled_202508.zip"
)

DEFAULT_ZIP_PATH = Path("data/census/bps_master_latest.zip")
RAW_CSV_PATH = Path("data/census/bps_compiled_raw.csv")
OUT_TIMESERIES_PATH = Path("data/census/census_bps_timeseries.csv")

# Column mapping for BPS
COLUMN_MAP = {
    # time
    "year": "year",
    "month": "month",

    # geography keys
    # state FIPS (2-digit)
    "state_fips": "state_code",
    # county FIPS (5-digit combined)
    "county_fips": "fips_county_5_digits",
    # place FIPS (should be 7-digit, but here it's the place code;
    # we still treat it as our place key)
    "place_fips": "fips_place_code",
    # CBSA / MSA
    "cbsa_code": "cbsa_code",

    # units by structure size
    "units_1": "units_1_unit",
    "units_2": "units_2_units",
    "units_3_4": "units_3_4_units",
    "units_5plus": "units_5_units",
}


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------


def download_file(url: str, dest: Path, overwrite: bool = False) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not overwrite:
        print(f"[bps] Using cached ZIP {dest}")
        return dest

    print(f"[bps] Downloading {url}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
    print(f"[bps] Downloaded ZIP → {dest}")
    return dest


def load_first_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise SystemExit("[bps] No CSV found in ZIP")

        name = sorted(names)[0]
        print(f"[bps] Extracting CSV: {name}")
        with zf.open(name, "r") as f:
            text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
            df = pd.read_csv(text, low_memory=False)

    df.columns = [c.strip().lower() for c in df.columns]
    return df


def apply_column_map(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for logical, raw in COLUMN_MAP.items():
        if raw not in df.columns:
            print(f"[bps] WARNING missing column {raw} → filling NA")
            df[raw] = pd.NA
    df = df.rename(columns={raw: logical for logical, raw in COLUMN_MAP.items()})
    return df


def add_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["date"] = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=1),
        errors="coerce"
    )
    return df


def compute_total_units(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["units_1", "units_2", "units_3_4", "units_5plus"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["total_units"] = (
        df["units_1"] +
        df["units_2"] +
        df["units_3_4"] +
        df["units_5plus"]
    )
    return df


def normalize_geo_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize FIPS / CBSA code columns so they match geo_manifest.census_code.

    Strategy:
    - Coerce to numeric (handles 11.0, 24031.0, etc.).
    - Round and cast to Int64 (preserve NA).
    - Convert to string.
    - Zero-pad to standard widths:
        state_fips  -> 2
        county_fips -> 5
        place_fips  -> 7
        cbsa_code   -> 5
    """
    df = df.copy()

    for col in ["state_fips", "county_fips", "place_fips", "cbsa_code"]:
        if col not in df.columns:
            df[col] = pd.NA

        # Coerce to numeric, then to Int64 to keep NA
        s = pd.to_numeric(df[col], errors="coerce")
        s = s.round().astype("Int64")
        df[col] = s.astype("string")

    def zp(series: pd.Series, width: int) -> pd.Series:
        return series.where(series.notna(), None).str.zfill(width)

    df["state_fips"]  = zp(df["state_fips"], 2)
    df["county_fips"] = zp(df["county_fips"], 5)
    df["place_fips"]  = zp(df["place_fips"], 7)
    df["cbsa_code"]   = zp(df["cbsa_code"], 5)

    return df


"""
def reshape_long(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    id_cols = [
        "date", "year", "month",
        "state_fips", "county_fips", "place_fips", "cbsa_code"
    ]
    value_cols = {
        "units_1": "1",
        "units_2": "2",
        "units_3_4": "3_4",
        "units_5plus": "5plus",
        "total_units": "total",
    }

    df_long = df.melt(
        id_vars=id_cols,
        value_vars=list(value_cols.keys()),
        var_name="unit_col",
        value_name="units"
    )
    df_long["unit_size_band"] = df_long["unit_col"].map(value_cols)
    df_long = df_long.drop(columns=["unit_col"])
    df_long = df_long[df_long["date"].notna()].copy()
    df_long = df_long[df_long["units"].notna()].copy()
    return df_long
"""

def reshape_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert unit counts to long format by 'unit_size_band', e.g.:

        unit_size_band ∈ ["1", "2", "3_4", "5plus", "total"]
        units          = count

    Keeps `location_type` so we can correctly map to geo_manifest.
    """
    df = df.copy()

    id_cols = [
        "date",
        "year",
        "month",
        "state_fips",
        "county_fips",
        "place_fips",
        "cbsa_code",
    ]

    # 🔑 This is the piece that was missing:
    if "location_type" in df.columns:
        id_cols.append("location_type")
    else:
        print("[bps] WARNING: location_type column missing before melt; mapping will fail")

    value_cols = {
        "units_1": "1",
        "units_2": "2",
        "units_3_4": "3_4",
        "units_5plus": "5plus",
        "total_units": "total",
    }
    existing_value_cols = [c for c in value_cols.keys() if c in df.columns]

    df_long = df.melt(
        id_vars=id_cols,
        value_vars=existing_value_cols,
        var_name="unit_col",
        value_name="units",
    )
    df_long["unit_size_band"] = df_long["unit_col"].map(value_cols)
    df_long = df_long.drop(columns=["unit_col"])

    df_long = df_long[df_long["date"].notna()].copy()
    df_long = df_long[df_long["units"].notna()].copy()

    return df_long


# -------------------------------------------------------------------
# GEO MANIFEST JOIN LOGIC
# -------------------------------------------------------------------

def load_geo_manifest() -> pd.DataFrame:
    gm = pd.read_csv(GEO_MANIFEST, dtype=str)
    gm["include_census"] = gm["include_census"].astype(str).str.strip().isin(["1", "true", "True", "Y", "y"])
    gm = gm[gm["include_census"]]
    gm["level"] = gm["level"].str.lower().str.strip()
    gm["census_code"] = gm["census_code"].str.strip()
    return gm


"""
def map_bps_to_geo(df_long: pd.DataFrame, gm: pd.DataFrame) -> pd.DataFrame:
    
    #Map BPS rows to geo_manifest rows via census_code depending on geo level:
    #  state      → state_fips
    #  county     → county_fips
    #  city/place → place_fips
    #  metro_area → cbsa_code
    
    df = df_long.copy()
    df["geo_id"] = None  # placeholder

    for row in gm.itertuples():
        geo = row.geo_id
        level = row.level
        code = row.census_code

        if level == "state":
            mask = df["state_fips"] == code
        elif level == "county":
            mask = df["county_fips"] == code
        elif level == "city":
            mask = df["place_fips"] == code
        elif level in ("metro_area", "msa", "metro"):
            mask = df["cbsa_code"] == code
        else:
            # MSD / CSA / others → no BPS coverage
            continue

        count = mask.sum()
        if count > 0:
            print(f"[bps] matched {count} rows for geo_id={geo}, level={level}, census_code={code}")
        df.loc[mask, "geo_id"] = geo

    df = df[df["geo_id"].notna()].copy()
    return df
"""


def map_bps_to_geo(df_long: pd.DataFrame, gm: pd.DataFrame) -> pd.DataFrame:
    """
    Correct mapping of BPS rows to geo_manifest geos using location_type.

    - Only match rows whose `location_type` matches the geo's true level.
    - Prevents duplication and stops incorrect cascading matches.
    """

    if "location_type" not in df_long.columns:
        raise SystemExit("[bps] ERROR: location_type missing in df_long; check reshape_long()")
    
    df = df_long.copy()
    df["geo_id"] = None

    for row in gm.itertuples():
        geo = row.geo_id
        level = row.level
        code = row.census_code

        if level == "state":
            # Match ONLY state rows
            mask = (df["location_type"] == "State") & (df["state_fips"] == code)

        elif level == "county":
            # Match ONLY county rows
            mask = (df["location_type"] == "County") & (df["county_fips"] == code)

        elif level == "city":
            # Match ONLY place rows
            mask = (df["location_type"] == "Place") & (df["place_fips"] == code)

        elif level in ("metro_area", "msa", "metro"):
            # Match ONLY metro (CBSA) rows
            mask = (df["location_type"] == "Metro") & (df["cbsa_code"] == code)

        else:
            # No BPS coverage for: metro_division, csa, region, country, micro
            continue

        count = mask.sum()
        if count > 0:
            print(f"[bps] matched {count} authoritative rows for {geo} ({level})")
        df.loc[mask, "geo_id"] = geo

    # Keep only rows successfully mapped
    df = df[df["geo_id"].notna()].copy()
    df = df.reset_index(drop=True)
    return df


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="BPS ingest with geo mapping")
    parser.add_argument("--url", default=BPS_MASTER_URL_DEFAULT)
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--out", default=str(OUT_TIMESERIES_PATH))
    args = parser.parse_args(argv)

    zip_path = download_file(args.url, DEFAULT_ZIP_PATH, overwrite=args.force_download)
    df_raw = load_first_csv_from_zip(zip_path)
    df_raw.to_csv(RAW_CSV_PATH, index=False)

    df = apply_column_map(df_raw)
    df = add_date(df)
    df = compute_total_units(df)
    df = normalize_geo_keys(df)
    df_long = reshape_long(df)

    gm = load_geo_manifest()
    df_geo = map_bps_to_geo(df_long, gm)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_geo.to_csv(out_path, index=False)

    print(f"[bps] Final mapped rows: {len(df_geo):,}")
    print(f"[bps] Wrote → {out_path}")


if __name__ == "__main__":
    main()
