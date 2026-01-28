from __future__ import annotations
# sources/census_bps/ingest.py

import argparse
import io
import zipfile
import re
import pandas as pd
import requests

from pathlib import Path
from typing import Optional, List
from urllib.parse import urljoin

# ===================================================================
# CONFIG and CONSTANTS
# ===================================================================
BPS_MASTER_DIR_URL = "https://www2.census.gov/econ/bps/Master%20Data%20Set/"
BPS_COMPILED_RE = re.compile(r"BPS(?:%20|[ _])Compiled_(\d{6})\.zip", re.IGNORECASE)

GEO_MANIFEST = Path("config/geo_manifest.csv")

DEFAULT_ZIP_PATH = Path("data/census/bps_master_latest.zip")
RAW_CSV_PATH = Path("data/census/bps_compiled_raw.csv")
OUT_TIMESERIES_PATH = Path("data/census/census_bps_timeseries.csv")


REQUIRED_COLS = [
    "year", "month", "period", "location_type",
    # geo codes:
    "state_fips", "county_fips", "place_fips", "cbsa_code",
    # measures (examples — adjust once you confirm real headers):
    "units_1", "units_2", "units_3_4", "units_5plus",
    "bldgs_1", "bldgs_2", "bldgs_3_4", "bldgs_5plus",
    "value_1", "value_2", "value_3_4", "value_5plus",
]

RAW_TO_CANON = {
    "year": "year",
    "yr": "year",
    "month": "month",
    "mo": "month",
    "period": "period",
    "location_type": "location_type",
    # geo
    "state": "state_fips",
    "state_fips": "state_fips",
    "county": "county_fips",
    "county_fips": "county_fips",
    "place": "place_fips",
    "place_fips": "place_fips",
    "cbsa": "cbsa_code",
    "cbsa_code": "cbsa_code",
    # etc...
}

# ===================================================================
# HELPERS
# ===================================================================
def discover_latest_compiled_zip_url() -> str:
    """
    Scrape the Census 'Master Data Set' directory listing and return the newest
    BPS Compiled_YYYYMM.zip URL.
    """
    print(f"[bps] discovering latest compiled ZIP from {BPS_MASTER_DIR_URL}")
    r = requests.get(BPS_MASTER_DIR_URL, timeout=60)
    r.raise_for_status()

    html = r.text
    matches = BPS_COMPILED_RE.findall(html)
    if not matches:
        raise SystemExit(
            "[bps] could not find any BPS Compiled_YYYYMM.zip links in Master Data Set directory"
        )

    yyyymm = max(matches)  # lexicographic works for YYYYMM
    # Prefer URL-encoded form to be safe
    fname = f"BPS%20Compiled_{yyyymm}.zip"
    url = urljoin(BPS_MASTER_DIR_URL, fname)

    print(f"[bps] latest compiled ZIP detected: {yyyymm} → {url}")
    return url


def yyyymm_from_compiled_url(url: str) -> str:
    m = re.search(r"Compiled_(\d{6})\.zip", url)
    if not m:
        return "latest"
    return m.group(1)


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    rename = {c: RAW_TO_CANON[c] for c in df.columns if c in RAW_TO_CANON}
    df = df.rename(columns=rename)

    missing = [c for c in ["year","month","period","location_type"] if c not in df.columns]
    if missing:
        raise SystemExit(f"[bps] Missing required columns after mapping: {missing}. "
                         f"Available columns: {sorted(df.columns)[:60]}...")

    return df


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


def filter_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only Monthly period rows (drop Year-to-date & Annual)."""
    df = df.copy()
    if "period" not in df.columns:
        print("[bps] WARNING: period column missing; cannot filter to Monthly")
        return df

    vc = df["period"].value_counts(dropna=False)
    print("[bps] period value counts BEFORE filter:")
    print(vc.head(10))

    p = df["period"].astype(str).str.strip().str.lower()
    monthly_mask = (p == "monthly")

    df_monthly = df[monthly_mask].copy()

    print(f"[bps] Kept {len(df_monthly):,} Monthly rows; "
          f"dropped {len(df) - len(df_monthly):,} non-Monthly rows")

    return df_monthly


def normalize_bps_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonicalize BPS compiled schema to the column names the rest of this pipeline expects.

    Strict behavior:
      - We ONLY accept known aliases per required field.
      - If any required field can't be found via aliases -> hard fail.
    """
    df = df.copy()

    # All columns already lowercased in load_first_csv_from_zip()
    cols = set(df.columns)

    # Canonical -> allowed raw aliases (include the old COLUMN_MAP raw names)
    ALIASES: dict[str, list[str]] = {
        # time
        "year": ["year"],
        "month": ["month"],

        # geo keys
        "state_fips": ["state_fips", "state_code"],
        "county_fips": ["county_fips", "fips_county_5_digits"],
        "place_fips": ["place_fips", "fips_place_code"],
        "cbsa_code": ["cbsa_code"],

        # measures (units)
        "units_1": ["units_1", "units_1_unit"],
        "units_2": ["units_2", "units_2_units"],
        "units_3_4": ["units_3_4", "units_3_4_units"],
        "units_5plus": ["units_5plus", "units_5_units"],

        # measures (buildings)
        "bldgs_1": ["bldgs_1", "bldgs_1_unit"],
        "bldgs_2": ["bldgs_2", "bldgs_2_units"],
        "bldgs_3_4": ["bldgs_3_4", "bldgs_3_4_units"],
        "bldgs_5plus": ["bldgs_5plus", "bldgs_5_units"],

        # measures (value)
        "value_1": ["value_1", "value_1_unit"],
        "value_2": ["value_2", "value_2_units"],
        "value_3_4": ["value_3_4", "value_3_4_units"],
        "value_5plus": ["value_5plus", "value_5_units"],

        # needed for mapping
        "location_type": ["location_type"],
    }

    rename_map: dict[str, str] = {}
    missing: list[str] = []

    for canon, aliases in ALIASES.items():
        found = None
        for a in aliases:
            if a in cols:
                found = a
                break
        if found is None:
            missing.append(canon)
            continue
        if found != canon:
            rename_map[found] = canon

    if missing:
        # print a small diagnostic to help you extend aliases once, correctly
        sample_cols = sorted(list(cols))[:80]
        raise SystemExit(
            "[bps] missing required canonical fields after alias resolution: "
            f"{missing}\n"
            f"[bps] NOTE: first 80 columns in file (lowercased): {sample_cols}"
        )

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def add_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["date"] = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=1),
        errors="coerce",
    )
    return df


def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure numeric types for units / bldgs / value, and compute totals.
    """
    df = df.copy()

    # Units
    for c in ["units_1", "units_2", "units_3_4", "units_5plus"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        else:
            df[c] = 0.0

    if "total_units" not in df.columns:
        df["total_units"] = (
            df["units_1"] + df["units_2"] + df["units_3_4"] + df["units_5plus"]
        )
    else:
        df["total_units"] = pd.to_numeric(df["total_units"], errors="coerce").fillna(0)

    # Buildings
    for c in ["bldgs_1", "bldgs_2", "bldgs_3_4", "bldgs_5plus"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        else:
            df[c] = 0.0

    if "total_bldgs" not in df.columns:
        df["total_bldgs"] = (
            df["bldgs_1"] + df["bldgs_2"] + df["bldgs_3_4"] + df["bldgs_5plus"]
        )
    else:
        df["total_bldgs"] = pd.to_numeric(df["total_bldgs"], errors="coerce").fillna(0)

    # Value ($)
    for c in ["value_1", "value_2", "value_3_4", "value_5plus"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        else:
            df[c] = 0.0

    if "total_value" not in df.columns:
        df["total_value"] = (
            df["value_1"] + df["value_2"] + df["value_3_4"] + df["value_5plus"]
        )
    else:
        df["total_value"] = pd.to_numeric(df["total_value"], errors="coerce").fillna(0)

    return df



def normalize_geo_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize FIPS / CBSA code columns so they match geo_manifest.census_code.
    """
    df = df.copy()

    for col in ["state_fips", "county_fips", "place_fips", "cbsa_code"]:
        if col not in df.columns:
            df[col] = pd.NA

        s = pd.to_numeric(df[col], errors="coerce")
        s = s.round().astype("Int64")
        df[col] = s.astype("string")

    def zp(series: pd.Series, width: int) -> pd.Series:
        return series.where(series.notna(), None).str.zfill(width)

    df["state_fips"] = zp(df["state_fips"], 2)
    df["county_fips"] = zp(df["county_fips"], 5)
    df["place_fips"] = zp(df["place_fips"], 7)
    df["cbsa_code"] = zp(df["cbsa_code"], 5)

    return df

    

def reshape_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Long format with three measure families:
        measure ∈ {"units","bldgs","value"}
        size_band ∈ {"1","2","3_4","5plus","total"}
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
    
    # Keep these so we can deterministically choose “the” row when the source repeats records
    for extra in ["location_type", "survey_date", "number_of_months_rep", "unique_place_id"]:
        if extra in df.columns:
            id_cols.append(extra)
        else:
            if extra == "location_type":
                print("[bps] WARNING: location_type column missing before melt; mapping may fail")


    # Value families
    units_cols = {
        "units_1": "1",
        "units_2": "2",
        "units_3_4": "3_4",
        "units_5plus": "5plus",
        "total_units": "total",
    }
    bldgs_cols = {
        "bldgs_1": "1",
        "bldgs_2": "2",
        "bldgs_3_4": "3_4",
        "bldgs_5plus": "5plus",
        "total_bldgs": "total",
    }
    value_cols = {
        "value_1": "1",
        "value_2": "2",
        "value_3_4": "3_4",
        "value_5plus": "5plus",
        "total_value": "total",
    }

    frames = []

    # Units
    existing_units = [c for c in units_cols if c in df.columns]
    if existing_units:
        u_long = df.melt(
            id_vars=id_cols,
            value_vars=existing_units,
            var_name="col",
            value_name="value",
        )
        u_long["size_band"] = u_long["col"].map(units_cols)
        u_long["measure"] = "units"
        frames.append(u_long)

    # Buildings
    existing_bldgs = [c for c in bldgs_cols if c in df.columns]
    if existing_bldgs:
        b_long = df.melt(
            id_vars=id_cols,
            value_vars=existing_bldgs,
            var_name="col",
            value_name="value",
        )
        b_long["size_band"] = b_long["col"].map(bldgs_cols)
        b_long["measure"] = "bldgs"
        frames.append(b_long)

    # Value ($)
    existing_value = [c for c in value_cols if c in df.columns]
    if existing_value:
        v_long = df.melt(
            id_vars=id_cols,
            value_vars=existing_value,
            var_name="col",
            value_name="value",
        )
        v_long["size_band"] = v_long["col"].map(value_cols)
        v_long["measure"] = "value"
        frames.append(v_long)

    if not frames:
        raise SystemExit("[bps] ERROR: no value columns found to reshape")

    df_long = pd.concat(frames, ignore_index=True)
    df_long = df_long.drop(columns=["col"])

    df_long = df_long[df_long["date"].notna()].copy()
    df_long = df_long[df_long["value"].notna()].copy()

    return df_long


# ===================================================================
# GEO MANIFEST JOIN LOGIC
# ===================================================================
def load_geo_manifest() -> pd.DataFrame:
    gm = pd.read_csv(GEO_MANIFEST, dtype=str)
    gm["include_census_bps"] = gm["include_census_bps"].astype(str).str.strip().isin(
        ["1", "true", "True", "Y", "y"]
    )
    gm = gm[gm["include_census_bps"]]
    gm["level"] = gm["level"].str.lower().str.strip()
    gm["census_code"] = gm["census_code"].str.strip()
    return gm


def map_bps_to_geo(df_long: pd.DataFrame, gm: pd.DataFrame) -> pd.DataFrame:
    """
    Correct mapping using location_type:

      nation     ← location_type == "Country"
      state      ← location_type == "State",  state_fips
      county     ← location_type == "County", county_fips
      city/place ← location_type == "Place",  place_fips
      metro_area ← location_type == "Metro",  cbsa_code
    """
    if "location_type" not in df_long.columns:
        raise SystemExit("[bps] ERROR: location_type missing in df_long; check reshape_long()")

    df = df_long.copy()
    df["geo_id"] = None

    for row in gm.itertuples():
        geo = row.geo_id
        level = row.level
        code = row.census_code

        # 🔹 NEW: national row from BPS
        if level == "nation":
            # BPS has a single national aggregate with location_type == "Country"
            mask = df["location_type"] == "Country"

        elif level == "state":
            mask = (df["location_type"] == "State") & (df["state_fips"] == code)

        elif level == "county":
            mask = (df["location_type"] == "County") & (df["county_fips"] == code)

        elif level == "city":
            mask = (df["location_type"] == "Place") & (df["place_fips"] == code)

        elif level in ("metro_area", "msa", "metro"):
            mask = (df["location_type"] == "Metro") & (df["cbsa_code"] == code)

        else:
            continue  # no BPS coverage for MSD/CSA/etc.

        count = mask.sum()
        if count > 0:
            print(f"[bps] matched {count} authoritative rows for {geo} ({level})")
        df.loc[mask, "geo_id"] = geo

    # after the for-loop, before df = df[df["geo_id"].notna()]
    if df["geo_id"].notna().any():
        # if any row got assigned twice, you'd only see the last assignment; detect overlaps by counting candidates
        pass

    df = df[df["geo_id"].notna()].copy()
    df = df.reset_index(drop=True)
    return df


# ===================================================================
# MAIN
# ===================================================================
def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="BPS ingest with Monthly-only filter and geo mapping")
    parser.add_argument("--url", default=None)

    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--out", default=str(OUT_TIMESERIES_PATH))
    args = parser.parse_args(argv)

    url = args.url or discover_latest_compiled_zip_url()
    yyyymm = yyyymm_from_compiled_url(url)
    
    # Cache per-month so “Using cached ZIP” can’t pin you to an old month forever.
    zip_dest = Path(f"data/census/bps_compiled_{yyyymm}.zip")
    
    zip_path = download_file(url, zip_dest, overwrite=args.force_download)


    df_raw = load_first_csv_from_zip(zip_path)

    # Save full compiled CSV (unfiltered) for inspection
    RAW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_raw.to_csv(RAW_CSV_PATH, index=False)
    print(f"[bps] Wrote raw compiled CSV: {RAW_CSV_PATH} ({len(df_raw):,} rows)")

    # 🔑 Filter to Monthly only
    df_raw = filter_monthly(df_raw)
    df_raw = canonicalize_columns(df_raw)

    # Normalize + reshape
    df = normalize_bps_schema(df_raw)

    df = add_date(df)

    if "survey_date" in df.columns:
        df["survey_date"] = pd.to_datetime(df["survey_date"], errors="coerce")
    
    if "number_of_months_rep" in df.columns:
        df["number_of_months_rep"] = pd.to_numeric(df["number_of_months_rep"], errors="coerce")

    #df = compute_total_units(df)
    df = compute_aggregates(df)
    df = normalize_geo_keys(df)
    df_long = reshape_long(df)

    # Map to geo_manifest
    gm = load_geo_manifest()
    df_geo = map_bps_to_geo(df_long, gm)

    # Deterministic collapse: if the compiled file repeats the same logical observation,
    # pick the “best” row (prefer latest survey_date; then highest number_of_months_rep).
    pk = ["geo_id", "date", "measure", "size_band"]
    
    sort_cols = pk.copy()
    if "survey_date" in df_geo.columns:
        sort_cols.append("survey_date")
    if "number_of_months_rep" in df_geo.columns:
        sort_cols.append("number_of_months_rep")
    
    before = len(df_geo)
    
    # sort so “best” row is last for drop_duplicates keep="last"
    df_geo = df_geo.sort_values(sort_cols)
    df_geo = df_geo.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    
    dropped = before - len(df_geo)
    if dropped:
        print(f"[bps] dropped {dropped} duplicate rows on logical PK {pk} (kept best by survey_date/number_of_months_rep)")


    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_geo.to_csv(out_path, index=False)

    print(f"[bps] Final mapped rows: {len(df_geo):,}")
    print(f"[bps] Wrote → {out_path}")


if __name__ == "__main__":
    main()
