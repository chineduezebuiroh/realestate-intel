from __future__ import annotations
# sources/census_bps_provisional/ingest.py

import argparse
import io
import re
import zipfile
from pathlib import Path
from typing import Optional, List
from urllib.parse import urljoin

import pandas as pd
import requests

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
# You MUST verify this base URL matches what you want to treat as "provisional".
# We'll harden discovery once you confirm the directory contents on your machine.
BPS_PROV_DIR_URL = "https://www2.census.gov/econ/bps/"

GEO_MANIFEST = Path("config/geo_manifest.generated.csv")

RAW_PATH = Path("data/census/bps_provisional_raw.csv")
OUT_TIMESERIES_PATH = Path("data/census/census_bps_provisional_timeseries.csv")

# Provisional discovery: try to find a "latest" monthly zip/csv link.
# This is intentionally conservative: it fails loudly if it can't discover.
PROV_ZIP_RE = re.compile(r'href="([^"]+\.zip)"', re.IGNORECASE)
PROV_CSV_RE = re.compile(r'href="([^"]+\.csv)"', re.IGNORECASE)

# Columns that we *expect* to exist after canonicalization.
REQUIRED_CANON = [
    "year", "month", "period", "location_type",
    "state_fips", #"county_fips", "place_fips", "cbsa_code",
    "units_1", "units_2", "units_3_4", "units_5plus",
    "bldgs_1", "bldgs_2", "bldgs_3_4", "bldgs_5plus",
    "value_1", "value_2", "value_3_4", "value_5plus",
]

# Canonical -> allowed raw aliases (extend once you see actual provisional headers)
ALIASES: dict[str, list[str]] = {
    "year": ["year", "yr"],
    "month": ["month", "mo"],
    "period": ["period"],  # if provisional doesn't have period, we can relax later
    "location_type": ["location_type", "location"],

    "state_fips": ["state_fips", "state_code", "state"],
    "county_fips": ["county_fips", "fips_county_5_digits", "county_code", "county"],
    "place_fips": ["place_fips", "fips_place_code", "census_place_code", "place"],
    "cbsa_code": ["cbsa_code", "cbsa"],

    "units_1": ["units_1", "units_1_unit"],
    "units_2": ["units_2", "units_2_units"],
    "units_3_4": ["units_3_4", "units_3_4_units"],
    "units_5plus": ["units_5plus", "units_5_units"],

    "bldgs_1": ["bldgs_1", "bldgs_1_unit"],
    "bldgs_2": ["bldgs_2", "bldgs_2_units"],
    "bldgs_3_4": ["bldgs_3_4", "bldgs_3_4_units"],
    "bldgs_5plus": ["bldgs_5plus", "bldgs_5_units"],

    "value_1": ["value_1", "value_1_unit"],
    "value_2": ["value_2", "value_2_units"],
    "value_3_4": ["value_3_4", "value_3_4_units"],
    "value_5plus": ["value_5plus", "value_5_units"],
}

MEASURE_COLUMNS = [
    "bldgs_1", "units_1", "value_1",
    "bldgs_2", "units_2", "value_2",
    "bldgs_3_4", "units_3_4", "value_3_4",
    "bldgs_5plus", "units_5plus", "value_5plus",
    "bldgs_1_rep", "units_1_rep", "value_1_rep",
    "bldgs_2_rep", "units_2_rep", "value_2_rep",
    "bldgs_3_4_rep", "units_3_4_rep", "value_3_4_rep",
    "bldgs_5plus_rep", "units_5plus_rep", "value_5plus_rep",
]

PROVISIONAL_COLUMNS_BY_LEVEL = {
    "state": [
        "survey_date", "state_fips", "region_code", "division_code", "geo_name",
        *MEASURE_COLUMNS,
    ],
    "county": [
        "survey_date", "state_fips", "county_fips_3", "region_code", "division_code", "geo_name",
        *MEASURE_COLUMNS,
    ],
    "cbsa_metro": [
        "survey_date", "csa_code", "cbsa_code", "hheader", "geo_name",
        *MEASURE_COLUMNS,
    ],
}

PROVISIONAL_LEVELS = {
    "state": {
        "url": "https://www2.census.gov/econ/bps/State/",
        "prefix": "st",
    },
    "county": {
        "url": "https://www2.census.gov/econ/bps/County/",
        "prefix": "co",
    },
    "cbsa_metro": {
        "url": "https://www2.census.gov/econ/bps/CBSA%20%28beginning%20Jan%202024%29/",
        "prefix": "cbsa",
    },
}

CURRENT_FILE_RE_TEMPLATE = r'href="({prefix}(\d{{4}})c\.txt)"'

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def http_get_text(url: str) -> str:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.text


def yymm_sort_key(yymm: str) -> tuple[int, int]:
    yy = int(yymm[:2])
    mm = int(yymm[2:])
    year = 2000 + yy if yy <= 79 else 1900 + yy
    return year, mm


def discover_latest_provisional_urls() -> dict[str, str]:
    urls: dict[str, str] = {}

    for level, cfg in PROVISIONAL_LEVELS.items():
        base_url = cfg["url"]
        prefix = cfg["prefix"]

        print(f"[bps:prov] discovering {level} provisional from {base_url}")
        html = http_get_text(base_url)

        pat = re.compile(
            CURRENT_FILE_RE_TEMPLATE.format(prefix=re.escape(prefix)),
            re.IGNORECASE,
        )
        matches = pat.findall(html)

        if not matches:
            raise SystemExit(
                f"[bps:prov] could not discover current-month {level} provisional file "
                f"with prefix={prefix} at {base_url}"
            )

        fname, yymm = max(matches, key=lambda x: yymm_sort_key(x[1]))
        url = urljoin(base_url, fname)

        print(f"[bps:prov] latest {level} provisional: {yymm} → {url}")
        urls[level] = url

    return urls


def download(url: str, dest: Path, overwrite: bool = False) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not overwrite:
        print(f"[bps:prov] Using cached file {dest}")
        return dest

    print(f"[bps:prov] Downloading {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)

    print(f"[bps:prov] Downloaded → {dest}")
    return dest


def read_csv_from_maybe_zip(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise SystemExit("[bps:prov] zip has no csv")
            name = sorted(names)[0]
            print(f"[bps:prov] Extracting CSV: {name}")
            with zf.open(name, "r") as f:
                text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                df = pd.read_csv(text, low_memory=False)
    else:
        df = pd.read_csv(path, low_memory=False)

    df.columns = [c.strip().lower() for c in df.columns]
    return df


def read_provisional_txt(path: Path, level: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            path,
            sep=",",
            header=None,
            skiprows=3,
            names=PROVISIONAL_COLUMNS_BY_LEVEL[level],
            dtype=str,
            engine="python",
            encoding="utf-8",
        )
    except UnicodeDecodeError:
        df = pd.read_csv(
            path,
            sep=",",
            header=None,
            skiprows=3,
            names=PROVISIONAL_COLUMNS_BY_LEVEL[level],
            dtype=str,
            engine="python",
            encoding="latin1",
        )

    df = df.dropna(how="all").copy()

    if level == "county":
        df["county_fips"] = (
            df["state_fips"].astype(str).str.zfill(2)
            + df["county_fips_3"].astype(str).str.zfill(3)
        )
    elif level == "cbsa_metro":
        df["state_fips"] = ""
        df["county_fips"] = ""
    else:
        df["county_fips"] = ""
    
    df["place_fips"] = ""

    df["provisional_level"] = level
    df["period"] = "Monthly"
    df["location_type"] = {
        "state": "State",
        "county": "County",
        "cbsa_metro": "Metro",
    }[level]

    df["year"] = df["survey_date"].str.slice(0, 4)
    df["month"] = df["survey_date"].str.slice(4, 6)

    return df


def normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cols = set(df.columns)
    rename_map: dict[str, str] = {}
    missing: list[str] = []

    for canon, aliases in ALIASES.items():
        found = None
        for a in aliases:
            if a in cols:
                found = a
                break
        if found is None:
            if canon in REQUIRED_CANON:
                missing.append(canon)
            continue
        if found != canon:
            rename_map[found] = canon

    if missing:
        sample_cols = sorted(list(cols))[:120]
        raise SystemExit(
            f"[bps:prov] missing required fields after alias resolution: {missing}\n"
            f"[bps:prov] first 120 cols: {sample_cols}"
        )

    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def filter_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Provisional may or may not have 'period'. If it does, keep only Monthly.
    If it doesn't, we proceed (but log) — we'll harden once confirmed.
    """
    df = df.copy()
    if "period" not in df.columns:
        print("[bps:prov][warn] period column missing; cannot filter to Monthly")
        return df

    vc = df["period"].value_counts(dropna=False)
    print("[bps:prov] period value counts BEFORE filter:")
    print(vc.head(10))

    p = df["period"].astype(str).str.strip().str.lower()
    out = df[p == "monthly"].copy()
    print(f"[bps:prov] kept {len(out):,} Monthly rows; dropped {len(df)-len(out):,}")
    return out


def add_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1), errors="coerce")
    return df


def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["units_1", "units_2", "units_3_4", "units_5plus",
              "bldgs_1", "bldgs_2", "bldgs_3_4", "bldgs_5plus",
              "value_1", "value_2", "value_3_4", "value_5plus"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["total_units"] = df["units_1"] + df["units_2"] + df["units_3_4"] + df["units_5plus"]
    df["total_bldgs"] = df["bldgs_1"] + df["bldgs_2"] + df["bldgs_3_4"] + df["bldgs_5plus"]
    df["total_value"] = df["value_1"] + df["value_2"] + df["value_3_4"] + df["value_5plus"]
    return df


def normalize_geo_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in ["state_fips", "county_fips", "place_fips", "cbsa_code"]:
        s = pd.to_numeric(df[col], errors="coerce").round().astype("Int64").astype("string")

        def zp(x: pd.Series, w: int) -> pd.Series:
            return x.where(x.notna(), None).str.zfill(w)

        if col == "state_fips":
            df[col] = zp(s, 2)
        elif col == "county_fips":
            df[col] = zp(s, 5)
        elif col == "place_fips":
            df[col] = zp(s, 7)
        elif col == "cbsa_code":
            df[col] = zp(s, 5)

    return df


def reshape_long(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    id_cols = ["date", "year", "month", "state_fips", "county_fips", "place_fips", "cbsa_code", "location_type"]

    units_cols = {"units_1":"1","units_2":"2","units_3_4":"3_4","units_5plus":"5plus","total_units":"total"}
    bldgs_cols = {"bldgs_1":"1","bldgs_2":"2","bldgs_3_4":"3_4","bldgs_5plus":"5plus","total_bldgs":"total"}
    value_cols = {"value_1":"1","value_2":"2","value_3_4":"3_4","value_5plus":"5plus","total_value":"total"}

    frames = []
    for measure, mapping in [("units", units_cols), ("bldgs", bldgs_cols), ("value", value_cols)]:
        present = [c for c in mapping if c in df.columns]
        long = df.melt(id_vars=id_cols, value_vars=present, var_name="col", value_name="value")
        long["size_band"] = long["col"].map(mapping)
        long["measure"] = measure
        frames.append(long.drop(columns=["col"]))

    out = pd.concat(frames, ignore_index=True)
    out = out[out["date"].notna() & out["value"].notna()].copy()
    return out


def load_geo_manifest() -> pd.DataFrame:
    gm = pd.read_csv(GEO_MANIFEST, dtype=str)
    gm["include_census_bps"] = gm["include_census_bps"].astype(str).str.strip().isin(["1","true","True","Y","y"])
    gm = gm[gm["include_census_bps"]].copy()
    gm["level"] = gm["level"].str.lower().str.strip()
    gm["census_code"] = gm["census_code"].str.strip()
    return gm


def map_to_geo(df_long: pd.DataFrame, gm: pd.DataFrame) -> pd.DataFrame:
    df = df_long.copy()
    df["geo_id"] = None

    for row in gm.itertuples():
        geo = row.geo_slug
        level = row.level
        code = row.census_code

        if level == "nation":
            mask = df["location_type"] == "Country"
        elif level == "state":
            mask = (df["location_type"] == "State") & (df["state_fips"] == code)
        elif level == "county":
            mask = (df["location_type"] == "County") & (df["county_fips"] == code)
        elif level == "city":
            mask = (df["location_type"] == "Place") & (df["place_fips"] == code)
        elif level in ("cbsa_metro", "metro_area", "msa", "metro"):
            mask = (df["location_type"] == "Metro") & (df["cbsa_code"] == code)
        else:
            continue

        df.loc[mask, "geo_id"] = geo

    df = df[df["geo_id"].notna()].copy().reset_index(drop=True)
    return df


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Ingest BPS provisional (monthly) into a long CSV")
    p.add_argument("--url", default=None, help="Override provisional URL discovery")
    p.add_argument("--force-download", action="store_true")
    p.add_argument("--out", default=str(OUT_TIMESERIES_PATH))
    args = p.parse_args(argv)

    urls = discover_latest_provisional_urls()
    
    frames = []
    for level, url in urls.items():
        fname = url.split("/")[-1].replace("%20", "_")
        cache = Path("data/census") / f"bps_provisional__{level}__{fname}"
    
        path = download(url, cache, overwrite=args.force_download)
        df_level = read_provisional_txt(path, level)
        frames.append(df_level)
    
    df_raw = pd.concat(frames, ignore_index=True)

    """
    df_raw.loc[county_mask, "county_fips"] = df_raw.loc[county_mask, "state_fips"]
    df_raw.loc[cbsa_mask, "cbsa_code"] = df_raw.loc[cbsa_mask, "state_fips"]
    """
    
    df_raw.to_csv(RAW_PATH, index=False)
    print(f"[bps:prov] wrote raw → {RAW_PATH} ({len(df_raw):,} rows)")

    df_raw = normalize_schema(df_raw)
    df_raw = filter_monthly(df_raw)

    df = add_date(df_raw)
    df = compute_aggregates(df)
    df = normalize_geo_keys(df)
    df_long = reshape_long(df)

    gm = load_geo_manifest()
    df_geo = map_to_geo(df_long, gm)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df_geo.to_csv(out, index=False)

    print(f"[bps:prov] Final mapped rows: {len(df_geo):,}")
    print(f"[bps:prov] Wrote → {out}")


if __name__ == "__main__":
    main()
