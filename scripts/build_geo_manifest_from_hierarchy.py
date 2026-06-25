from __future__ import annotations
# scripts/build_geo_manifest_from_hierarchy.py

from pathlib import Path

import pandas as pd

import re

# ===================================================
# Constants and Configs
# ===================================================
CONFIG_DIR = Path("config")

MACRO_SCOPE = CONFIG_DIR / "geo_scope_macro.csv"
LOCAL_SCOPE = CONFIG_DIR / "geo_scope_local.csv"
OUT = CONFIG_DIR / "geo_manifest.generated.csv"
COUNTY_FIPS_XREF = CONFIG_DIR / "xref_county_fips.csv"

MANIFEST_COLUMNS = [
    "geo_slug",
    "geo_name",
    "level",
    "bls_ces_area_code",
    "include_ces",
    "bls_laus_area_code",
    "include_laus",
    "redfin_code",
    "include_redfin",
    "census_code",
    "include_census",
    "include_census_bps",
    "bea_geo_fips",
    "include_bea_qgdp",
    "include_bea_agdp",
    "fred_unemp_series_id",
    "include_fred_unemp",
    "fred_geo_code",
    "include_fred",
]

STATE_FIPS = {
    "CA": "06",
    "DC": "11",
    "MD": "24",
    "NJ": "34",
    "VA": "51",
}

FRED_UNEMP_SERIES = {
    "nation": "UNRATE",
    "CA": "CAUR",
    "DC": "DCUR",
    "MD": "MDUR",
    "NJ": "NJUR",
    "VA": "VAUR",
}

# ===================================================
# Helpers
# ===================================================
def county_join_key(name: object, state_code: object) -> str:
    s = str(name).strip().lower()
    st = str(state_code).strip().upper()

    s = re.sub(r",\s*[a-z]{2}$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("&", "and")

    if st == "VA":
        if "city county" in s:
            return s.replace(" city county", " city").strip()

        if s.endswith(" county"):
            return s

        if s.endswith(" city"):
            return s

        return f"{s} city"

    return s


def read_scope(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required scope file: {path}")

    df = pd.read_csv(path, dtype=str).fillna("")

    # Fix known Redfin DC state naming artifact.
    dc_state_mask = (
        df.get("geo_level", "").astype(str).str.strip().eq("state")
        & df.get("state_code", "").astype(str).str.strip().str.upper().eq("DC")
        & df.get("geo_name", "").astype(str).str.strip().eq("Columbia")
    )
    df.loc[dc_state_mask, "geo_name"] = "District of Columbia"
        
    required = {"geo_name", "geo_level", "include", "redfin_code"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")

    df["include_norm"] = df["include"].astype(str).str.strip().str.lower()
    df = df[df["include_norm"].isin(["1", "true", "yes", "y"])].copy()

    return df


def slugify_geo_name(geo_name: object, level: object) -> str:
    name = str(geo_name or "").strip().lower()
    lvl = str(level or "").strip().lower()

    s = name
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")

    lvl = re.sub(r"[^a-z0-9]+", "_", lvl).strip("_")

    return f"{s}__{lvl}"


def build_base_manifest(scope: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "level": scope["geo_level"].astype(str).str.strip(),
            "geo_name": scope["geo_name"].astype(str).str.strip(),
        }
    )

    out = out.drop_duplicates(subset=["level", "geo_name"], keep="first")
    out["geo_slug"] = out.apply(
        lambda r: slugify_geo_name(r["geo_name"], r["level"]),
        axis=1,
    )

    return out
    
# ===================================================
# Resolvers
# ===================================================
def apply_redfin_resolver(manifest: pd.DataFrame, scope: pd.DataFrame) -> pd.DataFrame:
    redfin = scope[["geo_level", "geo_name", "redfin_code"]].copy()
    redfin = redfin.rename(columns={"geo_level": "level"})

    redfin["level"] = redfin["level"].astype(str).str.strip()
    redfin["geo_name"] = redfin["geo_name"].astype(str).str.strip()
    redfin["redfin_code"] = redfin["redfin_code"].astype(str).str.strip()
    redfin["include_redfin"] = redfin["redfin_code"].ne("").astype(int).astype(str)

    redfin = redfin.drop_duplicates(subset=["level", "geo_name"], keep="first")

    out = manifest.merge(
        redfin[["level", "geo_name", "redfin_code", "include_redfin"]],
        on=["level", "geo_name"],
        how="left",
    )

    out["redfin_code"] = out["redfin_code"].fillna("")
    out["include_redfin"] = out["include_redfin"].fillna("0")

    return out


def apply_bls_ces_resolver(manifest: pd.DataFrame, scope: pd.DataFrame) -> pd.DataFrame:
    out = manifest.copy()

    if "bls_ces_area_code" not in out.columns:
        out["bls_ces_area_code"] = ""
    else:
        out["bls_ces_area_code"] = out["bls_ces_area_code"].fillna("").astype(str)
    
    if "include_ces" not in out.columns:
        out["include_ces"] = "0"
    else:
        out["include_ces"] = out["include_ces"].fillna("0").astype(str)

    # Nation
    nation_mask = out["level"].astype(str).str.strip().eq("nation")
    out.loc[nation_mask, "bls_ces_area_code"] = "0000000"
    out.loc[nation_mask, "include_ces"] = "1"

    # Need state_code for state + cbsa_metro rows.
    if "state_code" not in scope.columns:
        raise ValueError("Scope must include state_code for BLS CES resolver.")

    ces_scope = scope[["geo_level", "geo_name", "state_code", "redfin_code"]].copy()
    ces_scope = ces_scope.rename(columns={"geo_level": "level"})
    ces_scope["level"] = ces_scope["level"].astype(str).str.strip()
    ces_scope["geo_name"] = ces_scope["geo_name"].astype(str).str.strip()
    ces_scope["state_code"] = ces_scope["state_code"].astype(str).str.upper().str.strip()
    ces_scope["redfin_code"] = ces_scope["redfin_code"].astype(str).str.strip()

    # State rows: state FIPS + 00000
    state_scope = ces_scope[ces_scope["level"].eq("state")].copy()
    state_scope["bls_ces_area_code_resolved"] = state_scope["state_code"].map(STATE_FIPS).fillna("") + "00000"
    state_scope.loc[state_scope["state_code"].map(STATE_FIPS).isna(), "bls_ces_area_code_resolved"] = ""
    state_scope["include_ces_resolved"] = state_scope["bls_ces_area_code_resolved"].ne("").astype(int).astype(str)

    out = out.merge(
        state_scope[["level", "geo_name", "bls_ces_area_code_resolved", "include_ces_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["bls_ces_area_code_resolved"].fillna("").ne("")
    out.loc[m, "bls_ces_area_code"] = out.loc[m, "bls_ces_area_code_resolved"]
    out.loc[m, "include_ces"] = out.loc[m, "include_ces_resolved"]

    out = out.drop(columns=["bls_ces_area_code_resolved", "include_ces_resolved"])

    # CBSA metro rows: state FIPS + CBSA code.
    # V1 safety rule: only keep if state_code is known and redfin_code is 5 digits.
    metro_scope = ces_scope[ces_scope["level"].eq("cbsa_metro")].copy()
    metro_scope["state_fips"] = metro_scope["state_code"].map(STATE_FIPS).fillna("")
    metro_scope["redfin_code_5"] = metro_scope["redfin_code"].str.extract(r"(\d{5})", expand=False).fillna("")
    metro_scope["bls_ces_area_code_resolved"] = metro_scope["state_fips"] + metro_scope["redfin_code_5"]

    valid_metro = (
        metro_scope["state_fips"].ne("")
        & metro_scope["redfin_code_5"].str.fullmatch(r"\d{5}")
    )

    metro_scope.loc[~valid_metro, "bls_ces_area_code_resolved"] = ""
    metro_scope["include_ces_resolved"] = metro_scope["bls_ces_area_code_resolved"].ne("").astype(int).astype(str)

    out = out.merge(
        metro_scope[["level", "geo_name", "bls_ces_area_code_resolved", "include_ces_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["bls_ces_area_code_resolved"].fillna("").ne("")
    out.loc[m, "bls_ces_area_code"] = out.loc[m, "bls_ces_area_code_resolved"]
    out.loc[m, "include_ces"] = out.loc[m, "include_ces_resolved"]

    out = out.drop(columns=["bls_ces_area_code_resolved", "include_ces_resolved"])

    return out


def apply_bls_laus_resolver(manifest: pd.DataFrame, scope: pd.DataFrame) -> pd.DataFrame:
    out = manifest.copy()

    for c in ["bls_laus_area_code", "include_laus"]:
        if c not in out.columns:
            out[c] = "" if c == "bls_laus_area_code" else "0"

    out["bls_laus_area_code"] = out["bls_laus_area_code"].fillna("").astype(str)
    out["include_laus"] = out["include_laus"].fillna("0").astype(str)

    # Nation
    nation_mask = out["level"].astype(str).str.strip().eq("nation")
    out.loc[nation_mask, "bls_laus_area_code"] = ""
    out.loc[nation_mask, "include_laus"] = "0"

    if "state_code" not in scope.columns:
        raise ValueError("Scope must include state_code for BLS LAUS resolver.")

    laus_scope = scope[["geo_level", "geo_name", "state_code", "redfin_code"]].copy()
    laus_scope = laus_scope.rename(columns={"geo_level": "level"})
    laus_scope["level"] = laus_scope["level"].astype(str).str.strip()
    laus_scope["geo_name"] = laus_scope["geo_name"].astype(str).str.strip()
    laus_scope["state_code"] = laus_scope["state_code"].astype(str).str.upper().str.strip()
    laus_scope["redfin_code"] = laus_scope["redfin_code"].astype(str).str.strip()

    # State: state_fips + 11 zeros
    state_scope = laus_scope[laus_scope["level"].eq("state")].copy()
    state_scope["state_fips"] = state_scope["state_code"].map(STATE_FIPS).fillna("")
    state_scope["bls_laus_area_code_resolved"] = state_scope["state_fips"] + "0000000000"
    state_scope.loc[state_scope["state_fips"].eq(""), "bls_laus_area_code_resolved"] = ""

    out = out.merge(
        state_scope[["level", "geo_name", "bls_laus_area_code_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["bls_laus_area_code_resolved"].fillna("").ne("")
    out.loc[m, "bls_laus_area_code"] = out.loc[m, "bls_laus_area_code_resolved"]
    out.loc[m, "include_laus"] = "1"
    out = out.drop(columns=["bls_laus_area_code_resolved"])

    # CBSA metro: state_fips + metro_fips + 6 zeros
    metro_scope = laus_scope[laus_scope["level"].eq("cbsa_metro")].copy()
    metro_scope["state_fips"] = metro_scope["state_code"].map(STATE_FIPS).fillna("")
    metro_scope["metro_fips"] = metro_scope["redfin_code"].str.extract(r"(\d{5})", expand=False).fillna("")
    metro_scope["bls_laus_area_code_resolved"] = (
        metro_scope["state_fips"] + metro_scope["metro_fips"] + "000000"
    )

    valid_metro = metro_scope["state_fips"].ne("") & metro_scope["metro_fips"].str.fullmatch(r"\d{5}")
    metro_scope.loc[~valid_metro, "bls_laus_area_code_resolved"] = ""

    out = out.merge(
        metro_scope[["level", "geo_name", "bls_laus_area_code_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["bls_laus_area_code_resolved"].fillna("").ne("")
    out.loc[m, "bls_laus_area_code"] = out.loc[m, "bls_laus_area_code_resolved"]
    out.loc[m, "include_laus"] = "1"
    out = out.drop(columns=["bls_laus_area_code_resolved"])

    # County: state_fips + county_fips + 8 zeros
    if not COUNTY_FIPS_XREF.exists():
        raise FileNotFoundError(f"Missing required county FIPS xref: {COUNTY_FIPS_XREF}")

    xref = pd.read_csv(COUNTY_FIPS_XREF, dtype=str).fillna("")
    xref.columns = [c.strip().lower() for c in xref.columns]

    required = {"county_name", "state_code", "state_fips", "county_fips"}
    missing = required - set(xref.columns)
    if missing:
        raise ValueError(f"{COUNTY_FIPS_XREF} missing required columns: {sorted(missing)}")

    xref["state_code"] = xref["state_code"].astype(str).str.upper().str.strip()
    xref["state_fips"] = xref["state_fips"].astype(str).str.zfill(2)
    xref["county_fips"] = xref["county_fips"].astype(str).str.zfill(3)
    xref = xref[
        xref["county_fips"].notna()
        & xref["county_fips"].ne("")
        & xref["county_fips"].ne("000")
    ].copy()

    xref["county_join_key"] = xref.apply(
        lambda r: county_join_key(r["county_name"], r["state_code"]),
        axis=1,
    )
    xref["bls_laus_area_code_resolved"] = xref["state_fips"] + xref["county_fips"] + "00000000"

    county_scope = laus_scope[laus_scope["level"].eq("county")].copy()
    county_scope["county_join_key"] = county_scope.apply(
        lambda r: county_join_key(r["geo_name"], r["state_code"]),
        axis=1,
    )

    county_scope = county_scope.merge(
        xref[["county_join_key", "state_code", "bls_laus_area_code_resolved"]].drop_duplicates(),
        on=["county_join_key", "state_code"],
        how="left",
    )

    out = out.merge(
        county_scope[["level", "geo_name", "bls_laus_area_code_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["bls_laus_area_code_resolved"].fillna("").ne("")
    out.loc[m, "bls_laus_area_code"] = out.loc[m, "bls_laus_area_code_resolved"]
    out.loc[m, "include_laus"] = "1"
    out = out.drop(columns=["bls_laus_area_code_resolved"])

    return out


def apply_bea_resolver(manifest: pd.DataFrame, scope: pd.DataFrame) -> pd.DataFrame:
    out = manifest.copy()

    for c in ["bea_geo_fips", "include_bea_qgdp", "include_bea_agdp"]:
        if c not in out.columns:
            out[c] = "" if c == "bea_geo_fips" else "0"

    out["bea_geo_fips"] = out["bea_geo_fips"].fillna("").astype(str)
    out["include_bea_qgdp"] = out["include_bea_qgdp"].fillna("0").astype(str)
    out["include_bea_agdp"] = out["include_bea_agdp"].fillna("0").astype(str)

    # Nation
    nation_mask = out["level"].eq("nation")
    out.loc[nation_mask, "bea_geo_fips"] = "00000"
    out.loc[nation_mask, "include_bea_qgdp"] = "1"
    out.loc[nation_mask, "include_bea_agdp"] = "1"

    # State
    if "state_code" not in scope.columns:
        raise ValueError("Scope must include state_code for BEA resolver.")

    bea_scope = scope[["geo_level", "geo_name", "state_code"]].copy()
    bea_scope = bea_scope.rename(columns={"geo_level": "level"})
    bea_scope["level"] = bea_scope["level"].astype(str).str.strip()
    bea_scope["geo_name"] = bea_scope["geo_name"].astype(str).str.strip()
    bea_scope["state_code"] = bea_scope["state_code"].astype(str).str.upper().str.strip()

    state_scope = bea_scope[bea_scope["level"].eq("state")].copy()
    state_scope["bea_geo_fips_resolved"] = state_scope["state_code"].map(STATE_FIPS).fillna("") + "000"
    state_scope.loc[state_scope["state_code"].map(STATE_FIPS).isna(), "bea_geo_fips_resolved"] = ""

    out = out.merge(
        state_scope[["level", "geo_name", "bea_geo_fips_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["bea_geo_fips_resolved"].fillna("").ne("")
    out.loc[m, "bea_geo_fips"] = out.loc[m, "bea_geo_fips_resolved"]
    out.loc[m, "include_bea_qgdp"] = "1"
    out.loc[m, "include_bea_agdp"] = "1"

    out = out.drop(columns=["bea_geo_fips_resolved"])

    # County annual GDP
    if not COUNTY_FIPS_XREF.exists():
        raise FileNotFoundError(f"Missing required county FIPS xref: {COUNTY_FIPS_XREF}")

    xref = pd.read_csv(COUNTY_FIPS_XREF, dtype=str).fillna("")
    xref.columns = [c.strip().lower() for c in xref.columns]

    required = {"county_name", "state_code", "state_fips", "county_fips"}
    missing = required - set(xref.columns)
    if missing:
        raise ValueError(f"{COUNTY_FIPS_XREF} missing required columns: {sorted(missing)}")

    xref["county_join_key"] = xref.apply(
        lambda r: county_join_key(r["county_name"], r["state_code"]),
        axis=1
    )
    xref["state_code"] = xref["state_code"].astype(str).str.upper().str.strip()
    xref["state_fips"] = xref["state_fips"].astype(str).str.zfill(2)
    xref["county_fips"] = xref["county_fips"].astype(str).str.zfill(3)

    # Drop state-level pseudo-county rows.
    xref = xref[xref["county_fips"].ne("000")].copy()
    xref["bea_geo_fips_resolved"] = xref["state_fips"] + xref["county_fips"]

    county_scope = bea_scope[bea_scope["level"].eq("county")].copy()
    county_scope["county_join_key"] = county_scope.apply(
        lambda r: county_join_key(r["geo_name"], r["state_code"]),
        axis=1
    )

    county_scope = county_scope.merge(
        xref[["county_join_key", "state_code", "bea_geo_fips_resolved"]].drop_duplicates(),
        on=["county_join_key", "state_code"],
        how="left",
    )

    out = out.merge(
        county_scope[["level", "geo_name", "bea_geo_fips_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["bea_geo_fips_resolved"].fillna("").ne("")
    out.loc[m, "bea_geo_fips"] = out.loc[m, "bea_geo_fips_resolved"]
    out.loc[m, "include_bea_qgdp"] = "0"
    out.loc[m, "include_bea_agdp"] = "1"

    out = out.drop(columns=["bea_geo_fips_resolved"])

    return out


def apply_fred_resolver(manifest: pd.DataFrame, scope: pd.DataFrame) -> pd.DataFrame:
    out = manifest.copy()

    for c in ["fred_unemp_series_id", "include_fred_unemp", "fred_geo_code", "include_fred"]:
        if c not in out.columns:
            out[c] = "" if c in ["fred_unemp_series_id", "fred_geo_code"] else "0"

    out["fred_unemp_series_id"] = out["fred_unemp_series_id"].fillna("").astype(str)
    out["include_fred_unemp"] = out["include_fred_unemp"].fillna("0").astype(str)
    out["fred_geo_code"] = out["fred_geo_code"].fillna("").astype(str)
    out["include_fred"] = out["include_fred"].fillna("0").astype(str)

    # Nation: national macro FRED + unemployment
    nation_mask = out["level"].astype(str).str.strip().eq("nation")
    out.loc[nation_mask, "fred_unemp_series_id"] = FRED_UNEMP_SERIES["nation"]
    out.loc[nation_mask, "include_fred_unemp"] = "1"
    out.loc[nation_mask, "fred_geo_code"] = "US"
    out.loc[nation_mask, "include_fred"] = "1"

    if "state_code" not in scope.columns:
        raise ValueError("Scope must include state_code for FRED resolver.")

    fred_scope = scope[["geo_level", "geo_name", "state_code"]].copy()
    fred_scope = fred_scope.rename(columns={"geo_level": "level"})
    fred_scope["level"] = fred_scope["level"].astype(str).str.strip()
    fred_scope["geo_name"] = fred_scope["geo_name"].astype(str).str.strip()
    fred_scope["state_code"] = fred_scope["state_code"].astype(str).str.upper().str.strip()

    # States: unemployment series only. No generic fred_geo_code.
    state_scope = fred_scope[fred_scope["level"].eq("state")].copy()
    state_scope["fred_unemp_series_id_resolved"] = state_scope["state_code"].map(FRED_UNEMP_SERIES).fillna("")
    state_scope["include_fred_unemp_resolved"] = state_scope["fred_unemp_series_id_resolved"].ne("").astype(int).astype(str)

    out = out.merge(
        state_scope[
            ["level", "geo_name", "fred_unemp_series_id_resolved", "include_fred_unemp_resolved"]
        ],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["fred_unemp_series_id_resolved"].fillna("").ne("")
    out.loc[m, "fred_unemp_series_id"] = out.loc[m, "fred_unemp_series_id_resolved"]
    out.loc[m, "include_fred_unemp"] = out.loc[m, "include_fred_unemp_resolved"]

    out = out.drop(columns=["fred_unemp_series_id_resolved", "include_fred_unemp_resolved"])

    return out


def apply_census_resolver(manifest: pd.DataFrame, scope: pd.DataFrame) -> pd.DataFrame:
    out = manifest.copy()

    for c in ["census_code", "include_census", "include_census_bps"]:
        if c not in out.columns:
            out[c] = "" if c == "census_code" else "0"

    out["census_code"] = out["census_code"].fillna("").astype(str)
    out["include_census"] = out["include_census"].fillna("0").astype(str)
    out["include_census_bps"] = out["include_census_bps"].fillna("0").astype(str)

    # Nation
    nation_mask = out["level"].astype(str).str.strip().eq("nation")
    out.loc[nation_mask, "census_code"] = "00"
    out.loc[nation_mask, "include_census"] = "1"
    out.loc[nation_mask, "include_census_bps"] = "1"

    if "state_code" not in scope.columns:
        raise ValueError("Scope must include state_code for Census resolver.")

    census_scope = scope[["geo_level", "geo_name", "state_code", "redfin_code"]].copy()
    census_scope = census_scope.rename(columns={"geo_level": "level"})
    census_scope["level"] = census_scope["level"].astype(str).str.strip()
    census_scope["geo_name"] = census_scope["geo_name"].astype(str).str.strip()
    census_scope["state_code"] = census_scope["state_code"].astype(str).str.upper().str.strip()
    census_scope["redfin_code"] = census_scope["redfin_code"].astype(str).str.strip()

    # State
    state_scope = census_scope[census_scope["level"].eq("state")].copy()
    state_scope["census_code_resolved"] = state_scope["state_code"].map(STATE_FIPS).fillna("")

    out = out.merge(
        state_scope[["level", "geo_name", "census_code_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["census_code_resolved"].fillna("").ne("")
    out.loc[m, "census_code"] = out.loc[m, "census_code_resolved"]
    out.loc[m, "include_census"] = "1"
    out.loc[m, "include_census_bps"] = "1"
    out = out.drop(columns=["census_code_resolved"])

    # CBSA metro: use redfin_code because it equals CBSA code in your hierarchy.
    metro_scope = census_scope[census_scope["level"].eq("cbsa_metro")].copy()
    metro_scope["census_code_resolved"] = metro_scope["redfin_code"].str.extract(r"(\d{5})", expand=False).fillna("")

    out = out.merge(
        metro_scope[["level", "geo_name", "census_code_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["census_code_resolved"].fillna("").ne("")
    out.loc[m, "census_code"] = out.loc[m, "census_code_resolved"]
    out.loc[m, "include_census"] = "1"
    out.loc[m, "include_census_bps"] = "1"
    out = out.drop(columns=["census_code_resolved"])

    # County: state_fips + county_fips from xref.
    if not COUNTY_FIPS_XREF.exists():
        raise FileNotFoundError(f"Missing required county FIPS xref: {COUNTY_FIPS_XREF}")

    xref = pd.read_csv(COUNTY_FIPS_XREF, dtype=str).fillna("")
    xref.columns = [c.strip().lower() for c in xref.columns]

    required = {"county_name", "state_code", "state_fips", "county_fips"}
    missing = required - set(xref.columns)
    if missing:
        raise ValueError(f"{COUNTY_FIPS_XREF} missing required columns: {sorted(missing)}")

    xref["state_code"] = xref["state_code"].astype(str).str.upper().str.strip()
    xref["state_fips"] = xref["state_fips"].astype(str).str.zfill(2)
    xref["county_fips"] = xref["county_fips"].astype(str).str.zfill(3)
    #xref = xref[xref["county_fips"].ne("000")].copy() <--- DELETE LATER?
    xref = xref[
        xref["county_fips"].notna() & xref["county_fips"].ne("") & xref["county_fips"].ne("000")
    ].copy()

    xref["county_join_key"] = xref.apply(
        lambda r: county_join_key(r["county_name"], r["state_code"]),
        axis=1,
    )
    xref["census_code_resolved"] = xref["state_fips"] + xref["county_fips"]

    county_scope = census_scope[census_scope["level"].eq("county")].copy()
    county_scope["county_join_key"] = county_scope.apply(
        lambda r: county_join_key(r["geo_name"], r["state_code"]),
        axis=1,
    )

    county_scope = county_scope.merge(
        xref[["county_join_key", "state_code", "census_code_resolved"]].drop_duplicates(),
        on=["county_join_key", "state_code"],
        how="left",
    )

    out = out.merge(
        county_scope[["level", "geo_name", "census_code_resolved"]],
        on=["level", "geo_name"],
        how="left",
    )

    m = out["census_code_resolved"].fillna("").ne("")
    out.loc[m, "census_code"] = out.loc[m, "census_code_resolved"]
    out.loc[m, "include_census"] = "1"
    out.loc[m, "include_census_bps"] = "1"
    out = out.drop(columns=["census_code_resolved"])

    return out
    
# ===================================================
# Final Manifest File
# ===================================================
def finalize_manifest(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in MANIFEST_COLUMNS:
        if c not in out.columns:
            out[c] = ""

    for c in MANIFEST_COLUMNS:
        if c.startswith("include_"):
            out[c] = out[c].fillna("").astype(str)
            out[c] = out[c].replace({"": "0"})

    out = out[MANIFEST_COLUMNS]
    out = out.drop_duplicates(subset=["geo_slug"], keep="first")
    out = out.sort_values(["level", "geo_name"]).reset_index(drop=True)

    return out


def main() -> int:
    macro_scope = read_scope(MACRO_SCOPE)
    local_scope = read_scope(LOCAL_SCOPE)

    scope = pd.concat([macro_scope, local_scope], ignore_index=True)

    manifest = build_base_manifest(scope)
    manifest = apply_redfin_resolver(manifest, scope)
    manifest = apply_bls_ces_resolver(manifest, scope)
    manifest = apply_bls_laus_resolver(manifest, scope)
    manifest = apply_bea_resolver(manifest, scope)
    manifest = apply_fred_resolver(manifest, scope)
    manifest = apply_census_resolver(manifest, scope)
    manifest = finalize_manifest(manifest)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(OUT, index=False)

    print(f"[geo-manifest] wrote {len(manifest):,} rows -> {OUT}")
    print("\n[geo-manifest] counts by level:")
    print(manifest["level"].value_counts().to_string())

    print("\n[geo-manifest] redfin:")
    print(manifest["include_redfin"].value_counts().to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
