from __future__ import annotations
# scripts/build_geo_hierarchy_from_redfin.py

import re
from pathlib import Path

import pandas as pd


RAW_DIR = Path("data/redfin/raw")
CONFIG_DIR = Path("config")

GEO_MANIFEST_PATH = CONFIG_DIR / "geo_manifest.csv"

OUT_MACRO = CONFIG_DIR / "geo_macro_hierarchy.generated.csv"
OUT_LOCAL = CONFIG_DIR / "geo_local_context.generated.csv"

# Optional. Script should run without these.
STATE_REF_PATH = CONFIG_DIR / "geo_state_scope.csv"
COUNTY_REF_PATH = CONFIG_DIR / "geo_county_scope.csv"
ZIP_COUNTY_XREF_PATH = CONFIG_DIR / "zip_county_xref.csv"


def _find_file(name: str) -> Path:
    path = RAW_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Missing Redfin raw file: {path}")
    return path


def _read_redfin(name: str) -> pd.DataFrame:
    path = _find_file(name)
    return pd.read_csv(path, sep="\t", dtype=str, low_memory=False)


def _norm_col(c: str) -> str:
    return str(c).strip().upper().replace(" ", "_")


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_norm_col(c) for c in out.columns]
    return out


def _norm_name(x: object) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" county", "")
    s = s.replace(" parish", "")
    s = s.replace(" city", "")
    return s.strip()


def _zip5(x: object) -> str:
    if pd.isna(x):
        return ""
    s = str(x)
    m = re.search(r"(\d{5})", s)
    return m.group(1) if m else ""


def _load_optional_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path, dtype=str)


def _scope_states(df: pd.DataFrame, state_ref: pd.DataFrame | None) -> pd.DataFrame:
    if state_ref is None:
        return df

    ref_cols = {_norm_col(c): c for c in state_ref.columns}
    if "STATE_CODE" not in ref_cols:
        raise ValueError(f"{STATE_REF_PATH} must contain STATE_CODE")

    allowed = set(state_ref[ref_cols["STATE_CODE"]].dropna().astype(str).str.upper())
    return df[df["STATE_CODE"].astype(str).str.upper().isin(allowed)].copy()


def _dedupe_geo(df: pd.DataFrame, geo_level: str, region_col: str = "REGION") -> pd.DataFrame:
    cols = [
        "TABLE_ID",
        region_col,
        "STATE",
        "STATE_CODE",
        "PARENT_METRO_REGION",
        "PARENT_METRO_REGION_METRO_CODE",
    ]
    keep = [c for c in cols if c in df.columns]
    out = df[keep].drop_duplicates().copy()
    out["geo_level"] = geo_level
    return out


def build_macro_hierarchy(
    county: pd.DataFrame,
    metro: pd.DataFrame,
    state: pd.DataFrame,
    state_ref: pd.DataFrame | None,
) -> pd.DataFrame:
    county = _scope_states(county, state_ref)
    metro = _scope_states(metro, state_ref)
    state = _scope_states(state, state_ref)

    county_d = _dedupe_geo(county, "county").rename(
        columns={
            "TABLE_ID": "county_table_id",
            "REGION": "county_name",
            "STATE": "county_state",
            "STATE_CODE": "state_code",
            "PARENT_METRO_REGION": "parent_cbsa_metro_name",
            "PARENT_METRO_REGION_METRO_CODE": "parent_cbsa_metro_code",
        }
    )

    metro_d = _dedupe_geo(metro, "metro").rename(
        columns={
            "TABLE_ID": "metro_table_id",
            "REGION": "cbsa_metro_name",
            "STATE_CODE": "metro_state_code",
            "PARENT_METRO_REGION_METRO_CODE": "parent_cbsa_metro_code",
        }
    )

    state_d = _dedupe_geo(state, "state").rename(
        columns={
            "TABLE_ID": "state_table_id",
            "REGION": "state_name",
            "STATE_CODE": "state_code",
            "PARENT_METRO_REGION": "census_region",
        }
    )

    county_d = county_d[
        county_d["parent_cbsa_metro_code"].notna()
        & (county_d["parent_cbsa_metro_code"].astype(str).str.upper() != "NA")
    ].copy()

    metro_d = metro_d[
        metro_d["parent_cbsa_metro_code"].notna()
        & (metro_d["parent_cbsa_metro_code"].astype(str).str.upper() != "NA")
    ].copy()

    out = county_d.merge(
        metro_d[["metro_table_id", "cbsa_metro_name", "parent_cbsa_metro_code"]],
        left_on="parent_cbsa_metro_code",
        right_on="parent_cbsa_metro_code",
        how="left",
    ).merge(
        state_d[["state_table_id", "state_name", "state_code", "census_region"]],
        on="state_code",
        how="left",
    )

    out["parent_nation_geo_id"] = "us_nation"
    out["mapping_method"] = "redfin_county_metro_state_metadata"
    out["mapping_confidence"] = out["metro_table_id"].notna().map({True: "high", False: "medium"})
    out["needs_review"] = out["metro_table_id"].isna()

    return out.sort_values(["state_code", "parent_cbsa_metro_name", "county_name"])


def build_local_zip_context(
    zip_df: pd.DataFrame,
    county: pd.DataFrame,
    zip_xref: pd.DataFrame | None,
    county_ref: pd.DataFrame | None,
) -> pd.DataFrame:
    if zip_xref is None:
        raise FileNotFoundError(
            f"Missing {ZIP_COUNTY_XREF_PATH}. "
            "V1 local ZIP mapping requires a ZIP-to-county crosswalk."
        )

    z = _dedupe_geo(zip_df, "zip").rename(
        columns={
            "TABLE_ID": "zip_table_id",
            "REGION": "zip_region",
            "STATE": "zip_state",
            "STATE_CODE": "state_code",
            "PARENT_METRO_REGION": "parent_cbsa_metro_name",
            "PARENT_METRO_REGION_METRO_CODE": "parent_cbsa_metro_code",
        }
    )
    z["zip5"] = z["zip_region"].map(_zip5)

    xref = zip_xref.copy()
    xref.columns = [_norm_col(c) for c in xref.columns]

    zip_col = "ZIP" if "ZIP" in xref.columns else "ZIP_CODE"
    county_col = "COUNTY" if "COUNTY" in xref.columns else None
    state_col = "STATE_CODE" if "STATE_CODE" in xref.columns else "STATE"

    if county_col is None:
        raise ValueError(f"{ZIP_COUNTY_XREF_PATH} must contain COUNTY")
    if zip_col not in xref.columns or state_col not in xref.columns:
        raise ValueError(f"{ZIP_COUNTY_XREF_PATH} must contain ZIP/ZIP_CODE and STATE/STATE_CODE")

    xref["zip5"] = xref[zip_col].map(_zip5)
    xref["county_norm"] = xref[county_col].map(_norm_name)
    xref["state_code_norm"] = xref[state_col].astype(str).str.upper().str.strip()
    xref['county_state_concat'] = xref["county_norm"] + ', ' + xref["state_code_norm"]

    if county_ref is not None:
        cref = county_ref.copy()
        cref.columns = [_norm_col(c) for c in cref.columns]
        if "COUNTY" not in cref.columns:
            raise ValueError(f"{COUNTY_REF_PATH} must contain COUNTY")
        cref["county_norm"] = cref["COUNTY"].map(_norm_name)
        if "STATE_CODE" in cref.columns:
            cref["state_code_norm"] = cref["STATE_CODE"].astype(str).str.upper().str.strip()
            xref = xref.merge(
                cref[["county_norm", "state_code_norm"]].drop_duplicates(),
                on=["county_norm", "state_code_norm"],
                how="inner",
            )
        else:
            xref = xref.merge(
                cref[["county_norm"]].drop_duplicates(),
                on="county_norm",
                how="inner",
            )

    c = _dedupe_geo(county, "county").rename(
        columns={
            "TABLE_ID": "county_table_id",
            "REGION": "county_name",
            "STATE": "county_state",
            "STATE_CODE": "state_code",
            "PARENT_METRO_REGION": "parent_cbsa_metro_name",
            "PARENT_METRO_REGION_METRO_CODE": "parent_cbsa_metro_code",
        }
    )
    c["county_norm"] = c["county_name"].map(_norm_name)
    c["state_code_norm"] = c["state_code"].astype(str).str.upper().str.strip()

    out = z.merge(
        xref[["zip5", "county_norm", "state_code_norm", "county_state_concat"]].drop_duplicates(),
        left_on=["zip5", "state_code"],
        right_on=["zip5", "state_code_norm"],
        how="inner",
    ).merge(
        c[
            [
                "county_table_id",
                "county_name",
                "county_norm",
                "state_code_norm",
                "parent_cbsa_metro_name",
                "parent_cbsa_metro_code",
            ]
        ].drop_duplicates(),
        left_on=["county_state_concat", "state_code_norm", "parent_cbsa_metro_code"],
        right_on=["county_norm", "state_code_norm", "parent_cbsa_metro_code"],
        how="left",
    )

    out["child_geo_level"] = "zip"
    out["parent_nation_geo_id"] = "us_nation"
    out["mapping_method"] = "redfin_zip_to_county_via_zip_county_xref"
    out["mapping_confidence"] = out["county_table_id"].notna().map({True: "high", False: "medium"})
    out["needs_review"] = out["county_table_id"].isna()

    return out.sort_values(["state_code", "county_name", "zip5"])


def main() -> int:
    if not GEO_MANIFEST_PATH.exists():
        raise FileNotFoundError(f"Missing {GEO_MANIFEST_PATH}")

    state_ref = _load_optional_csv(STATE_REF_PATH)
    county_ref = _load_optional_csv(COUNTY_REF_PATH)
    zip_xref = _load_optional_csv(ZIP_COUNTY_XREF_PATH)

    county = _normalize_cols(_read_redfin("county_market_tracker.tsv000"))
    metro = _normalize_cols(_read_redfin("redfin_metro_market_tracker.tsv000"))
    state = _normalize_cols(_read_redfin("state_market_tracker.tsv000"))
    zip_df = _normalize_cols(_read_redfin("zip_code_market_tracker.tsv000"))

    macro = build_macro_hierarchy(county, metro, state, state_ref)
    local_zip = build_local_zip_context(zip_df, county, zip_xref, county_ref)

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    macro.to_csv(OUT_MACRO, index=False)
    local_zip.to_csv(OUT_LOCAL, index=False)

    print(f"[geo] wrote {len(macro):,} rows -> {OUT_MACRO}")
    print(f"[geo] wrote {len(local_zip):,} rows -> {OUT_LOCAL}")
    print(f"[geo] macro needs_review={int(macro['needs_review'].sum())}")
    print(f"[geo] local needs_review={int(local_zip['needs_review'].sum())}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
