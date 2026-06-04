from __future__ import annotations
# scripts/build_geo_scope_from_hierarchy.py

from pathlib import Path
import pandas as pd


CONFIG_DIR = Path("config")

MACRO_IN = CONFIG_DIR / "geo_macro_hierarchy.generated.csv"
LOCAL_IN = CONFIG_DIR / "geo_local_context.generated.csv"

MACRO_OUT = CONFIG_DIR / "geo_scope_macro.csv"
LOCAL_OUT = CONFIG_DIR / "geo_scope_local.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path, dtype=str)


def _clean_bool_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().isin(["true", "1", "yes"])


def build_macro_scope(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows where the confidence match isn't high
    df = df[df['mapping_confidence'] == "high"]
    
    rows = []

    # nation
    rows.append(
        {
            "geo_name": "United States",
            "geo_level": "nation",
            "redfin_code": "",
            "include": 1,
            "notes": "Generated default nation scope",
        }
    )

    # census regions
    if "census_region" in df.columns:
        for name in sorted(df["census_region"].dropna().unique()):
            rows.append(
                {
                    "geo_name": name,
                    "geo_level": "region",
                    "redfin_code": "",
                    "include": 1,
                    "notes": "Generated from Redfin state parent/census region metadata",
                }
            )

    # states
    state_cols = ["state_name", "state_code", "state_table_id"]
    if all(c in df.columns for c in state_cols):
        states = df[state_cols].dropna().drop_duplicates()
        for r in states.itertuples(index=False):
            rows.append(
                {
                    "geo_name": r.state_name,
                    "geo_level": "state",
                    "redfin_code": r.state_table_id,
                    "include": 1,
                    "notes": f"Generated from Redfin macro hierarchy; state_code={r.state_code}",
                }
            )

    # metros
    metro_name_col = "cbsa_metro_name" if "cbsa_metro_name" in df.columns else "parent_cbsa_metro_name"
    metro_code_col = "parent_cbsa_metro_code"
    if metro_name_col in df.columns:
        metro_cols = [metro_name_col]
        if metro_code_col in df.columns:
            metro_cols.append(metro_code_col)

        metros = df[metro_cols].dropna().drop_duplicates()
        for _, r in metros.iterrows():
            code = r.get(metro_code_col, "")
            rows.append(
                {
                    "geo_name": r[metro_name_col],
                    "geo_level": "cbsa_metro",
                    "redfin_code": code,
                    "include": 1,
                    "notes": f"Generated from Redfin macro hierarchy; metro_code={code}",
                }
            )

    # counties
    if "county_name" in df.columns:
        county_cols = ["county_name"]
        for c in ["state_code", "county_table_id"]:
            if c in df.columns:
                county_cols.append(c)

        counties = df[county_cols].dropna(subset=["county_name"]).drop_duplicates()
        for _, r in counties.iterrows():
            notes = []
            if "state_code" in r:
                notes.append(f"state_code={r['state_code']}")
            if "county_table_id" in r:
                notes.append(f"redfin_table_id={r['county_table_id']}")
            rows.append(
                {
                    "geo_name": r["county_name"],
                    "geo_level": "county",
                    "redfin_code": r['county_table_id'],
                    "include": 1,
                    "notes": "Generated from Redfin macro hierarchy"
                    + (f"; {'; '.join(notes)}" if notes else ""),
                }
            )

    out = pd.DataFrame(rows).drop_duplicates(
        subset=["geo_name", "geo_level"], keep="first"
    )

    out = out.sort_values(["geo_level", "geo_name"]).reset_index(drop=True)
    return out[["geo_name", "geo_level", "redfin_code", "include", "notes"]]


def build_local_scope(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows where the confidence match isn't high
    df = df[df['mapping_confidence'] == "high"]
    
    rows = []

    # ZIPs
    zip_name_col = "zip_region"
    if zip_name_col not in df.columns:
        raise ValueError(f"Expected column {zip_name_col!r} in {LOCAL_IN}")

    local_cols = [zip_name_col]
    for c in ["zip5", "state_code", "county_name", "county_table_id", "zip_table_id"]:
        if c in df.columns:
            local_cols.append(c)

    zips = df[local_cols].dropna(subset=[zip_name_col]).drop_duplicates()

    for _, r in zips.iterrows():
        notes = []
        if "zip5" in r:
            notes.append(f"zip5={r['zip5']}")
        if "state_code" in r:
            notes.append(f"state_code={r['state_code']}")
        if "county_name" in r:
            notes.append(f"county={r['county_name']}")
        if "zip_table_id" in r:
            notes.append(f"redfin_table_id={r['zip_table_id']}")
        if "county_table_id" in r:
            notes.append(f"parent_county_table_id={r['county_table_id']}")

        rows.append(
            {
                "geo_name": r['zip5']+', '+r['county_name'],
                "geo_level": "zip",
                "redfin_code": r['zip_table_id'],
                "include": 1,
                "notes": "Generated from Redfin local context hierarchy"
                + (f"; {'; '.join(notes)}" if notes else ""),
            }
        )

    out = pd.DataFrame(rows).drop_duplicates(
        subset=["geo_name", "geo_level"], keep="first"
    )

    out = out.sort_values(["geo_level", "geo_name"]).reset_index(drop=True)
    return out[["geo_name", "geo_level", "redfin_code", "include", "notes"]]


def main() -> int:
    macro_df = _read_csv(MACRO_IN)
    local_df = _read_csv(LOCAL_IN)

    macro_scope = build_macro_scope(macro_df)
    local_scope = build_local_scope(local_df)

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    macro_scope.to_csv(MACRO_OUT, index=False)
    local_scope.to_csv(LOCAL_OUT, index=False)

    print(f"[geo-scope] wrote {len(macro_scope):,} rows -> {MACRO_OUT}")
    print(f"[geo-scope] wrote {len(local_scope):,} rows -> {LOCAL_OUT}")

    print("\n[geo-scope] macro counts:")
    print(macro_scope["geo_level"].value_counts().to_string())

    print("\n[geo-scope] local counts:")
    print(local_scope["geo_level"].value_counts().to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
