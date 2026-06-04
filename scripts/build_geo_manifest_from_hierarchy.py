from __future__ import annotations
# scripts/build_geo_manifest_from_hierarchy.py

from pathlib import Path

import pandas as pd


CONFIG_DIR = Path("config")

MACRO_SCOPE = CONFIG_DIR / "geo_scope_macro.csv"
LOCAL_SCOPE = CONFIG_DIR / "geo_scope_local.csv"
OUT = CONFIG_DIR / "geo_manifest.generated.csv"


MANIFEST_COLUMNS = [
    "level",
    "geo_name",
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
    "fred_unemp_series_id",
    "include_fred_unemp",
    "fred_geo_code",
    "include_fred",
]


def read_scope(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required scope file: {path}")

    df = pd.read_csv(path, dtype=str).fillna("")

    required = {"geo_name", "geo_level", "include", "redfin_code"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")

    df["include_norm"] = df["include"].astype(str).str.strip().str.lower()
    df = df[df["include_norm"].isin(["1", "true", "yes", "y"])].copy()

    return df


def build_base_manifest(scope: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "level": scope["geo_level"].astype(str).str.strip(),
            "geo_name": scope["geo_name"].astype(str).str.strip(),
        }
    )

    out = out.drop_duplicates(subset=["level", "geo_name"], keep="first")
    return out


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
    out = out.drop_duplicates(subset=["level", "geo_name"], keep="first")
    out = out.sort_values(["level", "geo_name"]).reset_index(drop=True)

    return out


def main() -> int:
    macro_scope = read_scope(MACRO_SCOPE)
    local_scope = read_scope(LOCAL_SCOPE)

    scope = pd.concat([macro_scope, local_scope], ignore_index=True)

    manifest = build_base_manifest(scope)
    manifest = apply_redfin_resolver(manifest, scope)
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
