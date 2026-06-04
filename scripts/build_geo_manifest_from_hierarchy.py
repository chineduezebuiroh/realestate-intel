from __future__ import annotations
# scripts/build_geo_manifest_from_hierarchy.py

from pathlib import Path
import pandas as pd


CONFIG_DIR = Path("config")

MACRO_HIERARCHY = CONFIG_DIR / "geo_macro_hierarchy.generated.csv"
LOCAL_CONTEXT = CONFIG_DIR / "geo_local_context.generated.csv"
MACRO_SCOPE = CONFIG_DIR / "geo_scope_macro.csv"
LOCAL_SCOPE = CONFIG_DIR / "geo_scope_local.csv"

OVERRIDES = CONFIG_DIR / "geo_manifest_overrides.csv"
OUT = CONFIG_DIR / "geo_manifest.generated.csv"


def read_csv(path: Path, required: bool = True) -> pd.DataFrame:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Missing required file: {path}")
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def norm(s: object) -> str:
    return str(s).strip().lower()


def include_set(scope: pd.DataFrame) -> set[tuple[str, str]]:
    """
    Returns {(geo_name_norm, geo_level_norm)} for include=1 rows.
    """
    if scope.empty:
        return set()

    required = {"geo_name", "geo_level", "include"}
    missing = required - set(scope.columns)
    if missing:
        raise ValueError(f"Scope file missing columns: {missing}")

    s = scope.copy()
    s["include"] = s["include"].astype(str).str.strip().str.lower()
    s = s[s["include"].isin(["1", "true", "yes", "y"])]

    return set(zip(s["geo_name"].map(norm), s["geo_level"].map(norm)))


def make_geo_id(name: str, level: str, state_code: str = "") -> str:
    """
    Compatibility helper only. Prefer preserving existing geo_ids later
    when we diff against current geo_manifest.csv.
    """
    base = norm(name)
    base = (
        base.replace(",", "")
        .replace(".", "")
        .replace("-", " ")
        .replace("/", " ")
        .replace("'", "")
        .replace(" county", "")
        .replace(" metro area", "")
        .replace(" zip code:", "zip")
    )
    parts = [p for p in base.split() if p]
    slug = "_".join(parts)

    if level == "state" and state_code:
        return f"{state_code.lower()}_state"
    if level == "nation":
        return "us_nation"
    if level == "zip":
        digits = "".join(ch for ch in name if ch.isdigit())
        return f"zip_{digits}" if digits else slug

    return slug


def append_row(rows: list[dict], **kwargs) -> None:
    defaults = {
        "geo_id": "",
        "geo_name": "",
        "geo_level": "",
        "state_code": "",
        "redfin_table_id": "",
        "redfin_metro_code": "",
        "parent_geo_id": "",
        "parent_geo_level": "",
        "include_redfin": "0",
        "include_census": "0",
        "include_bls_laus": "0",
        "include_bls_ces": "0",
        "include_bea": "0",
        "include_fred": "0",
        "source_method": "",
        "needs_review": "False",
        "notes": "",
    }
    defaults.update(kwargs)
    rows.append(defaults)


def build_macro_rows(macro: pd.DataFrame, macro_scope: pd.DataFrame) -> pd.DataFrame:
    scope = include_set(macro_scope)
    rows: list[dict] = []

    # Nation
    if ("united states", "nation") in scope or ("us_nation", "nation") in scope:
        append_row(
            rows,
            geo_id="us_nation",
            geo_name="United States",
            geo_level="nation",
            include_redfin="1",
            include_census="1",
            include_fred="1",
            source_method="generated_default_nation",
        )

    # Region
    if "census_region" in macro.columns:
        for region in sorted(set(macro["census_region"]) - {""}):
            if (norm(region), "region") not in scope:
                continue
            append_row(
                rows,
                geo_id=make_geo_id(region, "region"),
                geo_name=region,
                geo_level="region",
                parent_geo_id="us_nation",
                parent_geo_level="nation",
                include_redfin="0",
                source_method="generated_from_macro_hierarchy",
            )

    # States
    state_cols = {"state_name", "state_code", "state_table_id"}
    if state_cols <= set(macro.columns):
        states = macro[["state_name", "state_code", "state_table_id"]].drop_duplicates()
        for _, r in states.iterrows():
            name = r["state_name"]
            if not name or (norm(name), "state") not in scope:
                continue
            append_row(
                rows,
                geo_id=make_geo_id(name, "state", r["state_code"]),
                geo_name=name,
                geo_level="state",
                state_code=r["state_code"],
                redfin_table_id=r["state_table_id"],
                parent_geo_id="us_nation",
                parent_geo_level="nation",
                include_redfin="1",
                include_census="1",
                include_bls_laus="1",
                include_bls_ces="1",
                include_bea="1",
                source_method="generated_from_macro_hierarchy",
            )

    # Metros
    metro_name_col = "cbsa_metro_name" if "cbsa_metro_name" in macro.columns else "parent_cbsa_metro_name"
    if metro_name_col in macro.columns:
        metro_cols = [metro_name_col]
        for c in ["metro_table_id", "parent_cbsa_metro_code", "state_code"]:
            if c in macro.columns:
                metro_cols.append(c)

        metros = macro[metro_cols].drop_duplicates()
        for _, r in metros.iterrows():
            name = r[metro_name_col]
            if not name or (norm(name), "metro") not in scope:
                continue
            metro_code = r.get("parent_cbsa_metro_code", "")
            append_row(
                rows,
                geo_id=make_geo_id(name, "metro"),
                geo_name=name,
                geo_level="metro",
                state_code=r.get("state_code", ""),
                redfin_table_id=r.get("metro_table_id", ""),
                redfin_metro_code=metro_code,
                parent_geo_id="",
                parent_geo_level="state",
                include_redfin="1",
                include_bls_laus="1",
                include_bls_ces="1",
                source_method="generated_from_macro_hierarchy",
                needs_review="True",
                notes="Parent state may be ambiguous for multi-state metros; review needed.",
            )

    # Counties
    county_required = {"county_name", "county_table_id", "state_code"}
    if county_required <= set(macro.columns):
        counties = macro[
            [
                "county_name",
                "county_table_id",
                "state_code",
                "state_name",
                "parent_cbsa_metro_code",
                "cbsa_metro_name" if "cbsa_metro_name" in macro.columns else "parent_cbsa_metro_name",
            ]
        ].drop_duplicates()

        for _, r in counties.iterrows():
            name = r["county_name"]
            if not name or (norm(name), "county") not in scope:
                continue

            state_geo_id = make_geo_id(r.get("state_name", ""), "state", r["state_code"])
            append_row(
                rows,
                geo_id=make_geo_id(name, "county", r["state_code"]),
                geo_name=name,
                geo_level="county",
                state_code=r["state_code"],
                redfin_table_id=r["county_table_id"],
                redfin_metro_code=r.get("parent_cbsa_metro_code", ""),
                parent_geo_id=state_geo_id,
                parent_geo_level="state",
                include_redfin="1",
                include_census="1",
                include_bls_laus="1",
                source_method="generated_from_macro_hierarchy",
            )

    return pd.DataFrame(rows)


def build_local_rows(local: pd.DataFrame, local_scope: pd.DataFrame) -> pd.DataFrame:
    scope = include_set(local_scope)
    rows: list[dict] = []

    required = {"zip_region", "zip_table_id", "zip5", "state_code"}
    missing = required - set(local.columns)
    if missing:
        raise ValueError(f"Local context missing required columns: {missing}")

    for _, r in local.drop_duplicates(subset=["zip_region", "zip5", "state_code"]).iterrows():
        name = r["zip_region"]
        if not name or (norm(name), "zip") not in scope:
            continue

        county_name = r.get("county_name", "")
        parent_county_geo_id = make_geo_id(county_name, "county", r["state_code"]) if county_name else ""

        append_row(
            rows,
            geo_id=make_geo_id(name, "zip", r["state_code"]),
            geo_name=name,
            geo_level="zip",
            state_code=r["state_code"],
            redfin_table_id=r["zip_table_id"],
            redfin_metro_code=r.get("parent_cbsa_metro_code", ""),
            parent_geo_id=parent_county_geo_id,
            parent_geo_level="county",
            include_redfin="1",
            source_method="generated_from_local_context",
            needs_review=str(r.get("needs_review", "False")),
            notes=f"zip5={r['zip5']}; county={county_name}",
        )

    return pd.DataFrame(rows)


def apply_overrides(manifest: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    if overrides.empty:
        return manifest

    if "geo_id" not in overrides.columns:
        raise ValueError(f"{OVERRIDES} must contain geo_id")

    out = manifest.copy()

    for _, o in overrides.iterrows():
        geo_id = o["geo_id"]
        if not geo_id:
            continue

        mask = out["geo_id"] == geo_id
        if not mask.any():
            # Add new override row if geo_id does not exist.
            new = {c: "" for c in out.columns}
            for c in out.columns:
                if c in overrides.columns and o[c] != "":
                    new[c] = o[c]
            out = pd.concat([out, pd.DataFrame([new])], ignore_index=True)
            continue

        # Update nonblank override values.
        for c in out.columns:
            if c in overrides.columns and str(o[c]).strip() != "":
                out.loc[mask, c] = o[c]

    return out


def main() -> int:
    macro = read_csv(MACRO_HIERARCHY)
    local = read_csv(LOCAL_CONTEXT)
    macro_scope = read_csv(MACRO_SCOPE)
    local_scope = read_csv(LOCAL_SCOPE)
    overrides = read_csv(OVERRIDES, required=False)

    macro_rows = build_macro_rows(macro, macro_scope)
    local_rows = build_local_rows(local, local_scope)

    manifest = pd.concat([macro_rows, local_rows], ignore_index=True)

    if manifest.empty:
        raise SystemExit("Generated manifest is empty. Check scope include flags.")

    manifest = manifest.drop_duplicates(subset=["geo_id"], keep="first")
    manifest = apply_overrides(manifest, overrides)

    manifest = manifest.sort_values(["geo_level", "state_code", "geo_name"]).reset_index(drop=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(OUT, index=False)

    print(f"[geo-manifest] wrote {len(manifest):,} rows -> {OUT}")
    print("\n[geo-manifest] counts by geo_level:")
    print(manifest["geo_level"].value_counts().to_string())

    print("\n[geo-manifest] include flags:")
    for c in [x for x in manifest.columns if x.startswith("include_")]:
        print(f"  {c}: {int((manifest[c].astype(str) == '1').sum())}")

    print(f"\n[geo-manifest] needs_review={int(manifest['needs_review'].astype(str).str.lower().isin(['true','1','yes']).sum())}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
