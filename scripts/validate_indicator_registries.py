from __future__ import annotations
#scripts/validate_indicator_registries.py

from pathlib import Path

import duckdb
import pandas as pd


SERVING_DB = Path("data/market_serving.duckdb")
SOURCE_REGISTRY = Path("config/source_metric_registry.csv")
REGIME_REGISTRY = Path("config/indicator_regime_registry.csv")


VALID_DIRECTIONS = {"positive", "negative"}
VALID_AXES = {"demand", "supply"}
VALID_DIMENSIONS = {
    "demand",
    "supply",
    "affordability",
    "transaction_activity",
    "liquidity",
    "price",
    "capital_markets",
}
VALID_TRANSFORMS = {
    "level_zscore",
    "yoy_zscore",
    "mom_zscore",
    "none",
}


def fail(msg: str) -> None:
    raise SystemExit(f"[registry:validate] FAIL {msg}")


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        fail(f"missing file: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def truthy(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def main() -> int:
    src = read_csv(SOURCE_REGISTRY)
    reg = read_csv(REGIME_REGISTRY)

    required_src_cols = {
        "metric_key", "source_id", "metric_id", "metric_name", "geo_levels",
        "frequency", "native_units", "seasonality", "forecastable",
    }
    required_reg_cols = {
        "metric_key", "dimension", "axis", "transform", "direction", "weight",
        "required", "macro_enabled", "local_enabled",
    }

    missing = required_src_cols - set(src.columns)
    if missing:
        fail(f"{SOURCE_REGISTRY} missing columns: {sorted(missing)}")

    missing = required_reg_cols - set(reg.columns)
    if missing:
        fail(f"{REGIME_REGISTRY} missing columns: {sorted(missing)}")

    if src["metric_key"].duplicated().any():
        fail("duplicate metric_key in source registry:\n" + src[src["metric_key"].duplicated(keep=False)].to_string(index=False))

    if reg["metric_key"].duplicated().any():
        fail("duplicate metric_key in regime registry:\n" + reg[reg["metric_key"].duplicated(keep=False)].to_string(index=False))

    missing_keys = sorted(set(reg["metric_key"]) - set(src["metric_key"]))
    if missing_keys:
        fail(f"regime registry metric_keys missing from source registry: {missing_keys}")

    bad_dims = sorted(set(reg["dimension"]) - VALID_DIMENSIONS)
    if bad_dims:
        fail(f"invalid dimensions: {bad_dims}")

    bad_axes = sorted(set(reg["axis"]) - VALID_AXES)
    if bad_axes:
        fail(f"invalid axes: {bad_axes}")

    bad_dirs = sorted(set(reg["direction"]) - VALID_DIRECTIONS)
    if bad_dirs:
        fail(f"invalid directions: {bad_dirs}")

    bad_transforms = sorted(set(reg["transform"]) - VALID_TRANSFORMS)
    if bad_transforms:
        fail(f"invalid transforms: {bad_transforms}")

    reg["weight_num"] = pd.to_numeric(reg["weight"], errors="coerce")
    if reg["weight_num"].isna().any():
        fail("non-numeric weights:\n" + reg[reg["weight_num"].isna()].to_string(index=False))

    if (reg["weight_num"] < 0).any():
        fail("negative weights found")

    enabled = reg[truthy(reg["macro_enabled"]) | truthy(reg["local_enabled"])]
    zero_weight_enabled = enabled[enabled["weight_num"] <= 0]
    if not zero_weight_enabled.empty:
        fail("enabled rows with zero/nonpositive weight:\n" + zero_weight_enabled.to_string(index=False))

    con = duckdb.connect(str(SERVING_DB))
    facts = con.execute("""
        SELECT DISTINCT source_id, metric_id
        FROM fact_timeseries
    """).fetchdf()

    src_pairs = src[["source_id", "metric_id", "metric_key"]].drop_duplicates()
    merged = src_pairs.merge(facts, on=["source_id", "metric_id"], how="left", indicator=True)
    missing_fact_metrics = merged[merged["_merge"] == "left_only"]

    if not missing_fact_metrics.empty:
        fail(
            "source registry metrics missing from serving DB:\n"
            + missing_fact_metrics[["metric_key", "source_id", "metric_id"]].to_string(index=False)
        )

    print("[registry:validate] regime rows:", len(reg))
    print("[registry:validate] source metric rows:", len(src))
    print("[registry:validate] dimensions:")
    print(reg.groupby(["axis", "dimension"]).size().reset_index(name="rows").to_string(index=False))

    print("[registry:validate] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
