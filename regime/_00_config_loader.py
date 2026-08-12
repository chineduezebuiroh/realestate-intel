from __future__ import annotations
# regime/_00_config_loader.py

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd


SOURCE_REGISTRY = Path("config/source_metric_registry.csv")
FEATURE_REGISTRY = Path("config/feature_registry.csv")
METRIC_DIMENSION_REGISTRY = Path("config/metric_dimension_registry.csv")
AXIS_REGISTRY = Path("config/axis_registry.csv")
SERVING_DB = Path("data/market_serving.duckdb")


@dataclass(frozen=True)
class RegimeConfig:
    source_metrics: pd.DataFrame
    features: pd.DataFrame
    metric_dimensions: pd.DataFrame
    axes: pd.DataFrame


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _require_columns(df: pd.DataFrame, path: Path, required: set[str]) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")


def load_regime_config(validate: bool = True) -> RegimeConfig:
    source_metrics = _read_csv(SOURCE_REGISTRY)
    features = _read_csv(FEATURE_REGISTRY)
    metric_dimensions = _read_csv(METRIC_DIMENSION_REGISTRY)
    axes = _read_csv(AXIS_REGISTRY)

    config = RegimeConfig(
        source_metrics=source_metrics,
        features=features,
        metric_dimensions=metric_dimensions,
        axes=axes,
    )

    if validate:
        validate_regime_config(config)

    return config


def validate_regime_config(config: RegimeConfig) -> None:
    _require_columns(
        config.source_metrics,
        SOURCE_REGISTRY,
        {
            "metric_key",
            "source_id",
            "metric_id",
            "metric_name",
            "geo_levels",
            "frequency",
            "native_units",
            "seasonality",
            "forecastable",
        },
    )

    _require_columns(
        config.features,
        FEATURE_REGISTRY,
        {
            "feature_key",
            "metric_key",
            "feature_type",
            "transform",
            "feature_weight",
            "feature_window",
            "dimension_context",
        },
    )

    _require_columns(
        config.metric_dimensions,
        METRIC_DIMENSION_REGISTRY,
        {
            "metric_key",
            "canonical_metric_key",
            "dimension",
            "subcomponent",
            "metric_weight",
            "demand_block",
            "block_weight",
            "source_priority",
            "merge_strategy",
            "enabled",
            "diagnostic_only",
            "required",
            "macro_enabled",
            "local_enabled",
        },
    )

    _require_columns(
        config.axes,
        AXIS_REGISTRY,
        {"axis", "dimension", "dimension_weight", "enabled"},
    )

    if config.source_metrics["metric_key"].duplicated().any():
        dupes = config.source_metrics[
            config.source_metrics["metric_key"].duplicated(keep=False)
        ]
        raise ValueError(f"Duplicate metric_key in source registry:\n{dupes}")

    if config.features["feature_key"].duplicated().any():
        dupes = config.features[
            config.features["feature_key"].duplicated(keep=False)
        ]
        raise ValueError(f"Duplicate feature_key in feature registry:\n{dupes}")

    source_keys = set(config.source_metrics["metric_key"])
    feature_keys = set(config.features["metric_key"])
    dim_keys = set(config.metric_dimensions["metric_key"])

    missing_feature_refs = sorted(feature_keys - source_keys)
    if missing_feature_refs:
        raise ValueError(f"Feature registry references unknown metric_key(s): {missing_feature_refs}")

    missing_dim_refs = sorted(dim_keys - source_keys)
    if missing_dim_refs:
        raise ValueError(f"Metric dimension registry references unknown metric_key(s): {missing_dim_refs}")


    # Feature coverage is required at the canonical metric level, not the physical
    # source metric level. Example: acs1_population and acs5_population both resolve
    # to canonical_metric_key=population, so only one population_* feature family is needed.
    feature_dim = config.features.merge(
        config.metric_dimensions[["metric_key", "canonical_metric_key"]],
        on="metric_key",
        how="left",
    )
    
    feature_canon_keys = set(feature_dim["canonical_metric_key"].dropna())
    enabled_dim = config.metric_dimensions[
        _truthy(config.metric_dimensions["enabled"])
        & ~_truthy(config.metric_dimensions["diagnostic_only"])
    ]
    required_canon_keys = set(enabled_dim["canonical_metric_key"].dropna())
    
    missing_feature_coverage = sorted(required_canon_keys - feature_canon_keys)
    if missing_feature_coverage:
        raise ValueError(
            "Canonical metrics missing feature rows: "
            f"{missing_feature_coverage}"
        )


    missing_dimension_coverage = sorted(source_keys - dim_keys)
    if missing_dimension_coverage:
        raise ValueError(f"Source metrics missing dimension rows: {missing_dimension_coverage}")

    for name, df, col in [
        ("feature_weight", config.features, "feature_weight"),
        ("metric_weight", config.metric_dimensions, "metric_weight"),
        ("source_priority", config.metric_dimensions, "source_priority"),
        ("dimension_weight", config.axes, "dimension_weight"),
    ]:
        parsed = pd.to_numeric(df[col], errors="coerce")
        if parsed.isna().any():
            bad = df[parsed.isna()]
            raise ValueError(f"Non-numeric {name} values:\n{bad}")
        if name != "source_priority" and (parsed < 0).any():
            bad = df[parsed < 0]
            raise ValueError(f"Negative {name} values:\n{bad}")

    axis_dims = set(config.axes["dimension"])
    
    unknown_axis_dims = sorted(axis_dims - set(config.metric_dimensions["dimension"]))
    if unknown_axis_dims:
        raise ValueError(
            "Axis registry references dimensions not present in metric_dimension_registry: "
            f"{unknown_axis_dims}"
        )

    if SERVING_DB.exists():
        con = duckdb.connect(str(SERVING_DB))
        facts = con.execute("""
            SELECT DISTINCT source_id, metric_id
            FROM fact_timeseries
        """).fetchdf()
        con.close()

        check_source_metrics = config.source_metrics[
            config.source_metrics["source_id"] != "derived"
        ].copy()
        
        merged = check_source_metrics.merge(
            facts,
            on=["source_id", "metric_id"],
            how="left",
            indicator=True,
        )

        missing_facts = merged[merged["_merge"] == "left_only"]
        if not missing_facts.empty:
            raise ValueError(
                "Source registry metrics missing from serving DB:\n"
                + missing_facts[["metric_key", "source_id", "metric_id"]]
                    .to_string(index=False)
            )


    active_dims = (
        config.metric_dimensions[
            _truthy(config.metric_dimensions["enabled"])
            & ~_truthy(config.metric_dimensions["diagnostic_only"])
            & _truthy(config.metric_dimensions["macro_enabled"])
        ]
        .copy()
    )

    duplicate_dimension_rows = active_dims.duplicated(
        subset=["metric_key", "canonical_metric_key", "dimension"], keep=False
    )
    if duplicate_dimension_rows.any():
        raise ValueError(
            "Duplicate active metric-to-dimension registry rows:\n"
            + active_dims.loc[duplicate_dimension_rows, [
                "metric_key", "canonical_metric_key", "dimension", "metric_weight"
            ]].to_string(index=False)
        )

    active_dims["metric_weight"] = pd.to_numeric(
        active_dims["metric_weight"],
        errors="coerce",
    )

    active_canonical_dims = active_dims[
        ["dimension", "canonical_metric_key", "metric_weight"]
    ].drop_duplicates()

    conflicts = (
        active_canonical_dims
        .groupby(["dimension", "canonical_metric_key"])["metric_weight"]
        .nunique()
        .reset_index(name="weight_count")
    )

    conflicts = conflicts[conflicts["weight_count"] > 1]
    if not conflicts.empty:
        raise ValueError(
            "Conflicting metric weights for canonical metric/dimension pairs:\n"
            + conflicts.to_string(index=False)
        )

    active_canonical_dims = active_canonical_dims.drop_duplicates(
        subset=["dimension", "canonical_metric_key"],
        keep="first",
    )

    dim_weight_sums = (
        active_canonical_dims
        .groupby("dimension")["metric_weight"]
        .sum()
        .reset_index(name="metric_weight_sum")
    )

    bad_dim_weights = dim_weight_sums[
        (dim_weight_sums["metric_weight_sum"] - 1.0).abs() > 0.001
    ]

    if not bad_dim_weights.empty:
        raise ValueError(
            "Active canonical metric weights must sum to 1.0 by dimension:\n"
            + bad_dim_weights.to_string(index=False)
        )

    bad_enabled_zero = active_dims[active_dims["metric_weight"] <= 0]
    if not bad_enabled_zero.empty:
        raise ValueError(
            "Enabled non-diagnostic metric rows must have positive metric_weight:\n"
            + bad_enabled_zero[
                ["metric_key", "canonical_metric_key", "dimension", "metric_weight"]
            ].to_string(index=False)
        )


def build_registry_resolution(config: RegimeConfig | None = None) -> pd.DataFrame:
    if config is None:
        config = load_regime_config(validate=True)

    df = (
        config.features
        .merge(config.source_metrics, on="metric_key", how="left")
        .merge(config.metric_dimensions, on="metric_key", how="left")
        .merge(config.axes, on="dimension", how="left", suffixes=("", "_axis"))
    )

    return df
