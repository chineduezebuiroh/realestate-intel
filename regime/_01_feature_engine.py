from __future__ import annotations
# regime/_01_feature_engine.py

from pathlib import Path

import duckdb
import pandas as pd
import numpy as np

from regime._00_config_loader import RegimeConfig, load_regime_config
from regime.derived_metrics import build_derived_metrics_with_lineage
from regime.canonical_metrics import resolve_canonical_metrics


SERVING_DB = Path("data/market_serving.duckdb")

CANONICAL_SOURCE_METRIC_COLUMNS = [
    "geo_id",
    "date",
    "canonical_metric_key",
    "value",
    "metric_origin",
]


def _zscore(s: pd.Series, min_obs: int = 12) -> pd.Series:
    expanding_mean = s.expanding(min_periods=min_obs).mean()
    expanding_std = s.expanding(min_periods=min_obs).std()
    return ((s - expanding_mean) / expanding_std).clip(-3, 3) / 3


def _window_to_periods(feature_window: str, default: int) -> int:
    value = str(feature_window or "").strip().lower()

    if not value:
        return default

    if value.endswith(("m", "q", "y")):
        value = value[:-1]

    try:
        periods = int(value)
    except ValueError:
        return default

    return max(periods, 1)


def _compute_feature(
    group: pd.DataFrame,
    transform: str,
    feature_window: str = "",
) -> pd.Series:
    group = group.sort_values("date")
    value = group["value"].astype(float)

    if transform == "level_zscore":
        return value

    if transform == "mom_zscore":
        periods = _window_to_periods(feature_window, default=1)
        return value.pct_change(periods)

    if transform == "qoq_zscore":
        periods = _window_to_periods(feature_window, default=1)
        return value.pct_change(periods)

    if transform == "yoy_zscore":
        periods = _window_to_periods(feature_window, default=12)
        return value.pct_change(periods)

    if transform == "rolling_yoy_zscore":
        periods = _window_to_periods(feature_window, default=3)
        return value.pct_change(periods)

    if transform == "ma12_level":
        return value.rolling(
            window=12,
            min_periods=12,
        ).mean()

    if transform == "ma3_vs_ma12_pct":
        fast_ma = value.rolling(
            window=3,
            min_periods=3,
        ).mean()

        slow_ma = value.rolling(
            window=12,
            min_periods=12,
        ).mean()

        denominator = slow_ma.replace(0.0, np.nan)

        return (fast_ma / denominator) - 1.0

    if transform == "ma12_yoy_pct":
        slow_ma = value.rolling(
            window=12,
            min_periods=12,
        ).mean()

        prior_slow_ma = slow_ma.shift(12)
        denominator = prior_slow_ma.replace(0.0, np.nan)

        return (slow_ma / denominator) - 1.0

    if transform == "none":
        return value

    raise ValueError(f"Unsupported transform: {transform}")


def load_raw_metric_series(
    config: RegimeConfig,
    db_path: str | Path = SERVING_DB,
) -> pd.DataFrame:
    source_metrics = config.source_metrics[["metric_key", "source_id", "metric_id"]]

    db_path = Path(db_path)
    
    if not db_path.is_file():
        raise FileNotFoundError(f"Serving database not found: {db_path}")
    
    con = duckdb.connect(str(db_path), read_only=True)

    facts = con.execute("""
        SELECT geo_id, date, source_id, metric_id, value
        FROM fact_timeseries
        WHERE value IS NOT NULL
    """).fetchdf()
    con.close()

    facts["date"] = pd.to_datetime(facts["date"])

    raw = facts.merge(
        source_metrics,
        on=["source_id", "metric_id"],
        how="inner",
    )

    return raw[["geo_id", "date", "metric_key", "value"]]


def _validate_canonical_source_metrics(
    observations: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = set(
        CANONICAL_SOURCE_METRIC_COLUMNS
    )

    missing = (
        required_columns
        - set(observations.columns)
    )

    if missing:
        raise ValueError(
            "Canonical source metrics are missing "
            f"required columns: {sorted(missing)}"
        )

    work = observations[
        CANONICAL_SOURCE_METRIC_COLUMNS
    ].copy()

    work["geo_id"] = (
        work["geo_id"]
        .astype(str)
        .str.strip()
    )

    work["canonical_metric_key"] = (
        work["canonical_metric_key"]
        .astype(str)
        .str.strip()
    )

    work["metric_origin"] = (
        work["metric_origin"]
        .astype(str)
        .str.strip()
    )

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["value"] = pd.to_numeric(
        work["value"],
        errors="coerce",
    )

    invalid = work[
        work["geo_id"].eq("")
        | work["canonical_metric_key"].eq("")
        | work["metric_origin"].eq("")
        | work["date"].isna()
        | work["value"].isna()
        | ~np.isfinite(work["value"])
    ]

    if not invalid.empty:
        raise ValueError(
            "Canonical source metrics contain invalid "
            "keys, dates, origins, or values:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicate_keys = work.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise ValueError(
            "Canonical source metrics contain duplicate "
            "geo/date/metric observations:\n"
            + work.loc[
                duplicate_keys
            ]
            .sort_values(
                [
                    "geo_id",
                    "canonical_metric_key",
                    "date",
                ]
            )
            .head(50)
            .to_string(index=False)
        )

    return (
        work.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def build_canonical_source_metrics_with_lineage(
    config: RegimeConfig | None = None,
    db_path: str | Path = SERVING_DB,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build the immutable canonical pre-feature observation frame and
    the derived-input lineage produced from that same frame.

    The canonical observation artifact contains both resolved source
    metrics and derived canonical metrics.
    """
    if config is None:
        config = load_regime_config(
            validate=True
        )

    raw_source = load_raw_metric_series(
        config,
        db_path=db_path,
    )

    canonical_source = (
        resolve_canonical_metrics(
            raw_source,
            config,
        )
        .copy()
    )

    required_columns = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
    }

    missing_source_columns = (
        required_columns
        - set(canonical_source.columns)
    )

    if missing_source_columns:
        raise ValueError(
            "Canonical metric resolution is missing "
            f"columns: {sorted(missing_source_columns)}"
        )

    canonical_source["metric_origin"] = (
        "canonical_source"
    )

    derived, derived_lineage = (
        build_derived_metrics_with_lineage(
            canonical_source[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "value",
                ]
            ]
        )
    )

    frames = [
        canonical_source[
            CANONICAL_SOURCE_METRIC_COLUMNS
        ]
    ]

    if not derived.empty:
        missing_derived_columns = (
            required_columns
            - set(derived.columns)
        )

        if missing_derived_columns:
            raise ValueError(
                "Derived metric output is missing "
                f"columns: "
                f"{sorted(missing_derived_columns)}"
            )

        derived = derived.copy()
        derived["metric_origin"] = "derived"

        frames.append(
            derived[
                CANONICAL_SOURCE_METRIC_COLUMNS
            ]
        )

    observations = pd.concat(
        frames,
        ignore_index=True,
    )

    observations = (
        _validate_canonical_source_metrics(
            observations
        )
    )

    return (
        observations,
        derived_lineage,
    )


def build_canonical_source_metrics(
    config: RegimeConfig | None = None,
    db_path: str | Path = SERVING_DB,
) -> pd.DataFrame:
    observations, _ = (
        build_canonical_source_metrics_with_lineage(
            config=config,
            db_path=db_path,
        )
    )

    return observations


def build_feature_matrix_with_lineage(
    config: RegimeConfig | None = None,
    db_path: str | Path = SERVING_DB,
    *,
    canonical_observations: (
        pd.DataFrame | None
    ) = None,
    derived_metric_lineage: (
        pd.DataFrame | None
    ) = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if config is None:
        config = load_regime_config(
            validate=True
        )

    if canonical_observations is None:
        (
            raw,
            resolved_derived_lineage,
        ) = (
            build_canonical_source_metrics_with_lineage(
                config=config,
                db_path=db_path,
            )
        )

        if derived_metric_lineage is not None:
            raise ValueError(
                "derived_metric_lineage cannot be supplied "
                "without canonical_observations"
            )

        derived_lineage = (
            resolved_derived_lineage
        )

    else:
        raw = _validate_canonical_source_metrics(
            canonical_observations
        )

        if derived_metric_lineage is None:
            raise ValueError(
                "derived_metric_lineage is required when "
                "canonical_observations are supplied"
            )

        derived_lineage = (
            derived_metric_lineage.copy()
        )

    feature_defs = (
        config.features
        .merge(
            config.metric_dimensions[
                [
                    "metric_key",
                    "canonical_metric_key",
                ]
            ],
            on="metric_key",
            how="left",
        )
        .drop_duplicates(
            subset=[
                "canonical_metric_key",
                "feature_type",
                "transform",
                "feature_window",
                "dimension_context",
            ]
        )
    )

    rows = []

    for _, feature_definition in (
        feature_defs.iterrows()
    ):
        metric_key = feature_definition[
            "canonical_metric_key"
        ]

        feature_key = feature_definition[
            "feature_key"
        ]

        transform = feature_definition[
            "transform"
        ]

        metric_df = raw[
            raw[
                "canonical_metric_key"
            ].eq(metric_key)
        ].copy()

        if metric_df.empty:
            continue

        metric_df["feature_key"] = (
            feature_key
        )

        metric_df["transform"] = transform

        metric_df = metric_df.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        ).copy()

        feature_window = (
            feature_definition.get(
                "feature_window",
                "",
            )
        )

        metric_df["raw_feature_value"] = (
            metric_df.groupby(
                [
                    "geo_id",
                    "canonical_metric_key",
                ],
                group_keys=False,
            )["value"]
            .transform(
                lambda series: _compute_feature(
                    pd.DataFrame(
                        {
                            "date": (
                                metric_df.loc[
                                    series.index,
                                    "date",
                                ]
                            ),
                            "value": series,
                        }
                    ),
                    transform,
                    feature_window,
                )
            )
        )

        rows.append(
            metric_df[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "feature_key",
                    "raw_feature_value",
                ]
            ]
        )

    feature_columns = [
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
        "raw_feature_value",
    ]

    if not rows:
        return (
            pd.DataFrame(
                columns=feature_columns
            ),
            derived_lineage,
        )

    output = pd.concat(
        rows,
        ignore_index=True,
    )

    output = output.dropna(
        subset=[
            "raw_feature_value",
        ]
    )

    # Preserve the legacy feature-definition and observation ordering.
    # Downstream stages must receive the same ordered frame as they did
    # before canonical source-metric persistence was introduced.
    output = output.reset_index(
        drop=True
    )

    return (
        output,
        derived_lineage,
    )


def build_feature_matrix(
    config: RegimeConfig | None = None,
    db_path: str | Path = SERVING_DB,
    *,
    canonical_observations: (
        pd.DataFrame | None
    ) = None,
    derived_metric_lineage: (
        pd.DataFrame | None
    ) = None,
) -> pd.DataFrame:
    features, _ = (
        build_feature_matrix_with_lineage(
            config=config,
            db_path=db_path,
            canonical_observations=(
                canonical_observations
            ),
            derived_metric_lineage=(
                derived_metric_lineage
            ),
        )
    )

    return features
