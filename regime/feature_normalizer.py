from __future__ import annotations
# regime/feature_normalizer.py

from pathlib import Path

import pandas as pd

from regime.feature_engine import build_feature_matrix


NORMALIZATION_REGISTRY = Path("config/normalization_registry.csv")


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _load_normalization_registry() -> pd.DataFrame:
    if not NORMALIZATION_REGISTRY.exists():
        raise FileNotFoundError(f"Missing normalization registry: {NORMALIZATION_REGISTRY}")

    df = pd.read_csv(NORMALIZATION_REGISTRY, dtype=str).fillna("")

    required = {
        "feature_key",
        "normalization_method",
        "lookback_periods",
        "min_periods",
        "score_direction",
        "clip_low",
        "clip_high",
        "enabled",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{NORMALIZATION_REGISTRY} missing columns: {sorted(missing)}")

    df = df[_truthy(df["enabled"])].copy()

    if "*" not in set(df["feature_key"]):
        raise ValueError("normalization_registry.csv must include a '*' default row")

    return df


def _policy_for_features(features: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    default = registry[registry["feature_key"] == "*"].iloc[0].to_dict()
    specific = registry[registry["feature_key"] != "*"].copy()

    base = features[["feature_key"]].drop_duplicates().copy()
    for col, val in default.items():
        if col != "feature_key":
            base[col] = val

    overrides = specific.drop_duplicates(subset=["feature_key"], keep="last")
    out = base.merge(
        overrides,
        on="feature_key",
        how="left",
        suffixes=("", "_override"),
    )

    for col in [
        "normalization_method",
        "lookback_periods",
        "min_periods",
        "score_direction",
        "clip_low",
        "clip_high",
        "enabled",
        "notes",
    ]:
        override_col = f"{col}_override"
        if override_col in out.columns:
            out[col] = out[override_col].where(out[override_col].astype(str) != "", out[col])
            out = out.drop(columns=[override_col])

    return out


def _rolling_percentile(values: pd.Series, lookback_periods: int, min_periods: int) -> pd.Series:
    def percentile_last(window: pd.Series) -> float:
        current = window.iloc[-1]
        hist = window.dropna()
        if len(hist) < min_periods or pd.isna(current):
            return float("nan")

        return float((hist <= current).mean())

    return values.rolling(
        window=lookback_periods,
        min_periods=min_periods,
    ).apply(percentile_last, raw=False)


def _normalize_group(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values("date").copy()

    method = group["normalization_method"].iloc[0]
    if method != "expanding_percentile":
        raise ValueError(f"Unsupported normalization_method: {method}")

    lookback_periods = int(float(group["lookback_periods"].iloc[0]))
    min_periods = int(float(group["min_periods"].iloc[0]))
    clip_low = float(group["clip_low"].iloc[0])
    clip_high = float(group["clip_high"].iloc[0])
    direction = str(group["score_direction"].iloc[0]).strip().lower()

    percentile = _rolling_percentile(
        group["raw_feature_value"].astype(float),
        lookback_periods=lookback_periods,
        min_periods=min_periods,
    ).clip(clip_low, clip_high)

    if direction == "positive":
        score = (percentile * 2.0) - 1.0
    elif direction == "negative":
        score = ((1.0 - percentile) * 2.0) - 1.0
    else:
        raise ValueError(f"Unsupported score_direction: {direction}")

    group["percentile"] = percentile
    group["feature_score"] = score.clip(-1.0, 1.0)

    return group


def normalize_features(features: pd.DataFrame | None = None) -> pd.DataFrame:
    if features is None:
        features = build_feature_matrix()

    registry = _load_normalization_registry()
    policy = _policy_for_features(features, registry)

    df = features.merge(policy, on="feature_key", how="left")

    out = (
        df.groupby(["geo_id", "feature_key"], group_keys=False)
        .apply(_normalize_group)
        .reset_index(drop=True)
    )

    return out[
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
            "raw_feature_value",
            "percentile",
            "feature_score",
            "normalization_method",
            "score_direction",
        ]
    ].dropna(subset=["feature_score"])
