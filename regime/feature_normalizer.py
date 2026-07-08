from __future__ import annotations
# regime/feature_normalizer.py

from pathlib import Path

import pandas as pd

from regime.config_loader import load_regime_config
from regime.feature_engine import build_feature_matrix


NORMALIZATION_REGISTRY = Path("config/normalization_registry.csv")


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _load_normalization_registry() -> pd.DataFrame:
    if not NORMALIZATION_REGISTRY.exists():
        raise FileNotFoundError(f"Missing normalization registry: {NORMALIZATION_REGISTRY}")

    df = pd.read_csv(NORMALIZATION_REGISTRY, dtype=str).fillna("")

    required = {
        "policy_scope",
        "policy_key",
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

    if not ((df["policy_scope"] == "global") & (df["policy_key"] == "*")).any():
        raise ValueError("normalization_registry.csv must include global,* default row")

    return df


def _source_family_from_metric_key(metric_key: str) -> str:
    value = str(metric_key).strip()

    if value.startswith("redfin_"):
        return "redfin"
    if value.startswith("acs1_") or value.startswith("acs5_"):
        return "acs"
    if value.startswith("ces_"):
        return "ces"
    if value.startswith("laus_"):
        return "laus"
    if value.startswith("fred_unemployment"):
        return "fred_unemp"
    if value.startswith("fred_"):
        return "fred_macro"
    if value.startswith("derived_"):
        return "derived"
    if value.startswith("bea_"):
        return "bea"
    if value.startswith("bps_"):
        return "bps"
    if value.startswith("nrc_"):
        return "nrc"

    return "unknown"


def _feature_source_family_map() -> pd.DataFrame:
    config = load_regime_config(validate=True)

    feature_map = (
        config.features[["feature_key", "metric_key"]]
        .drop_duplicates()
        .copy()
    )
    feature_map["source_family"] = feature_map["metric_key"].map(_source_family_from_metric_key)

    # One feature_key should resolve to one source family after canonical collapse.
    feature_map = feature_map.drop_duplicates(subset=["feature_key"], keep="first")

    return feature_map[["feature_key", "source_family"]]


def _policy_for_features(features: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    base = features[["feature_key"]].drop_duplicates().copy()
    base = base.merge(_feature_source_family_map(), on="feature_key", how="left")
    base["source_family"] = base["source_family"].fillna("unknown")

    global_policy = (
        registry[
            (registry["policy_scope"] == "global")
            & (registry["policy_key"] == "*")
        ]
        .iloc[0]
        .to_dict()
    )

    for col, val in global_policy.items():
        if col not in {"policy_scope", "policy_key"}:
            base[col] = val

    source_policies = registry[registry["policy_scope"] == "source_family"].copy()
    source_policies = source_policies.rename(columns={"policy_key": "source_family"})
    source_policies = source_policies.drop(columns=["policy_scope"])

    out = base.merge(
        source_policies,
        on="source_family",
        how="left",
        suffixes=("", "_source"),
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
        source_col = f"{col}_source"
        if source_col in out.columns:
            out[col] = out[source_col].where(out[source_col].astype(str) != "", out[col])
            out = out.drop(columns=[source_col])

    feature_policies = registry[registry["policy_scope"] == "feature_key"].copy()
    feature_policies = feature_policies.rename(columns={"policy_key": "feature_key"})
    feature_policies = feature_policies.drop(columns=["policy_scope"])

    out = out.merge(
        feature_policies,
        on="feature_key",
        how="left",
        suffixes=("", "_feature"),
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
        feature_col = f"{col}_feature"
        if feature_col in out.columns:
            out[col] = out[feature_col].where(out[feature_col].astype(str) != "", out[col])
            out = out.drop(columns=[feature_col])

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
            "source_family",
            "raw_feature_value",
            "percentile",
            "feature_score",
            "normalization_method",
            "score_direction",
            "lookback_periods",
            "min_periods",
        ]
    ].dropna(subset=["feature_score"])
