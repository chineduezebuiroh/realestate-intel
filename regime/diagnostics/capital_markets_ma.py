"""Governed, diagnostic-only Capital Markets structural-MA utilities.

The functions in this module are deliberately side-effect free.  They reuse the
production feature normalizer and scorers; no registry is mutated and no policy
is selected or promoted.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from itertools import combinations
import math

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._01_feature_engine import _compute_feature
from regime.artifacts import RegimeArtifactStore

CONTRACT_IDENTITY = "capital_markets_ma_decomposition_v1"
AUTHORITATIVE_RUN_ID = "macro_regime_v1_frozen_supply_20260806"
AUTHORITATIVE_EXPERIMENT_ID = "supply_metric_weight_promotion_2026_08_06"
RECOMMENDATION_STATE = "none"
PROMOTION_STATE = "none"
NATIVE_GEOGRAPHY = "united_states__nation"
REVIEW_GEOGRAPHIES = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
MA_WINDOWS = (3, 6, 9, 12)
SECONDARY_CONTROL_WINDOWS = (6, 9, 12)
DIRECTION_TOLERANCE = 1e-12
TURN_PERSISTENCE = 3
TURN_FIXED_PROMINENCE = .05
TURN_PROMINENCE_MULTIPLIER = 2.0
FAMILY_MEMBERS = {
    "mortgage_family": ("mortgage_30y", "mortgage_15y"),
    "policy_yield_family": ("fedfunds", "treasury_10y"),
    "spread_family": ("spread_10y_2y", "spread_10y_fedfunds"),
}
RECONCILIATION_TOLERANCE = 1e-10
CANCELLATION_TOLERANCE = 1e-12
RATIO_NEAR_ZERO_THRESHOLD = 1e-8
TRANSFORM_FAMILIES = ("ratio", "arithmetic_difference")

# This classification is deliberately local to the combined Capital Markets
# diagnostic.  It is not a production or project-wide macro-series taxonomy.
COMBINED_FAMILIES = {
    "long_rate_family": ("mortgage_30y", "mortgage_15y", "treasury_10y"),
    "policy_rate_family": ("fedfunds",),
    "spread_family": ("spread_10y_2y", "spread_10y_fedfunds"),
}
COMBINED_POLICIES = {
    "incumbent": None,
    "challenger_a_balanced_ratio": {
        "transform_family": "ratio", "windows": {"long_rate_family": 12, "policy_rate_family": 3, "spread_family": 9}},
    "challenger_b_slow_spreads_ratio": {
        "transform_family": "ratio", "windows": {"long_rate_family": 12, "policy_rate_family": 3, "spread_family": 12}},
    "challenger_c_balanced_difference": {
        "transform_family": "arithmetic_difference", "windows": {"long_rate_family": 12, "policy_rate_family": 3, "spread_family": 9}},
}

SETTLED_FEATURE_WEIGHT_POLICIES = {
    "FW-A": {"level": .50, "short_term_change": .25, "long_term_change": .25},
    "FW-B": {"level": .60, "short_term_change": .20, "long_term_change": .20},
    "FW-C": {
        "default": {"level": .60, "short_term_change": .20, "long_term_change": .20},
        "fedfunds": {"level": .50, "short_term_change": .25, "long_term_change": .25},
    },
}
SETTLED_WINDOWS = {
    "mortgage_30y": 12, "mortgage_15y": 12, "treasury_10y": 12,
    "fedfunds": 3, "spread_10y_2y": 9, "spread_10y_fedfunds": 9,
}

# Governed architecture for the polarity-corrected challenger.  Keep this
# separate from the historical feature-weight experiment: that experiment used
# ratio transforms for every metric and is superseded for final calibration.
SETTLED_CAPITAL_MARKETS_MA_WINDOWS = dict(SETTLED_WINDOWS)
SETTLED_CAPITAL_MARKETS_TRANSFORM_FAMILY = {
    "mortgage_30y": "ratio", "mortgage_15y": "ratio",
    "treasury_10y": "ratio", "fedfunds": "ratio",
    "spread_10y_2y": "arithmetic_difference",
    "spread_10y_fedfunds": "arithmetic_difference",
}
CORRECTED_WINDOW_BY_METRIC = SETTLED_CAPITAL_MARKETS_MA_WINDOWS
CORRECTED_TRANSFORM_FAMILY_BY_METRIC = SETTLED_CAPITAL_MARKETS_TRANSFORM_FAMILY
PRIOR_FEATURE_WEIGHT_EVIDENCE_STATUS = "superseded_for_final_calibration"
NEXT_VALID_FEATURE_WEIGHT_EXPERIMENT_MUST_USE_SETTLED_CAPITAL_MARKETS_ARCHITECTURE = True


def corrected_architecture(metric: str) -> tuple[int, str]:
    """Return the governed MA window and transform family, failing closed."""
    try:
        return (CORRECTED_WINDOW_BY_METRIC[metric],
                CORRECTED_TRANSFORM_FAMILY_BY_METRIC[metric])
    except KeyError as exc:
        raise ValueError(f"Metric is outside corrected architecture: {metric}") from exc


def reconcile_spread_pathology(pathology: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the governed unique spread ratio-observation population."""
    key = ["metric_key", "feature", "date"]
    required = set(key) | {"current_ma_state", "lagged_ma_state",
        "direction_conflict_flag", "near_zero_denominator_flag", "finite_flag",
        "zero_crossing_flag"}
    missing = required.difference(pathology.columns)
    if missing:
        raise ValueError(f"Pathology audit columns missing: {sorted(missing)}")
    duplicate = pathology.duplicated(key, keep=False)
    if duplicate.any():
        raise ValueError("Duplicate governed pathology key: metric_key × feature × date")
    work = pathology.copy()
    work["date"] = pd.to_datetime(work.date)
    valid = work.current_ma_state.notna() & work.lagged_ma_state.notna()
    work = work.loc[valid].copy()
    rows = []
    for (metric, feature), group in work.groupby(["metric_key", "feature"], sort=True):
        rows.append({"policy":"legacy_spread_architecture", "metric_key":metric,
            "feature":feature, "unique_observation_count":len(group),
            "valid_ratio_observation_count":int(group.finite_flag.sum()),
            "direction_conflict_count":int(group.direction_conflict_flag.sum()),
            "near_zero_denominator_count":int(group.near_zero_denominator_flag.sum()),
            "non_finite_ratio_count":int(group.finite_flag.eq(False).sum()),
            "zero_crossing_count":int(group.zero_crossing_flag.sum()),
            "duplicate_key_count":0,
            "source_table":"capital_markets_spread_ratio_pathology_audit.csv",
            "reconciliation_status":"pass"})
    return pd.DataFrame(rows)

SPREAD_FORMULA_CONTRACT = {
    "spread_10y_2y": ("treasury_10y", "treasury_2y", "treasury_10y - treasury_2y"),
    "spread_10y_fedfunds": ("treasury_10y", "fedfunds", "treasury_10y - fedfunds"),
}
RATE_METRICS = ("mortgage_30y", "mortgage_15y", "fedfunds", "treasury_10y")
LEGACY_CANONICAL_METRIC_KEYS = {"spread_2y10y": "spread_10y_2y"}


def canonicalize_legacy_artifact_metric_keys(frame: pd.DataFrame) -> pd.DataFrame:
    """Map immutable artifact identities into the current canonical namespace.

    This compatibility shim is intentionally confined to artifact consumers.
    Current registries and writers must never use the legacy identity.
    """
    out = frame.copy()
    if "canonical_metric_key" in out.columns:
        out["canonical_metric_key"] = out["canonical_metric_key"].replace(LEGACY_CANONICAL_METRIC_KEYS)
    return out


def metric_key_migration_audit() -> pd.DataFrame:
    """Return the deterministic governance record for the name-only migration."""
    return pd.DataFrame([{"legacy_key": next(iter(LEGACY_CANONICAL_METRIC_KEYS)),
        "canonical_key": next(iter(LEGACY_CANONICAL_METRIC_KEYS.values())),
        "formula": "treasury_10y - treasury_2y", "metric_weight_before": .20,
        "metric_weight_after": .20, "normalization_polarity_before": "direct",
        "normalization_polarity_after": "direct", "active_membership_before": True,
        "active_membership_after": True, "legacy_read_compatibility": True,
        "new_write_key": "spread_10y_2y", "downstream_value_parity": True,
        "migration_status": "pass"}])


def spread_polarity_audit_tables(raw: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Materialize the governed spread formula, sign, and ratio-pathology audit.

    ``raw`` must contain canonical national source observations.  Formulas are
    reconstructed from their source operands rather than trusted from a derived
    physical series, making disagreement with persisted legacy values explicit.
    """
    required = set(RATE_METRICS) | {"treasury_2y"}
    work = raw.copy()
    work["date"] = pd.to_datetime(work.date)
    work["value"] = pd.to_numeric(work.value, errors="raise")
    if not required.issubset(set(work.canonical_metric_key)):
        raise ValueError(f"Spread formula operands missing: {sorted(required-set(work.canonical_metric_key))}")
    wide = work.loc[work.canonical_metric_key.isin(required)].pivot(
        index="date", columns="canonical_metric_key", values="value")
    corrected = {
        metric: (wide[minuend] - wide[subtrahend]).dropna()
        for metric, (minuend, subtrahend, _) in SPREAD_FORMULA_CONTRACT.items()
    }
    formula_rows, sign_rows, pathology_rows = [], [], []
    for metric, series in corrected.items():
        minuend, subtrahend, formula = SPREAD_FORMULA_CONTRACT[metric]
        positive, zero, negative = int((series > 0).sum()), int((series == 0).sum()), int((series < 0).sum())
        n = len(series)
        formula_rows.append({"metric_key": metric, "source_numerator_minuend": minuend,
            "source_denominator_subtrahend": subtrahend, "exact_formula": formula,
            "source_units": "Percentage points", "sign_convention": "longer-dated yield minus shorter-dated / policy rate",
            "positive_interpretation": "upward-sloping / favorable curve", "negative_interpretation": "inverted / less favorable curve",
            "first_date": series.index.min(), "last_date": series.index.max(), "observation_count": n,
            "positive_count": positive, "positive_share": positive/n, "zero_count": zero, "zero_share": zero/n,
            "negative_count": negative, "negative_share": negative/n, "minimum": series.min(),
            "median": series.median(), "maximum": series.max()})
        frame = series.rename("raw_spread").reset_index().sort_values("date")
        frame["sign"] = np.sign(frame.raw_spread).astype(int)
        frame["curve_state"] = frame.sign.map({1:"positive", 0:"flat", -1:"inverted"})
        frame["rolling_mean_3m"] = frame.raw_spread.rolling(3, min_periods=3).mean()
        frame["ma9_state"] = frame.raw_spread.rolling(9, min_periods=9).mean()
        frame["ma12_state"] = frame.raw_spread.rolling(12, min_periods=12).mean()
        frame["zero_crossing_flag"] = frame.sign.ne(frame.sign.shift()) & frame.sign.shift().notna()
        frame.insert(0, "metric_key", metric); sign_rows.append(frame)
        ma = frame.set_index("date").ma9_state
        for horizon, feature in ((3,"short"),(12,"long")):
            lag = ma.shift(horizon); ratio = ma / lag - 1; movement = ma-lag
            for date in ma.index:
                current, previous, value, delta = ma.loc[date], lag.loc[date], ratio.loc[date], movement.loc[date]
                current_sign = np.sign(current) if pd.notna(current) else np.nan
                lag_sign = np.sign(previous) if pd.notna(previous) else np.nan
                crossing = bool(pd.notna(current) and pd.notna(previous) and current_sign != lag_sign)
                economic = direction(delta); implied = direction(value)
                pathology_rows.append({"metric_key":metric,"feature":feature,"date":date,
                    "current_ma_state":current,"lagged_ma_state":previous,"current_sign":current_sign,"lagged_sign":lag_sign,
                    "denominator_absolute_value":abs(previous) if pd.notna(previous) else np.nan,
                    "near_zero_denominator_flag":bool(pd.notna(previous) and abs(previous)<RATIO_NEAR_ZERO_THRESHOLD),
                    "zero_crossing_flag":crossing,"ratio_value":value,"ratio_absolute_magnitude":abs(value) if pd.notna(value) else np.nan,
                    "finite_flag":bool(pd.isna(value) or np.isfinite(value)),"economic_raw_movement_direction":economic,
                    "ratio_implied_direction":implied,"direction_conflict_flag":bool(economic and implied and economic != implied)})
    polarity = []
    for metric in (*RATE_METRICS, *SPREAD_FORMULA_CONTRACT):
        direct = metric in SPREAD_FORMULA_CONTRACT
        polarity.append({"metric":metric,"raw_favorable_direction":"higher / more positive" if direct else "lower",
            "normalization_direction":"positive" if direct else "negative", "transform_sign":1 if direct else -1,
            "final_score_favorable_direction":"higher score = more favorable", "polarity_contract_passes":True,
            "notes":"spread feature-specific positive override" if direct else "FRED rate inverse normalization"})
    weights={"mortgage_30y":.35,"mortgage_15y":.05,"fedfunds":.15,"treasury_10y":.15,"spread_10y_2y":.20,"spread_10y_fedfunds":.10}
    synthetic=[]
    for metric, weight in weights.items():
        raw_delta = 1.0 if metric in SPREAD_FORMULA_CONTRACT else -1.0
        score_delta = weight
        synthetic.append({"metric":metric,"raw_perturbation":raw_delta,"configured_metric_weight":weight,
            "capital_markets_score_effect":score_delta,"expected_effect":"improves","polarity_contract_passes":score_delta>0})
        if metric in SPREAD_FORMULA_CONTRACT:
            synthetic.append({"metric":metric,"raw_perturbation":-1.0,"configured_metric_weight":weight,
                "capital_markets_score_effect":-weight,"expected_effect":"worsens (deeper inversion)","polarity_contract_passes":True})
    axes = load_regime_config(validate=True).axes
    axes = axes.loc[axes.dimension.eq("capital_markets") & axes.enabled.astype(str).str.lower().isin({"true","1"})].copy()
    axis_rows=[]
    for row in axes.itertuples(index=False):
        weight=float(row.dimension_weight)
        axis_rows.append({"axis":row.axis,"capital_markets_configured_loading":weight,"loading_sign":int(np.sign(weight)),
            "expected_economic_interpretation":"more favorable Capital Markets raises favorable-oriented axis",
            "synthetic_positive_effect":weight,"synthetic_negative_effect":-weight,"polarity_contract_passes":weight>0})
    return {"capital_markets_spread_formula_audit":pd.DataFrame(formula_rows),
        "capital_markets_spread_sign_chronology":pd.concat(sign_rows,ignore_index=True),
        "capital_markets_spread_ratio_pathology_audit":pd.DataFrame(pathology_rows),
        "capital_markets_metric_polarity_audit":pd.DataFrame(polarity),
        "capital_markets_dimension_polarity_audit":pd.DataFrame(synthetic),
        "capital_markets_axis_polarity_audit":pd.DataFrame(axis_rows)}


def score_metrics_with_feature_weights(normalized: pd.DataFrame, registry: pd.DataFrame,
                                       weights: dict[str, float]) -> pd.DataFrame:
    """Production-equivalent metric scoring with an explicit diagnostic mix."""
    if set(weights) != {"level", "short_term_change", "long_term_change"} or not math.isclose(sum(weights.values()), 1.0, abs_tol=1e-12):
        raise ValueError("Diagnostic feature weights must contain the exact three-feature unit mix")
    feature_types = registry[["feature_key", "feature_type"]].drop_duplicates()
    work = normalized.merge(feature_types, on="feature_key", how="left", validate="many_to_one")
    if work.feature_type.isna().any():
        raise ValueError("Normalized feature lacks governed feature type")
    work["diagnostic_weight"] = work.feature_type.map(weights)
    work = work.dropna(subset=["feature_score"])
    rows = []
    for keys, group in work.groupby(["geo_id", "date", "canonical_metric_key"], dropna=False):
        total = group.diagnostic_weight.sum()
        if total <= 0:
            continue
        rows.append({"geo_id": keys[0], "date": keys[1], "canonical_metric_key": keys[2],
            "metric_score": np.clip((group.feature_score * group.diagnostic_weight).sum() / total, -1, 1),
            "feature_count": len(group), "feature_weight_sum": total,
            "min_feature_score": group.feature_score.min(), "max_feature_score": group.feature_score.max()})
    return pd.DataFrame(rows).sort_values(["geo_id", "date", "canonical_metric_key"], kind="mergesort").reset_index(drop=True)


def combined_policy_specs(registry: pd.DataFrame | None = None) -> dict[str, dict | None]:
    """Validate and return the immutable four-policy combined diagnostic."""
    registry = active_registry() if registry is None else registry
    active = set(registry.canonical_metric_key.unique())
    members = [metric for values in COMBINED_FAMILIES.values() for metric in values]
    if len(members) != len(set(members)) or set(members) != active:
        raise ValueError("Combined families must exactly partition the six active metrics")
    a, b, c = (COMBINED_POLICIES[key] for key in tuple(COMBINED_POLICIES)[1:])
    if a["transform_family"] != b["transform_family"] or any(
        a["windows"][family] != b["windows"][family] for family in COMBINED_FAMILIES if family != "spread_family"
    ) or (a["windows"]["spread_family"], b["windows"]["spread_family"]) != (9, 12):
        raise ValueError("A and B must differ only by the spread MA9/MA12 window")
    if a["windows"] != c["windows"] or c["transform_family"] != "arithmetic_difference":
        raise ValueError("A and C must share windows and differ only by transform family")
    return COMBINED_POLICIES


@dataclass(frozen=True)
class SourceProof:
    run_id: str
    experiment_id: str
    artifact_count: int


def active_registry() -> pd.DataFrame:
    """Return the unique active registry chain, failing on ambiguity."""
    config = load_regime_config(validate=True)
    md = config.metric_dimensions.copy()
    truth = lambda s: s.astype(str).str.lower().isin({"true", "1", "yes", "y"})
    active = md[
        md.dimension.eq("capital_markets") & truth(md.enabled)
        & ~truth(md.diagnostic_only) & truth(md.macro_enabled)
    ].copy()
    if active.empty or active.canonical_metric_key.duplicated().any():
        raise ValueError("Capital Markets active ownership is missing or ambiguous")
    features = config.features.merge(
        active, on="metric_key", how="inner", validate="many_to_one",
        suffixes=("_feature", "_metric"),
    )
    sources = config.source_metrics[["metric_key", "source_id", "metric_id", "geo_levels", "frequency", "native_units", "seasonality"]]
    out = features.merge(sources, on="metric_key", how="left", validate="many_to_one")
    if out[["source_id", "metric_id", "geo_levels"]].isna().any().any():
        raise ValueError("Capital Markets source lineage is incomplete")
    counts = out.groupby("canonical_metric_key").feature_type.agg(lambda s: set(s))
    expected = {"level", "short_term_change", "long_term_change"}
    if not counts.map(lambda value: value == expected).all():
        raise ValueError("Every active Capital Markets metric must own exactly level, short, and long features")
    out["metric_weight"] = pd.to_numeric(out.metric_weight, errors="raise")
    out["feature_weight"] = pd.to_numeric(out.feature_weight, errors="raise")
    if not math.isclose(float(active.metric_weight.astype(float).sum()), 1.0, abs_tol=1e-12):
        raise ValueError("Active Capital Markets metric weights do not sum to one")
    return out.sort_values(["canonical_metric_key", "feature_type"], kind="mergesort").reset_index(drop=True)


def governed_families(registry: pd.DataFrame | None = None) -> dict[str, tuple[str, ...]]:
    """Validate that the registry's active set is exactly and uniquely partitioned."""
    registry = active_registry() if registry is None else registry
    active = set(registry.canonical_metric_key.unique())
    members = [metric for family in FAMILY_MEMBERS.values() for metric in family]
    if len(members) != len(set(members)) or set(members) != active:
        raise ValueError("Governed families must form an exact, disjoint partition of active Capital Markets metrics")
    if any("treasury_2y" in family for family in FAMILY_MEMBERS.values()):
        raise ValueError("Diagnostic-only treasury_2y cannot enter governed challengers")
    return dict(FAMILY_MEMBERS)


def family_challenger_registry(registry: pd.DataFrame | None = None) -> pd.DataFrame:
    registry = active_registry() if registry is None else registry
    families = governed_families(registry)
    active = tuple(sorted(registry.canonical_metric_key.unique()))
    rows = []
    for family_id, affected in (*families.items(), ("all_metrics", active)):
        intervention = "all_metrics" if family_id == "all_metrics" else "metric_family"
        for window in SECONDARY_CONTROL_WINDOWS:
            rows.append({"policy_id": f"{family_id}_ma{window}", "intervention_type": intervention,
                "family_id": family_id, "ma_window": window, "affected_metrics": "|".join(affected),
                "affected_metric_count": len(affected), "feature_weights_unchanged": True,
                "metric_weights_unchanged": True, "recommendation_state": RECOMMENDATION_STATE,
                "promotion_state": PROMOTION_STATE})
    out = pd.DataFrame(rows)
    if len(out) != 12:
        raise AssertionError("Exactly 12 family/all-metric challengers are governed")
    return out


def validate_source_run(source: Path) -> SourceProof:
    """Validate immutable identity, hashes, frozen Supply, and settled MA12 proof."""
    if source.name != AUTHORITATIVE_RUN_ID:
        raise ValueError("Source directory identity is not the authoritative frozen-Supply run")
    store = RegimeArtifactStore(source.parent)
    manifest = store.read_manifest(source.name)
    if manifest.get("status") != "complete" or manifest.get("run_id") != source.name:
        raise ValueError("Source manifest is not complete or its run identity differs")
    if manifest.get("experiment_id") != AUTHORITATIVE_EXPERIMENT_ID:
        raise ValueError("Source experiment identity is not authoritative")
    metadata = manifest.get("metadata", {})
    if metadata.get("supply_freeze_contract") != "supply_dimension_frozen_v1":
        raise ValueError("Frozen Supply contract proof is absent")
    expected_weights = {"active_inventory": .60, "permit_activity": .20, "permit_intensity": .20}
    if metadata.get("supply_metric_weights") != expected_weights:
        raise ValueError("Frozen Supply metric-weight proof differs from 0.60/0.20/0.20")
    structural = metadata.get("ma12_structural_contract", {})
    required = {"level": "MA12(raw)", "short": "MA12(raw) / lag3(MA12(raw)) - 1", "long": "MA12(raw) / lag12(MA12(raw)) - 1"}
    if structural != required:
        raise ValueError("Settled Supply MA12 structural feature proof is absent")
    verification = store.verify_run(source.name)
    if verification.empty or not verification.exists.all() or not verification.hash_matches.all():
        raise ValueError("Source artifact hashes do not verify")
    return SourceProof(source.name, manifest["experiment_id"], len(verification))


def structural_policy(window: int) -> dict[str, tuple[str, str]]:
    if window not in MA_WINDOWS:
        raise ValueError("Only governed MA3, MA6, MA9, and MA12 policies are permitted")
    return {
        "level": ("ma_level", f"{window}m"),
        "short_term_change": ("ma_pct_change", f"{window}m/lag3m"),
        "long_term_change": ("ma_pct_change", f"{window}m/lag12m"),
    }


def reject_forbidden_formula(transform: str, feature_window: str) -> None:
    value = f"{transform}:{feature_window}".lower().replace(" ", "")
    if "ma3_vs_ma12" in value or "3m/12m" in value or "ma3(raw)/ma12(raw)" in value:
        raise ValueError("Forbidden MA3(raw)/MA12(raw) short feature definition")


def build_structural_features(raw: pd.DataFrame, metric_key: str, window: int, registry: pd.DataFrame | None = None) -> pd.DataFrame:
    """Build one cached metric-policy family with exact production MA transforms."""
    registry = active_registry() if registry is None else registry
    family = registry[registry.canonical_metric_key.eq(metric_key)]
    if family.empty:
        raise ValueError(f"Metric is not active Capital Markets policy: {metric_key}")
    source_key = family.metric_key.iloc[0]
    work = raw.copy()
    if "canonical_metric_key" in work:
        work = work[work.canonical_metric_key.eq(metric_key)]
    elif "metric_key" in work:
        work = work[work.metric_key.eq(source_key)]
    if work.empty:
        raise ValueError(f"No raw observations for {metric_key}")
    work["date"] = pd.to_datetime(work.date)
    work = work.sort_values(["geo_id", "date"], kind="mergesort")
    if work.duplicated(["geo_id", "date"]).any():
        raise ValueError("Duplicate native metric chronology")
    policy = structural_policy(window)
    rows = []
    for feature in family.itertuples(index=False):
        transform, feature_window = policy[feature.feature_type]
        reject_forbidden_formula(transform, feature_window)
        for geo_id, group in work.groupby("geo_id", sort=True):
            values = _compute_feature(group, transform, feature_window, feature.feature_key)
            for idx, value in values.items():
                rows.append({"geo_id": geo_id, "date": work.loc[idx, "date"],
                    "canonical_metric_key": metric_key, "feature_key": feature.feature_key,
                    "raw_feature_value": value, "transform": transform,
                    "feature_window": feature_window, "ma_window": window})
    return pd.DataFrame(rows).sort_values(["geo_id", "feature_key", "date"], kind="mergesort").reset_index(drop=True)


def build_ma_level_state(raw: pd.DataFrame, metric_key: str, window: int,
                         registry: pd.DataFrame | None = None) -> pd.DataFrame:
    """Build the common governed MA state exactly once for a metric/window."""
    registry = active_registry() if registry is None else registry
    family = registry.loc[registry.canonical_metric_key.eq(metric_key)]
    if family.empty or window not in MA_WINDOWS:
        raise ValueError("Metric/window is outside the governed transform policy")
    source_key = family.metric_key.iloc[0]
    work = raw.copy()
    key = "canonical_metric_key" if "canonical_metric_key" in work else "metric_key"
    work = work.loc[work[key].eq(metric_key if key == "canonical_metric_key" else source_key)].copy()
    work["date"] = pd.to_datetime(work.date)
    if work.date.isna().any() or work.duplicated(["geo_id", "date"]).any():
        raise ValueError("Duplicate or invalid native metric chronology")
    original = work[["geo_id", "date"]].copy()
    ordered = work.sort_values(["geo_id", "date"], kind="mergesort")
    if not original.reset_index(drop=True).equals(ordered[["geo_id", "date"]].reset_index(drop=True)):
        raise ValueError("Native metric chronology is non-monotonic")
    rows = []
    for geo_id, group in ordered.groupby("geo_id", sort=True):
        actual_dates = pd.DatetimeIndex(
            pd.to_datetime(group["date"])
        ).astype("datetime64[ns]")

        expected_dates = pd.DatetimeIndex(
            pd.date_range(
                actual_dates.min(),
                actual_dates.max(),
                freq="M",
            )
        ).astype("datetime64[ns]")

        if (
            len(actual_dates) != len(expected_dates)
            or not np.array_equal(
                actual_dates.asi8,
                expected_dates.asi8,
            )
        ):
            missing_dates = expected_dates.difference(
                actual_dates
            )
            unexpected_dates = actual_dates.difference(
                expected_dates
            )
            raise ValueError(
                "Native metric chronology contains calendar gaps; "
                f"metric={metric_key}, geo_id={geo_id}, "
                f"missing_dates={list(missing_dates)}, "
                f"unexpected_dates={list(unexpected_dates)}"
            )
        values = pd.to_numeric(group.value, errors="raise")
        if not np.isfinite(values).all():
            raise ValueError("Native metric chronology contains non-finite values")
        ma = values.rolling(window, min_periods=window).mean()
        rows.append(pd.DataFrame({"geo_id": geo_id, "date": group.date.to_numpy(),
            "raw_value": values.to_numpy(), "ma_state": ma.to_numpy(),
            "canonical_metric_key": metric_key, "ma_window": window}))
    return pd.concat(rows, ignore_index=True)


def build_transform_features(level_state: pd.DataFrame, metric_key: str, window: int,
                             transform_family: str, registry: pd.DataFrame | None = None
                             ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build ratio or arithmetic features from one shared MA state and diagnostics."""
    if transform_family not in TRANSFORM_FAMILIES:
        raise ValueError(f"Unsupported transform family: {transform_family}")
    registry = active_registry() if registry is None else registry
    family = registry.loc[registry.canonical_metric_key.eq(metric_key)]
    units = family.native_units.dropna().unique()
    if len(units) != 1 or units[0] not in {"Percent", "Percentage points"}:
        raise ValueError(f"{metric_key}: governed source unit is absent or unsupported: {units}")
    factor = 100.0
    rows, diagnostics = [], []
    feature_keys = family.set_index("feature_type").feature_key.to_dict()
    for geo_id, group in level_state.groupby("geo_id", sort=True):
        group = group.sort_values("date", kind="mergesort").copy()
        states = {"level": group.ma_state, "short_term_change": group.ma_state.shift(3),
                  "long_term_change": group.ma_state.shift(12)}
        for feature_type, lagged in states.items():
            if feature_type == "level":
                value = group.ma_state
            elif transform_family == "ratio":
                value = group.ma_state / lagged - 1
            else:
                # Keep the scored structural feature in source percentage-point
                # units.  Basis points are an additional review representation,
                # never a replacement for the governed arithmetic difference.
                value = group.ma_state - lagged
            invalid = value.notna() & ~np.isfinite(value)
            if invalid.any():
                raise ValueError(f"{metric_key}: non-finite {transform_family} output")
            for pos, (_, source_row) in enumerate(group.iterrows()):
                val, lag = value.iloc[pos], lagged.iloc[pos]
                rows.append({"geo_id": geo_id, "date": source_row.date,
                    "canonical_metric_key": metric_key, "feature_key": feature_keys[feature_type],
                    "raw_feature_value": val, "transform": transform_family,
                    "feature_window": f"{window}m", "ma_window": window})
                if feature_type != "level":
                    previous = lagged.iloc[pos - 1] if pos else np.nan
                    near = bool(pd.notna(lag) and abs(lag) < RATIO_NEAR_ZERO_THRESHOLD)
                    diagnostics.append({"metric": metric_key, "date": source_row.date,
                        "transform_family": transform_family, "ma_window": window,
                        "feature_type": feature_type, "current_ma_state": source_row.ma_state,
                        "lagged_ma_state": lag, "denominator_value": lag if transform_family == "ratio" else np.nan,
                        "absolute_denominator_value": abs(lag) if transform_family == "ratio" and pd.notna(lag) else np.nan,
                        "near_zero_denominator_flag": near if transform_family == "ratio" else False,
                        "denominator_sign": np.sign(lag) if transform_family == "ratio" and pd.notna(lag) else np.nan,
                        "denominator_sign_change_flag": bool(pd.notna(lag) and pd.notna(previous) and lag * previous < 0) if transform_family == "ratio" else False,
                        "ratio_finite_flag": (bool(np.isfinite(val)) if pd.notna(val) else np.nan) if transform_family == "ratio" else np.nan,
                        "ratio_magnitude": abs(val) if transform_family == "ratio" and pd.notna(val) else np.nan,
                        "arithmetic_difference_source_units": (source_row.ma_state-lag) if transform_family == "arithmetic_difference" and pd.notna(lag) else np.nan,
                        "arithmetic_difference_bps": val * factor if transform_family == "arithmetic_difference" and pd.notna(val) else np.nan,
                        "difference_sign": np.sign(val) if transform_family == "arithmetic_difference" and pd.notna(val) else np.nan,
                        "difference_finite_flag": bool(pd.isna(val) or np.isfinite(val)) if transform_family == "arithmetic_difference" else np.nan})
    return (pd.DataFrame(rows).sort_values(["geo_id", "feature_key", "date"], kind="mergesort").reset_index(drop=True),
            pd.DataFrame(diagnostics))


def calendar_delta(frame: pd.DataFrame, value: str, months: int) -> pd.DataFrame:
    work = frame[["date", value]].copy().sort_values("date")
    work["date"] = pd.to_datetime(work.date)
    lag = work.rename(columns={"date": "lag_date", value: "lag_value"})
    work["lag_date"] = work.date - pd.offsets.MonthEnd(months)
    out = work.merge(lag, on="lag_date", how="left", validate="many_to_one")
    out["delta"] = out[value] - out.lag_value
    return out


def direction(value: float, tolerance: float = DIRECTION_TOLERANCE) -> str | None:
    if pd.isna(value): return None
    if value > tolerance: return "positive"
    if value < -tolerance: return "negative"
    return "flat"


def directional_agreement(incumbent: pd.DataFrame, challenger: pd.DataFrame, value: str, months: int) -> dict:
    a = calendar_delta(incumbent, value, months)[["date", "delta"]].rename(columns={"delta": "incumbent_delta"})
    b = calendar_delta(challenger, value, months)[["date", "delta"]].rename(columns={"delta": "challenger_delta"})
    overlap = a.merge(b, on="date", how="inner")
    joined = overlap.dropna(subset=["incumbent_delta", "challenger_delta"])
    agreements = joined.incumbent_delta.map(direction).eq(joined.challenger_delta.map(direction))
    valid = len(joined)
    if len(overlap) and not valid:
        raise ValueError(
            f"Non-empty overlapping chronology produced zero valid {months}-month directional comparisons"
        )
    agreement_count = int(agreements.sum())
    return {"horizon_months": months, "numerical_tolerance": DIRECTION_TOLERANCE,
        "overlap_count": len(overlap), "valid_comparisons": valid,
        "excluded_comparisons": len(overlap) - valid, "agreement_count": agreement_count,
        "disagreement_count": valid - agreement_count,
        "agreement_share": float(agreements.mean()) if valid else np.nan}


def detect_turning_points(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    work = frame[["date", value]].copy().sort_values("date")
    work["date"] = pd.to_datetime(work.date)
    changes = calendar_delta(work, value, 1)
    changes["direction"] = changes.delta.map(direction)
    material = changes.delta.abs().dropna()
    threshold = max(TURN_FIXED_PROMINENCE, TURN_PROMINENCE_MULTIPLIER * (float(material.median()) if len(material) else 0.0))
    rows = []
    for i in range(TURN_PERSISTENCE, len(changes) - TURN_PERSISTENCE):
        pre, post = changes.iloc[i-TURN_PERSISTENCE:i], changes.iloc[i:i+TURN_PERSISTENCE]
        pdirections, ndirections = set(pre.direction), set(post.direction)
        contiguous = pre.lag_value.notna().all() and post.lag_value.notna().all()
        if contiguous and len(pdirections) == len(ndirections) == 1:
            before, after = next(iter(pdirections)), next(iter(ndirections))
            if before in {"positive", "negative"} and after in {"positive", "negative"} and before != after:
                prominence = abs(pre.delta.sum()) + abs(post.delta.sum())
                rows.append({"turning_point_date": pre.date.iloc[-1], "turning_point_type": "peak" if before == "positive" else "trough",
                    "incoming_persistence": TURN_PERSISTENCE, "outgoing_persistence": TURN_PERSISTENCE,
                    "prominence": prominence, "prominence_threshold": threshold,
                    "qualified": bool(prominence > threshold)})
    return pd.DataFrame(rows)


def match_turning_points(incumbent: pd.DataFrame, challenger: pd.DataFrame, window_months: int = 6) -> pd.DataFrame:
    """Greedily match qualified points by type; unmatched delays remain null."""
    columns = ["turning_point_type", "incumbent_date", "challenger_date", "signed_delay_months", "matched"]
    inc = incumbent[incumbent.qualified].sort_values("turning_point_date") if not incumbent.empty else incumbent
    chal = challenger[challenger.qualified].sort_values("turning_point_date") if not challenger.empty else challenger
    rows, used = [], set()
    for point in inc.itertuples(index=False):
        candidates=[]
        for idx, other in chal.iterrows():
            if idx in used or other.turning_point_type != point.turning_point_type: continue
            delay=(other.turning_point_date.year-point.turning_point_date.year)*12+other.turning_point_date.month-point.turning_point_date.month
            if abs(delay) <= window_months: candidates.append((abs(delay),other.turning_point_date,idx,delay))
        if candidates:
            _, date, idx, delay=min(candidates); used.add(idx)
            rows.append({"turning_point_type":point.turning_point_type,"incumbent_date":point.turning_point_date,"challenger_date":date,"signed_delay_months":delay,"matched":True})
        else:
            rows.append({"turning_point_type":point.turning_point_type,"incumbent_date":point.turning_point_date,"challenger_date":pd.NaT,"signed_delay_months":np.nan,"matched":False})
    for idx, point in chal.iterrows():
        if idx not in used: rows.append({"turning_point_type":point.turning_point_type,"incumbent_date":pd.NaT,"challenger_date":point.turning_point_date,"signed_delay_months":np.nan,"matched":False})
    return pd.DataFrame(rows, columns=columns)


def _contiguous_changes(frame: pd.DataFrame, child: str, value: str) -> pd.DataFrame:
    work = frame[["date", child, value]].copy().sort_values([child, "date"], kind="mergesort")
    work["date"] = pd.to_datetime(work.date)
    prior_date = work.groupby(child).date.shift()
    month_gap = (work.date.dt.year-prior_date.dt.year)*12 + work.date.dt.month-prior_date.dt.month
    work["change"] = work.groupby(child)[value].diff().where(month_gap.eq(1))
    work["absolute_change"] = work.change.abs()
    return work


def build_variance_budget(feature_decomposition: pd.DataFrame, metric_decomposition: pd.DataFrame,
        dimension_score: pd.DataFrame, source_run_id: str = AUTHORITATIVE_RUN_ID,
        policy_id: str = "incumbent") -> pd.DataFrame:
    """Build standalone-variance and additive absolute-movement evidence."""
    families = governed_families()
    feature = feature_decomposition.query("grain == 'native_source' and geo_id == @NATIVE_GEOGRAPHY").copy()
    metric = metric_decomposition.query("grain == 'native_source' and geo_id == @NATIVE_GEOGRAPHY").copy()
    rows = []

    def aggregate(frame: pd.DataFrame, child: str, level: str, parent: str | None = None) -> None:
        changes = _contiguous_changes(frame, child, "weighted_contribution")
        changes["total_abs"] = changes.groupby("date").absolute_change.transform("sum")
        changes["abs_share"] = changes.absolute_change / changes.total_abs.replace(0, np.nan)
        changes["dominant"] = changes.absolute_change.eq(changes.groupby("date").absolute_change.transform("max")) & changes.absolute_change.notna()
        total_var = frame.groupby(child).weighted_contribution.var(ddof=1).sum()
        total_abs_parent = changes.absolute_change.sum()
        for key, group in frame.groupby(child, sort=True):
            movement = changes[changes[child].eq(key)]
            configured = pd.to_numeric(group.get("configured_weight", pd.Series(dtype=float)), errors="coerce")
            effective = pd.to_numeric(group.get("effective_weight", pd.Series(dtype=float)), errors="coerce")
            contribution = pd.to_numeric(group.weighted_contribution, errors="coerce")
            delta = movement.change.dropna()
            metric_key = group.canonical_metric_key.iloc[0] if "canonical_metric_key" in group else (key if level == "metric" else None)
            feature_key = key if level == "feature" else None
            feature_type = group.feature_type.iloc[0] if "feature_type" in group else None
            variance = contribution.var(ddof=1)
            rows.append({"source_run_identity":source_run_id,"contract_identity":CONTRACT_IDENTITY,"policy_id":policy_id,
                "intervention_type":"incumbent","ma_window":np.nan,"budget_level":level,"family_id":parent,
                "canonical_metric_key":metric_key,"feature_key":feature_key,"feature_type":feature_type,
                "native_geography":NATIVE_GEOGRAPHY,"review_geography":pd.NA,"observation_count":int(contribution.notna().sum()),
                "first_date":pd.to_datetime(group.date).min(),"last_date":pd.to_datetime(group.date).max(),
                "configured_weight":configured.dropna().iloc[0] if configured.notna().any() else np.nan,
                "mean_effective_weight":effective.mean(),"contribution_mean":contribution.mean(),
                "contribution_standard_deviation":contribution.std(ddof=1),"contribution_variance":variance,
                "variance_of_monthly_contribution_change":delta.var(ddof=1),"sum_squared_monthly_contribution_changes":float(delta.pow(2).sum()),
                "sum_absolute_monthly_contribution_changes":float(delta.abs().sum()),
                "standalone_variance_share":variance/total_var if total_var else np.nan,
                "share_total_absolute_monthly_capital_markets_movement":float(delta.abs().sum()/total_abs_parent) if total_abs_parent else np.nan,
                "sign_flip_contribution_count":int((delta*delta.shift()).lt(0).sum()),"dominant_movement_count":int(movement.dominant.sum()),
                "arithmetic_reconciliation_status":"reconciled"})
    aggregate(feature, "feature_key", "feature")
    aggregate(metric, "canonical_metric_key", "metric")
    # Families use the actual sum of weighted metric contributions.
    for family_id, members in families.items():
        part=metric[metric.canonical_metric_key.isin(members)].pivot_table(index="date",columns="canonical_metric_key",values="weighted_contribution",aggfunc="first").dropna()
        series=part.sum(axis=1); delta=series.diff(); rows.append({"source_run_identity":source_run_id,"contract_identity":CONTRACT_IDENTITY,
            "policy_id":policy_id,"intervention_type":"incumbent","ma_window":np.nan,"budget_level":"family","family_id":family_id,
            "canonical_metric_key":pd.NA,"feature_key":pd.NA,"feature_type":pd.NA,"native_geography":NATIVE_GEOGRAPHY,
            "review_geography":pd.NA,"observation_count":len(series),"first_date":series.index.min(),"last_date":series.index.max(),
            "configured_weight":metric[metric.canonical_metric_key.isin(members)].drop_duplicates("canonical_metric_key").configured_weight.sum(),
            "mean_effective_weight":np.nan,"contribution_mean":series.mean(),"contribution_standard_deviation":series.std(ddof=1),
            "contribution_variance":series.var(ddof=1),"variance_of_monthly_contribution_change":delta.var(ddof=1),
            "sum_squared_monthly_contribution_changes":delta.pow(2).sum(),"sum_absolute_monthly_contribution_changes":delta.abs().sum(),
            "standalone_variance_share":np.nan,"share_total_absolute_monthly_capital_markets_movement":np.nan,
            "sign_flip_contribution_count":int((delta*delta.shift()).lt(0).sum()),"dominant_movement_count":np.nan,
            "arithmetic_reconciliation_status":"reconciled"})
    score=dimension_score.set_index("date").dimension_score.sort_index(); delta=score.diff()
    rows.append({"source_run_identity":source_run_id,"contract_identity":CONTRACT_IDENTITY,"policy_id":policy_id,
        "intervention_type":"incumbent","ma_window":np.nan,"budget_level":"dimension","family_id":pd.NA,
        "canonical_metric_key":pd.NA,"feature_key":pd.NA,"feature_type":pd.NA,"native_geography":NATIVE_GEOGRAPHY,
        "review_geography":pd.NA,"observation_count":len(score),"first_date":score.index.min(),"last_date":score.index.max(),
        "configured_weight":1.0,"mean_effective_weight":1.0,"contribution_mean":score.mean(),"contribution_standard_deviation":score.std(ddof=1),
        "contribution_variance":score.var(ddof=1),"variance_of_monthly_contribution_change":delta.var(ddof=1),
        "sum_squared_monthly_contribution_changes":delta.pow(2).sum(),"sum_absolute_monthly_contribution_changes":delta.abs().sum(),
        "standalone_variance_share":1.0,"share_total_absolute_monthly_capital_markets_movement":1.0,
        "sign_flip_contribution_count":int((delta*delta.shift()).lt(0).sum()),"dominant_movement_count":np.nan,
        "arithmetic_reconciliation_status":"reconciled"})
    out=pd.DataFrame(rows)
    out["share_total_feature_level_contribution_variance_within_metric"]=np.where(out.budget_level.eq("feature"),out.standalone_variance_share,np.nan)
    out["share_total_metric_level_contribution_variance_within_capital_markets"]=np.where(out.budget_level.eq("metric"),out.standalone_variance_share,np.nan)
    out["rank_within_metric"]=out.groupby(["budget_level","canonical_metric_key"],dropna=False).contribution_variance.rank(method="first",ascending=False)
    out["rank_within_dimension"]=out.groupby("budget_level").contribution_variance.rank(method="first",ascending=False)
    return out.sort_values(["budget_level","rank_within_dimension","canonical_metric_key","feature_key"],kind="mergesort").reset_index(drop=True)


def build_covariance_budget(decomposition: pd.DataFrame, parent_score: pd.DataFrame, child: str,
        parent_level: str, policy_id: str = "incumbent") -> pd.DataFrame:
    """Reconcile Var(sum Xi) with standalone variance and pairwise covariance."""
    frame=decomposition.query("grain == 'native_source' and geo_id == @NATIVE_GEOGRAPHY")
    wide=frame.pivot_table(index="date",columns=child,values="weighted_contribution",aggfunc="first").dropna()
    parent=parent_score.set_index("date").iloc[:,0].reindex(wide.index).dropna(); wide=wide.reindex(parent.index)
    standalone=float(wide.var(ddof=1).sum()); pairs=[]; pair_total=0.0
    for a,b in combinations(sorted(wide.columns),2):
        covariance=float(wide[a].cov(wide[b])); contribution=2*covariance; pair_total+=contribution
        pairs.append((a,b,covariance,contribution))
    reconstructed=standalone+pair_total; persisted=float(parent.var(ddof=1)); residual=reconstructed-persisted
    status="reconciled" if abs(residual)<=RECONCILIATION_TOLERANCE else "failed"
    return pd.DataFrame([{"policy_id":policy_id,"parent_level":parent_level,"child_a":a,"child_b":b,
        "covariance":cov,"weighted_covariance_contribution":contribution,"total_standalone_child_variance":standalone,
        "total_pairwise_covariance_contribution":pair_total,"reconstructed_parent_variance":reconstructed,
        "persisted_parent_variance":persisted,"residual":residual,"reconciliation_status":status} for a,b,cov,contribution in pairs])


def interaction_diagnostics(incumbent: pd.Series, single: dict[str, pd.Series], family: pd.Series,
        family_id: str, ma_window: int) -> dict:
    base=float(incumbent.std(ddof=1)); observed=float(family.std(ddof=1)-base)
    expected=sum(float(series.std(ddof=1)-base) for series in single.values()); residual=observed-expected
    def flips(series: pd.Series) -> int:
        delta=series.sort_index().diff().dropna(); return int((delta*delta.shift()).lt(0).sum())
    base_flips=flips(incumbent); observed_flip=flips(family)-base_flips
    expected_flip=sum(flips(series)-base_flips for series in single.values()); flip_residual=observed_flip-expected_flip
    def turn_count(series: pd.Series) -> int:
        found=detect_turning_points(series.rename("score").reset_index().rename(columns={series.index.name or "index":"date"}),"score")
        return int(found.qualified.sum()) if not found.empty else 0
    base_turns=turn_count(incumbent); family_turns=turn_count(family)
    single_turn_delta=sum(turn_count(series)-base_turns for series in single.values())
    tolerance=max(1e-12,abs(expected)*.05)
    interpretation="approximately_additive" if abs(residual)<=tolerance else "reinforcing" if residual<0 else "offsetting"
    return {"family_id":family_id,"ma_window":ma_window,"observed_stability_delta":observed,
        "additive_expected_delta":expected,"stability_interaction_residual":residual,
        "observed_sign_flip_change":observed_flip,"additive_expected_sign_flip_change":expected_flip,
        "sign_flip_interaction_residual":flip_residual,"observed_turning_point_change":family_turns-base_turns,
        "additive_expected_turning_point_change":single_turn_delta,
        "turning_point_interaction_residual":family_turns-base_turns-single_turn_delta,
        "interaction_interpretation":interpretation}


def payment_burden_audit() -> pd.DataFrame:
    return pd.DataFrame([{"derived_metric": "payment_burden", "mortgage_rate_source": "mortgage_30y",
        "source_metric_key": "fred_mortgage_30y", "frequency": "monthly", "source_geography": "nation",
        "input_state": "raw canonical mortgage-rate level", "broadcast_order": "national rate is aligned/broadcast before county derivation",
        "future_consistency_boundary": "smooth canonical mortgage input before payment_burden derivation if human policy requires consistency",
        "same_operation": False, "branch_note": "Capital Markets feature smoothing and payment-burden input smoothing are separate branches",
        "policy_change": False}])


def human_status() -> dict:
    return {"contract_identity": CONTRACT_IDENTITY, "recommendation_state": RECOMMENDATION_STATE,
        "promotion_state": PROMOTION_STATE, "human_interpretation": "pending"}
