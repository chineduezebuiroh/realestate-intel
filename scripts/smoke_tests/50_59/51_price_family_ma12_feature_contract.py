from __future__ import annotations
# scripts/smoke_tests/50_59/51_price_family_ma12_feature_contract.py

import json
import math
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from regime.experiments.linked_price_family_features import (
    LEVEL_WINDOW,
    LONG_LAG_PERIODS,
    PRICE_FAMILY_METRICS,
    SHORT_LAG_PERIODS,
    build_linked_price_family_features,
)

CANDIDATE_ID = "price_family_ma12_structural_linked"
LEGACY_CANDIDATE_ID = "price_family_ma12_momentum_lag3"
OUTPUT_DIR = Path(
    "artifacts/regime/comparisons/price_family_ma12_structural_linked/phase1_feature_contract"
)
SOURCE_METRICS = ("median_sale_price", "median_ppsf")
DERIVED_METRICS = ("price_to_income", "payment_burden")


def _build_fixture() -> pd.DataFrame:
    dates = pd.date_range("2020-01-31", periods=30, freq="M")
    rows: list[dict[str, object]] = []
    for geo_index, geo_id in enumerate(("geo_alpha", "geo_beta")):
        for index, date in enumerate(dates):
            price = 300_000.0 + geo_index * 80_000.0 + index * 4_250.0 + (index % 5) * 725.0
            ppsf = 240.0 + geo_index * 45.0 + index * 1.75 + (index % 4) * 0.5
            rows.extend(
                [
                    {"geo_id": geo_id, "date": date, "canonical_metric_key": "median_sale_price", "value": price},
                    {"geo_id": geo_id, "date": date, "canonical_metric_key": "median_ppsf", "value": ppsf},
                ]
            )
        for annual_date, income in (
            (pd.Timestamp("2020-01-31"), 100_000.0 + geo_index * 15_000.0),
            (pd.Timestamp("2021-01-31"), 104_000.0 + geo_index * 15_000.0),
            (pd.Timestamp("2022-01-31"), 109_000.0 + geo_index * 15_000.0),
        ):
            rows.append({"geo_id": geo_id, "date": annual_date, "canonical_metric_key": "median_household_income", "value": income})
    for index, date in enumerate(dates):
        rows.append({"geo_id": "national", "date": date, "canonical_metric_key": "mortgage_30y", "value": 3.1 + index * 0.04})
    return pd.DataFrame(rows)


def _assert_close(actual: float, expected: float, label: str) -> None:
    if not math.isclose(float(actual), float(expected), rel_tol=0.0, abs_tol=1e-12):
        raise AssertionError(f"{label}: expected {expected}, found {actual}")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False)


def _canonical_features(frame: pd.DataFrame) -> pd.DataFrame:
    comparable = frame.drop(columns=["price_family_experiment_id"], errors="ignore").copy()
    return comparable.sort_values(["canonical_metric_key", "geo_id", "date", "feature_component"]).reset_index(drop=True)


def _canonical_substitution_lineage(frame: pd.DataFrame) -> pd.DataFrame:
    comparable = frame.drop(columns=["substitution_id"], errors="ignore").copy()
    return comparable.sort_values(["canonical_metric_key", "geo_id", "date"]).reset_index(drop=True)


def _assert_legacy_parity(source: pd.DataFrame, preferred_result) -> None:
    legacy_result = build_linked_price_family_features(source, experiment_id=LEGACY_CANDIDATE_ID)
    pd.testing.assert_frame_equal(preferred_result.level_history, legacy_result.level_history, check_exact=True)
    pd.testing.assert_frame_equal(preferred_result.derived_metrics, legacy_result.derived_metrics, check_exact=True)
    pd.testing.assert_frame_equal(preferred_result.derived_lineage, legacy_result.derived_lineage, check_exact=True)
    pd.testing.assert_frame_equal(_canonical_features(preferred_result.feature_history), _canonical_features(legacy_result.feature_history), check_exact=True)
    pd.testing.assert_frame_equal(_canonical_substitution_lineage(preferred_result.source_substitution_lineage), _canonical_substitution_lineage(legacy_result.source_substitution_lineage), check_exact=True)


def _assert_lazy_exports_resolve() -> None:
    import regime.experiments as experiments

    expected_exports = {
        "build_smoothed_metric_features",
        "build_smoothed_metric_features_wide",
        "DEFAULT_SMOOTHING_EXPERIMENT_REGISTRY",
        "SmoothingExperiment",
        "SmoothingMetricPolicy",
        "load_smoothing_experiments",
        "apply_smoothing_experiment",
    }
    missing = expected_exports - set(experiments.__all__)
    if missing:
        raise AssertionError(f"regime.experiments.__all__ is missing public exports: {sorted(missing)}")
    for export_name in sorted(expected_exports):
        getattr(experiments, export_name)


def _assert_perturbation_isolation(source: pd.DataFrame, baseline_features: pd.DataFrame) -> dict[str, Any]:
    perturbed = source.copy()
    target_mask = (
        perturbed["geo_id"].eq("geo_alpha")
        & perturbed["canonical_metric_key"].eq("median_sale_price")
        & perturbed["date"].eq(pd.Timestamp("2021-06-30"))
    )
    if int(target_mask.sum()) != 1:
        raise AssertionError("Perturbation target row was not unique")
    perturbed.loc[target_mask, "value"] = perturbed.loc[target_mask, "value"] + 10_000.0
    perturbed_features = build_linked_price_family_features(perturbed, experiment_id=CANDIDATE_ID).feature_history

    compare_columns = [column for column in baseline_features.columns if column in perturbed_features.columns]
    key_columns = ["geo_id", "date", "canonical_metric_key", "feature_component"]
    merged = baseline_features[compare_columns].merge(
        perturbed_features[compare_columns],
        on=key_columns,
        how="outer",
        suffixes=("_baseline", "_perturbed"),
        validate="one_to_one",
        indicator=True,
    )
    if not merged["_merge"].eq("both").all():
        raise AssertionError("Perturbation changed the feature key set")

    value_pairs = [
        ("raw_feature_value_baseline", "raw_feature_value_perturbed"),
        ("source_level_value_baseline", "source_level_value_perturbed"),
        ("reference_value_baseline", "reference_value_perturbed"),
    ]
    changed = np.zeros(len(merged), dtype=bool)
    for left, right in value_pairs:
        equal_or_both_missing = merged[left].eq(merged[right]) | (merged[left].isna() & merged[right].isna())
        changed |= ~equal_or_both_missing.to_numpy()

    changed_rows = merged.loc[changed, key_columns]
    if changed_rows.empty:
        raise AssertionError(
            "Perturbing geo_alpha median_sale_price did not change any reachable feature rows"
        )

    changed_metrics = set(changed_rows["canonical_metric_key"])
    if "median_sale_price" not in changed_metrics:
        raise AssertionError(
            "Median-sale-price perturbation did not affect median_sale_price features"
        )
    if not changed_metrics.intersection(DERIVED_METRICS):
        raise AssertionError(
            "Median-sale-price perturbation did not propagate to linked derived features"
        )

    unrelated_geography_changes = changed_rows[~changed_rows["geo_id"].eq("geo_alpha")]
    if not unrelated_geography_changes.empty:
        raise AssertionError(
            "Median-sale-price perturbation changed unrelated geographies:\n"
            + unrelated_geography_changes.head(20).to_string(index=False)
        )

    reachable_metric = changed_rows["canonical_metric_key"].isin(("median_sale_price", *DERIVED_METRICS))
    if not reachable_metric.all():
        raise AssertionError(
            "Median-sale-price perturbation changed unrelated metrics:\n"
            + changed_rows.loc[~reachable_metric].head(20).to_string(index=False)
        )

    unrelated_mask = ~(merged["geo_id"].eq("geo_alpha") & merged["canonical_metric_key"].isin(("median_sale_price", *DERIVED_METRICS)))
    if changed[unrelated_mask.to_numpy()].any():
        raise AssertionError("Unrelated geographies or unrelated metrics changed after perturbation")

    metric_counts = (
        changed_rows.groupby("canonical_metric_key")
        .size()
        .sort_index()
        .astype(int)
        .to_dict()
    )
    return {
        "changed_rows": int(changed_rows.shape[0]),
        "changed_metric_counts": metric_counts,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    source = _build_fixture()
    print(f"[phase1_feature_contract] candidate={CANDIDATE_ID} legacy_identifier={LEGACY_CANDIDATE_ID}")
    first = build_linked_price_family_features(source, experiment_id=CANDIDATE_ID)
    second = build_linked_price_family_features(source, experiment_id=CANDIDATE_ID)
    pd.testing.assert_frame_equal(first.feature_history, second.feature_history, check_exact=True)
    pd.testing.assert_frame_equal(first.level_history, second.level_history, check_exact=True)
    _assert_legacy_parity(source, first)
    _assert_lazy_exports_resolve()

    features = first.feature_history
    levels = first.level_history
    key = ["geo_id", "date", "canonical_metric_key", "feature_component"]
    if features.duplicated(key).any():
        raise AssertionError("Duplicate feature keys found")
    valid = features["raw_feature_value"].dropna()
    if not np.isfinite(valid).all():
        raise AssertionError("Feature output contains non-finite values")
    valid_levels = levels["structural_level_value"].dropna()
    if not np.isfinite(valid_levels).all():
        raise AssertionError("Structural-level output contains non-finite values")
    if set(features["canonical_metric_key"].unique()) != set(PRICE_FAMILY_METRICS):
        raise AssertionError("Feature metrics differ from linked structural contract")

    formula_rows: list[dict[str, Any]] = []
    for geo_id in ("geo_alpha", "geo_beta"):
        for metric_key in SOURCE_METRICS:
            raw = source[source["geo_id"].eq(geo_id) & source["canonical_metric_key"].eq(metric_key)].sort_values("date").reset_index(drop=True)
            metric_levels = levels[levels["geo_id"].eq(geo_id) & levels["canonical_metric_key"].eq(metric_key)].sort_values("date").reset_index(drop=True)
            first_valid = metric_levels[metric_levels["structural_level_value"].notna()].iloc[0]
            expected_first_date = raw.loc[LEVEL_WINDOW - 1, "date"]
            if first_valid["date"] != expected_first_date:
                raise AssertionError(f"{geo_id}/{metric_key}: unexpected first full-window date")
            if metric_levels.loc[: LEVEL_WINDOW - 2, "structural_level_value"].notna().any():
                raise AssertionError(f"{geo_id}/{metric_key}: partial MA12 level emitted")
            for idx in (LEVEL_WINDOW - 1, LEVEL_WINDOW + 3, LEVEL_WINDOW + 12):
                expected_level = raw.loc[idx - LEVEL_WINDOW + 1 : idx, "value"].mean()
                actual_level = float(metric_levels.loc[idx, "structural_level_value"])
                _assert_close(actual_level, expected_level, f"{geo_id}/{metric_key}/level/{idx}")
            feature_slice = features[features["geo_id"].eq(geo_id) & features["canonical_metric_key"].eq(metric_key)]
            for component, lag in (("short", SHORT_LAG_PERIODS), ("long", LONG_LAG_PERIODS)):
                row_date = raw.loc[LEVEL_WINDOW - 1 + lag, "date"]
                row = feature_slice[feature_slice["date"].eq(row_date) & feature_slice["feature_component"].eq(component)].iloc[0]
                current = float(metric_levels.loc[LEVEL_WINDOW - 1 + lag, "structural_level_value"])
                reference = float(metric_levels.loc[LEVEL_WINDOW - 1, "structural_level_value"])
                _assert_close(float(row["raw_feature_value"]), current / reference - 1.0, f"{geo_id}/{metric_key}/{component}")
                _assert_close(float(row["reference_value"]), reference, f"{geo_id}/{metric_key}/{component}/reference")
            formula_rows.append({"geo_id": geo_id, "metric": metric_key, "first_level_date": expected_first_date.strftime("%Y-%m-%d")})

    duplicate_source = pd.concat([source, source.head(1)], ignore_index=True)
    try:
        build_linked_price_family_features(duplicate_source, experiment_id=CANDIDATE_ID)
    except ValueError as exc:
        duplicate_error = str(exc).splitlines()[0]
    else:
        raise AssertionError("Duplicate source keys were not rejected")

    perturbation_summary = _assert_perturbation_isolation(source, features)
    coverage = features.groupby(["canonical_metric_key", "feature_component"], as_index=False).agg(rows=("raw_feature_value", "size"), valid_rows=("raw_feature_value", "count"), first_valid_date=("date", lambda s: s[features.loc[s.index, "raw_feature_value"].notna()].min()))
    _write_csv(coverage, OUTPUT_DIR / "feature_contract_coverage.csv")
    _write_csv(pd.DataFrame(formula_rows), OUTPUT_DIR / "feature_contract_formula_samples.csv")
    summary = {"candidate_id": CANDIDATE_ID, "legacy_identifier": LEGACY_CANDIDATE_ID, "legacy_identifier_parity_validated": True, "source_rows": len(source), "feature_rows": len(features), "duplicate_protection": duplicate_error, "deterministic_rerun": True, "full_window_ma12": True, "finite_structural_levels": True, "non_finite_valid_outputs": 0, "perturbation_changed_reachable_rows": perturbation_summary["changed_rows"], "perturbation_changed_reachable_metric_counts": perturbation_summary["changed_metric_counts"], "positive_reachable_perturbation_propagation_validated": True, "lazy_public_exports_resolved": True}
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(coverage.to_string(index=False))
    print(f"[phase1_feature_contract] artifacts={OUTPUT_DIR}")
    print("[phase1_feature_contract] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
