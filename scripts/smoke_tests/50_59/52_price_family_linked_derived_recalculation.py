from __future__ import annotations
# scripts/smoke_tests/50_59/52_price_family_linked_derived_recalculation.py

import json
import math
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END

from regime.experiments.linked_price_family_features import (
    PRICE_FAMILY_STRUCTURAL_CANDIDATES,
    build_linked_price_family_features,
    get_price_family_structural_candidate,
)
from regime.linked_price_family import (
    apply_linked_price_family_augmentation,
)

CANDIDATE_IDS = (
    "price_family_ma6_structural_linked",
    "price_family_ma9_structural_linked",
    "price_family_ma12_structural_linked",
)
OUTPUT_DIR = Path(
    "artifacts/regime/comparisons/price_family_structural_windows/phase1_linked_recalculation"
)
EXPECTED_COMPONENTS = {
    "price_to_income": {"median_sale_price", "median_household_income"},
    "payment_burden": {"median_sale_price", "median_household_income", "mortgage_30y"},
}


def _build_fixture() -> pd.DataFrame:
    dates = pd.date_range("2020-01-31", periods=30, freq=MONTH_END)
    rows: list[dict[str, object]] = []
    for geo_index, geo_id in enumerate(("geo_alpha", "geo_beta")):
        for index, date in enumerate(dates):
            rows.extend([
                {"geo_id": geo_id, "date": date, "canonical_metric_key": "median_sale_price", "value": 310_000.0 + geo_index * 90_000.0 + index * 3_900.0 + (index % 6) * 900.0},
                {"geo_id": geo_id, "date": date, "canonical_metric_key": "median_ppsf", "value": 255.0 + geo_index * 35.0 + index * 1.9},
            ])
        for annual_date, income in (
            (pd.Timestamp("2020-01-31"), 98_000.0 + geo_index * 18_000.0),
            (pd.Timestamp("2021-01-31"), 103_000.0 + geo_index * 18_000.0),
            (pd.Timestamp("2022-01-31"), 111_000.0 + geo_index * 18_000.0),
        ):
            rows.append({"geo_id": geo_id, "date": annual_date, "canonical_metric_key": "median_household_income", "value": income})
    for index, date in enumerate(dates):
        rows.append({"geo_id": "national", "date": date, "canonical_metric_key": "mortgage_30y", "value": 2.95 + index * 0.055})
    return pd.DataFrame(rows)


def _expected_payment_burden(price: float, annual_income: float, annual_rate_percent: float) -> float:
    monthly_rate = annual_rate_percent / 100.0 / 12.0
    term_months = 360
    principal = price * 0.80
    growth_factor = (1.0 + monthly_rate) ** term_months
    payment = principal * (monthly_rate * growth_factor) / (growth_factor - 1.0)
    return payment / (annual_income / 12.0)


def _assert_close(actual: float, expected: float, label: str) -> None:
    if not math.isclose(float(actual), float(expected), rel_tol=0.0, abs_tol=1e-12):
        raise AssertionError(f"{label}: expected {expected}, found {actual}")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False)


def _sorted_observations(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    return (
        frame.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _sorted_lineage(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    return (
        frame.sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "date",
                "component_metric_key",
            ]
        )
        .reset_index(drop=True)
    )


def _component(lineage: pd.DataFrame, geo_id: str, date: pd.Timestamp, derived_key: str, component_key: str) -> pd.Series:
    rows = lineage[lineage["geo_id"].eq(geo_id) & lineage["date"].eq(date) & lineage["derived_metric_key"].eq(derived_key) & lineage["component_metric_key"].eq(component_key)]
    if len(rows) != 1:
        raise AssertionError(f"Expected one lineage row for {geo_id}/{date}/{derived_key}/{component_key}, found {len(rows)}")
    return rows.iloc[0]


def _raw_price_series(source: pd.DataFrame, geo_id: str) -> pd.DataFrame:
    return source[source["geo_id"].eq(geo_id) & source["canonical_metric_key"].eq("median_sale_price")].sort_values("date").reset_index(drop=True)


def _expected_smoothed_price(source: pd.DataFrame, geo_id: str, date: pd.Timestamp, level_window: int) -> float:
    raw_price = _raw_price_series(source, geo_id)
    sample_index = int(raw_price.index[raw_price["date"].eq(date)][0])
    return float(raw_price.loc[sample_index - level_window + 1 : sample_index, "value"].mean())


def _expected_income(source: pd.DataFrame, geo_id: str, date: pd.Timestamp) -> pd.Series:
    rows = source[
        source["geo_id"].eq(geo_id)
        & source["canonical_metric_key"].eq("median_household_income")
        & (source["date"] <= date)
    ].sort_values("date")
    if rows.empty:
        raise AssertionError(f"No expected income source for {geo_id}/{date}")
    return rows.iloc[-1]


def _expected_mortgage(source: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    rows = source[source["canonical_metric_key"].eq("mortgage_30y") & source["date"].eq(date)]
    if len(rows) != 1:
        raise AssertionError(f"Expected one mortgage source row for {date}, found {len(rows)}")
    return rows.iloc[0]


def _expected_derived_level(source: pd.DataFrame, geo_id: str, date: pd.Timestamp, metric_key: str, level_window: int) -> float:
    price = _expected_smoothed_price(source, geo_id, date, level_window)
    income = float(_expected_income(source, geo_id, date)["value"])
    if metric_key == "price_to_income":
        return price / income
    if metric_key == "payment_burden":
        mortgage = float(_expected_mortgage(source, date)["value"])
        return _expected_payment_burden(price, income, mortgage)
    raise AssertionError(f"Unexpected derived metric {metric_key}")


def _feature_row(features: pd.DataFrame, geo_id: str, date: pd.Timestamp, metric_key: str, component: str) -> pd.Series:
    rows = features[
        features["geo_id"].eq(geo_id)
        & features["date"].eq(date)
        & features["canonical_metric_key"].eq(metric_key)
        & features["feature_component"].eq(component)
    ]
    if len(rows) != 1:
        raise AssertionError(f"Expected one feature row for {geo_id}/{date}/{metric_key}/{component}, found {len(rows)}")
    return rows.iloc[0]


def _assert_lineage_complete(derived_metrics: pd.DataFrame, lineage: pd.DataFrame, candidate_id: str) -> None:
    expected_observations = (
        derived_metrics[derived_metrics["canonical_metric_key"].isin(EXPECTED_COMPONENTS)][["geo_id", "date", "canonical_metric_key"]]
        .rename(columns={"canonical_metric_key": "derived_metric_key"})
        .drop_duplicates()
        .sort_values(["geo_id", "date", "derived_metric_key"])
        .reset_index(drop=True)
    )
    relevant = lineage[lineage["derived_metric_key"].isin(EXPECTED_COMPONENTS)].copy()
    if relevant.empty:
        raise AssertionError(f"{candidate_id}: no derived lineage rows found for linked metrics")
    actual_observations = relevant[["geo_id", "date", "derived_metric_key"]].drop_duplicates().sort_values(["geo_id", "date", "derived_metric_key"]).reset_index(drop=True)
    comparison = expected_observations.merge(actual_observations, on=["geo_id", "date", "derived_metric_key"], how="outer", indicator=True, validate="one_to_one")
    if not comparison["_merge"].eq("both").all():
        raise AssertionError(f"{candidate_id}: derived observation keys do not match lineage observation keys:\n" + comparison[~comparison["_merge"].eq("both")].head(30).to_string(index=False))
    counts = relevant.groupby(["geo_id", "date", "derived_metric_key", "component_metric_key"]).size().reset_index(name="count")
    if not counts["count"].eq(1).all():
        raise AssertionError(f"{candidate_id}: derived lineage components are not unique per observation")
    membership = relevant.groupby(["geo_id", "date", "derived_metric_key"])["component_metric_key"].agg(lambda values: set(values)).reset_index()
    for row in membership.itertuples(index=False):
        expected = EXPECTED_COMPONENTS[row.derived_metric_key]
        if row.component_metric_key != expected:
            raise AssertionError(
                f"{candidate_id}: lineage components for {row.geo_id}/{row.date}/{row.derived_metric_key} "
                f"expected {sorted(expected)}, found {sorted(row.component_metric_key)}"
            )


def _assert_source_preservation(source: pd.DataFrame, lineage: pd.DataFrame, geo_id: str, date: pd.Timestamp, candidate_id: str) -> dict[str, Any]:
    level_window = get_price_family_structural_candidate(candidate_id).level_window
    expected_price = _expected_smoothed_price(source, geo_id, date, level_window)
    expected_income = _expected_income(source, geo_id, date)
    expected_mortgage = _expected_mortgage(source, date)

    for derived_key in ("price_to_income", "payment_burden"):
        price_lineage = _component(lineage, geo_id, date, derived_key, "median_sale_price")
        _assert_close(float(price_lineage["component_value"]), expected_price, f"{candidate_id}/{geo_id}/{derived_key}/lineage_price")
        if price_lineage["component_source_date"] != date or price_lineage["component_source_geo_id"] != geo_id or bool(price_lineage["was_carried_forward"]):
            raise AssertionError(f"{candidate_id}/{geo_id}/{derived_key}: substituted price lineage metadata mismatch")

        income_lineage = _component(lineage, geo_id, date, derived_key, "median_household_income")
        _assert_close(float(income_lineage["component_value"]), float(expected_income["value"]), f"{candidate_id}/{geo_id}/{derived_key}/lineage_income")
        expected_income_carried = bool(expected_income["date"] < date)
        if income_lineage["component_source_date"] != expected_income["date"]:
            raise AssertionError(f"{candidate_id}/{geo_id}/{derived_key}: income source date mismatch")
        if income_lineage["component_source_geo_id"] != expected_income["geo_id"]:
            raise AssertionError(f"{candidate_id}/{geo_id}/{derived_key}: income source geography mismatch")
        if bool(income_lineage["was_carried_forward"]) != expected_income_carried:
            raise AssertionError(f"{candidate_id}/{geo_id}/{derived_key}: income carry-forward flag mismatch")

    mortgage_lineage = _component(lineage, geo_id, date, "payment_burden", "mortgage_30y")
    _assert_close(float(mortgage_lineage["component_value"]), float(expected_mortgage["value"]), f"{candidate_id}/{geo_id}/payment_burden/lineage_mortgage")
    if mortgage_lineage["component_source_date"] != expected_mortgage["date"]:
        raise AssertionError(f"{candidate_id}/{geo_id}: mortgage source date mismatch")
    if mortgage_lineage["component_source_geo_id"] != expected_mortgage["geo_id"]:
        raise AssertionError(f"{candidate_id}/{geo_id}: mortgage source geography mismatch")
    if bool(mortgage_lineage["was_carried_forward"]):
        raise AssertionError(f"{candidate_id}/{geo_id}: mortgage should be same-month, not carried forward")

    return {
        "candidate_id": candidate_id,
        "level_window": level_window,
        "geo_id": geo_id,
        "date": date,
        "substituted_price": expected_price,
        "income": float(expected_income["value"]),
        "income_source_date": expected_income["date"],
        "income_source_geo_id": expected_income["geo_id"],
        "mortgage_30y": float(expected_mortgage["value"]),
        "mortgage_source_date": expected_mortgage["date"],
        "mortgage_source_geo_id": expected_mortgage["geo_id"],
    }


def _assert_derived_feature_contract(source: pd.DataFrame, features: pd.DataFrame, geo_id: str, metric_key: str, candidate_id: str) -> list[dict[str, Any]]:
    raw_price = _raw_price_series(source, geo_id)
    candidate = get_price_family_structural_candidate(candidate_id)
    level_window = candidate.level_window
    samples = [
        ("level", 0, level_window - 1),
        ("short", candidate.short_lag_periods, level_window - 1 + candidate.short_lag_periods),
        ("long", candidate.long_lag_periods, level_window - 1 + candidate.long_lag_periods),
    ]
    rows: list[dict[str, Any]] = []
    for component, lag, index in samples:
        date = pd.Timestamp(raw_price.loc[index, "date"])
        expected_level = _expected_derived_level(source, geo_id, date, metric_key, level_window)
        row = _feature_row(features, geo_id, date, metric_key, component)
        if int(row["level_window"]) != level_window or int(row["lag_periods"]) != lag:
            raise AssertionError(f"{candidate_id}/{geo_id}/{metric_key}/{component}: window/lag metadata mismatch")
        if component == "level":
            _assert_close(float(row["raw_feature_value"]), expected_level, f"{candidate_id}/{geo_id}/{metric_key}/level")
            if not pd.isna(row["reference_value"]):
                raise AssertionError(f"{candidate_id}/{geo_id}/{metric_key}/level should not have a reference value")
            expected_reference = np.nan
            expected_feature = expected_level
        else:
            reference_date = pd.Timestamp(raw_price.loc[index - lag, "date"])
            expected_reference = _expected_derived_level(source, geo_id, reference_date, metric_key, level_window)
            expected_feature = expected_level / expected_reference - 1.0
            _assert_close(float(row["source_level_value"]), expected_level, f"{candidate_id}/{geo_id}/{metric_key}/{component}/source_level")
            _assert_close(float(row["reference_value"]), expected_reference, f"{candidate_id}/{geo_id}/{metric_key}/{component}/reference")
            _assert_close(float(row["raw_feature_value"]), expected_feature, f"{candidate_id}/{geo_id}/{metric_key}/{component}")
        rows.append({"candidate_id": candidate_id, "level_window": level_window, "geo_id": geo_id, "metric": metric_key, "feature_component": component, "date": date, "expected_level": expected_level, "expected_reference": expected_reference, "expected_feature": expected_feature})
    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    source = _build_fixture()
    print(f"[phase1_linked_recalculation] candidates={','.join(CANDIDATE_IDS)}")
    sample_rows: list[dict[str, Any]] = []
    feature_formula_rows: list[dict[str, Any]] = []
    coverage_rows: list[pd.DataFrame] = []
    lineage_summary_rows: list[pd.DataFrame] = []
    duplicate_errors: dict[str, str] = {}

    for candidate_id in CANDIDATE_IDS:
        if candidate_id not in PRICE_FAMILY_STRUCTURAL_CANDIDATES:
            raise AssertionError(f"Missing structural candidate registry entry: {candidate_id}")
        first = build_linked_price_family_features(source, experiment_id=candidate_id)
        second = build_linked_price_family_features(source, experiment_id=candidate_id)
        pd.testing.assert_frame_equal(first.derived_metrics, second.derived_metrics, check_exact=True)
        pd.testing.assert_frame_equal(first.derived_lineage, second.derived_lineage, check_exact=True)

        derived = first.derived_metrics
        lineage = first.derived_lineage
        features = first.feature_history
        if derived.duplicated(["geo_id", "date", "canonical_metric_key"]).any():
            raise AssertionError(f"{candidate_id}: duplicate derived metric keys found")
        if lineage.duplicated(["geo_id", "date", "derived_metric_key", "component_metric_key"]).any():
            raise AssertionError(f"{candidate_id}: duplicate lineage keys found")
        if not np.isfinite(derived["value"]).all():
            raise AssertionError(f"{candidate_id}: derived values contain non-finite outputs")
        if (lineage["component_age_days"] < 0).any():
            raise AssertionError(f"{candidate_id}: lineage contains negative component ages")
        _assert_lineage_complete(derived, lineage, candidate_id)

        for geo_id in ("geo_alpha", "geo_beta"):
            date = pd.Timestamp("2021-06-30")
            preserved = _assert_source_preservation(source, lineage, geo_id, date, candidate_id)
            level_window = get_price_family_structural_candidate(candidate_id).level_window
            pti = derived[derived["geo_id"].eq(geo_id) & derived["date"].eq(date) & derived["canonical_metric_key"].eq("price_to_income")]["value"].iloc[0]
            burden = derived[derived["geo_id"].eq(geo_id) & derived["date"].eq(date) & derived["canonical_metric_key"].eq("payment_burden")]["value"].iloc[0]
            expected_pti = _expected_derived_level(source, geo_id, date, "price_to_income", level_window)
            expected_burden = _expected_derived_level(source, geo_id, date, "payment_burden", level_window)
            _assert_close(float(pti), expected_pti, f"{candidate_id}/{geo_id}/price_to_income")
            _assert_close(float(burden), expected_burden, f"{candidate_id}/{geo_id}/payment_burden")
            preserved.update({"price_to_income": float(pti), "payment_burden": float(burden)})
            sample_rows.append(preserved)
            for metric_key in ("price_to_income", "payment_burden"):
                feature_formula_rows.extend(_assert_derived_feature_contract(source, features, geo_id, metric_key, candidate_id))

        try:
            build_linked_price_family_features(pd.concat([source, source.head(1)], ignore_index=True), experiment_id=candidate_id)
        except ValueError as exc:
            duplicate_errors[candidate_id] = str(exc).splitlines()[0]
        else:
            raise AssertionError(f"{candidate_id}: duplicate source keys were not rejected")

        #
        # Production augmentation contract
        #

        canonical_source = source.copy()
        canonical_source["metric_origin"] = "source"

        canonical_derived = first.derived_metrics.copy()
        canonical_derived["metric_origin"] = "derived"

        canonical = pd.concat(
            [
                canonical_source[
                    [
                        "geo_id",
                        "date",
                        "canonical_metric_key",
                        "value",
                        "metric_origin",
                    ]
                ],
                canonical_derived[
                    [
                        "geo_id",
                        "date",
                        "canonical_metric_key",
                        "value",
                        "metric_origin",
                    ]
                ],
            ],
            ignore_index=True,
        )

        augmented_observations, augmented_lineage = (
            apply_linked_price_family_augmentation(
                observations=canonical,
                derived_lineage=first.derived_lineage,
                experiment_id=candidate_id,
            )
        )

        source_metric_keys = {
            "median_sale_price",
            "median_ppsf",
            "median_household_income",
            "mortgage_30y",
        }
        linked_metric_keys = {
            "price_to_income",
            "payment_burden",
        }

        # 1. Canonical source observations must remain unchanged.
        expected_sources = _sorted_observations(
            canonical[
                canonical["canonical_metric_key"].isin(
                    source_metric_keys
                )
            ][
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "value",
                    "metric_origin",
                ]
            ]
        )
        actual_sources = _sorted_observations(
            augmented_observations[
                augmented_observations[
                    "canonical_metric_key"
                ].isin(source_metric_keys)
            ][
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "value",
                    "metric_origin",
                ]
            ]
        )
        pd.testing.assert_frame_equal(
            actual_sources,
            expected_sources,
            check_exact=True,
        )

        # 2. Linked derived observations must match the recalculation.
        expected_linked_metrics = _sorted_observations(
            first.derived_metrics[
                first.derived_metrics[
                    "canonical_metric_key"
                ].isin(linked_metric_keys)
            ]
            .assign(metric_origin="derived")[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "value",
                    "metric_origin",
                ]
            ]
        )
        actual_linked_metrics = _sorted_observations(
            augmented_observations[
                augmented_observations[
                    "canonical_metric_key"
                ].isin(linked_metric_keys)
            ][
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "value",
                    "metric_origin",
                ]
            ]
        )
        pd.testing.assert_frame_equal(
            actual_linked_metrics,
            expected_linked_metrics,
            check_exact=True,
        )

        # 3. Linked lineage must match the recalculated lineage.
        expected_linked_lineage = _sorted_lineage(
            first.derived_lineage[
                first.derived_lineage[
                    "derived_metric_key"
                ].isin(linked_metric_keys)
            ]
        )
        actual_linked_lineage = _sorted_lineage(
            augmented_lineage[
                augmented_lineage[
                    "derived_metric_key"
                ].isin(linked_metric_keys)
            ]
        )
        pd.testing.assert_frame_equal(
            actual_linked_lineage,
            expected_linked_lineage,
            check_exact=True,
        )

        # 4. Augmentation must not produce duplicate keys.
        observation_duplicates = (
            augmented_observations.duplicated(
                subset=[
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                ],
                keep=False,
            )
        )
        if observation_duplicates.any():
            raise AssertionError(
                f"{candidate_id}: augmentation produced duplicate "
                "observation keys:\n"
                + augmented_observations.loc[
                    observation_duplicates
                ]
                .sort_values(
                    [
                        "geo_id",
                        "canonical_metric_key",
                        "date",
                    ]
                )
                .head(30)
                .to_string(index=False)
            )

        lineage_duplicates = augmented_lineage.duplicated(
            subset=[
                "geo_id",
                "date",
                "derived_metric_key",
                "component_metric_key",
            ],
            keep=False,
        )
        if lineage_duplicates.any():
            raise AssertionError(
                f"{candidate_id}: augmentation produced duplicate "
                "lineage keys:\n"
                + augmented_lineage.loc[lineage_duplicates]
                .sort_values(
                    [
                        "geo_id",
                        "derived_metric_key",
                        "date",
                        "component_metric_key",
                    ]
                )
                .head(30)
                .to_string(index=False)
            )

        # 5. Replacement semantics: unrelated observations remain
        # unchanged, while linked-family rows are replaced exactly once.
        expected_unrelated = _sorted_observations(
            canonical[
                ~canonical["canonical_metric_key"].isin(
                    linked_metric_keys
                )
            ]
        )
        actual_unrelated = _sorted_observations(
            augmented_observations[
                ~augmented_observations[
                    "canonical_metric_key"
                ].isin(linked_metric_keys)
            ]
        )
        pd.testing.assert_frame_equal(
            actual_unrelated,
            expected_unrelated,
            check_exact=True,
        )

        original_linked_count = int(
            canonical["canonical_metric_key"]
            .isin(linked_metric_keys)
            .sum()
        )
        expected_augmented_count = (
            len(canonical)
            - original_linked_count
            + len(expected_linked_metrics)
        )
        if len(augmented_observations) != expected_augmented_count:
            raise AssertionError(
                f"{candidate_id}: expected "
                f"{expected_augmented_count} augmented observations, "
                f"found {len(augmented_observations)}"
            )

        coverage = derived.groupby(["canonical_metric_key"], as_index=False).agg(rows=("value", "size"), first_date=("date", "min"), last_date=("date", "max"))
        coverage.insert(0, "candidate_id", candidate_id)
        coverage.insert(1, "level_window", get_price_family_structural_candidate(candidate_id).level_window)
        coverage_rows.append(coverage)
        lineage_summary = lineage.groupby(["derived_metric_key", "component_metric_key"], as_index=False).agg(rows=("component_value", "size"), carried_forward_rows=("was_carried_forward", "sum"), max_age_days=("component_age_days", "max"))
        lineage_summary.insert(0, "candidate_id", candidate_id)
        lineage_summary_rows.append(lineage_summary)

    _write_csv(pd.concat(coverage_rows, ignore_index=True), OUTPUT_DIR / "derived_recalculation_coverage.csv")
    _write_csv(pd.concat(lineage_summary_rows, ignore_index=True), OUTPUT_DIR / "derived_lineage_component_summary.csv")
    _write_csv(pd.DataFrame(sample_rows), OUTPUT_DIR / "row_level_reproducibility.csv")
    _write_csv(pd.DataFrame(feature_formula_rows), OUTPUT_DIR / "derived_feature_formula_samples.csv")
    summary = {
        "candidate_ids": list(CANDIDATE_IDS),
        "level_windows": {candidate_id: get_price_family_structural_candidate(candidate_id).level_window for candidate_id in CANDIDATE_IDS},
        "source_rows": len(source),
        "duplicate_protection": duplicate_errors,
        "deterministic_rerun": True,
        "substituted_price_used": True,
        "income_and_mortgage_preserved_from_source_fixture": True,
        "lineage_component_membership_complete": True,
        "derived_observation_lineage_coverage_complete": True,
        "derived_same_state_features_validated": True,
    }
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(pd.concat(coverage_rows, ignore_index=True).to_string(index=False))
    print(pd.concat(lineage_summary_rows, ignore_index=True).to_string(index=False))
    print(f"[phase1_linked_recalculation] artifacts={OUTPUT_DIR}")
    print("[phase1_linked_recalculation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
