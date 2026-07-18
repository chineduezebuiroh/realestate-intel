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

from regime.experiments.linked_price_family_features import (
    LEVEL_WINDOW,
    LONG_LAG_PERIODS,
    SHORT_LAG_PERIODS,
    build_linked_price_family_features,
)

CANDIDATE_ID = "price_family_ma12_structural_linked"
LEGACY_CANDIDATE_ID = "price_family_ma12_momentum_lag3"
OUTPUT_DIR = Path(
    "artifacts/regime/comparisons/price_family_ma12_structural_linked/phase1_linked_recalculation"
)
EXPECTED_COMPONENTS = {
    "price_to_income": {"median_sale_price", "median_household_income"},
    "payment_burden": {"median_sale_price", "median_household_income", "mortgage_30y"},
}


def _build_fixture() -> pd.DataFrame:
    dates = pd.date_range("2020-01-31", periods=30, freq="M")
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


def _component(lineage: pd.DataFrame, geo_id: str, date: pd.Timestamp, derived_key: str, component_key: str) -> pd.Series:
    rows = lineage[lineage["geo_id"].eq(geo_id) & lineage["date"].eq(date) & lineage["derived_metric_key"].eq(derived_key) & lineage["component_metric_key"].eq(component_key)]
    if len(rows) != 1:
        raise AssertionError(f"Expected one lineage row for {geo_id}/{date}/{derived_key}/{component_key}, found {len(rows)}")
    return rows.iloc[0]


def _raw_price_series(source: pd.DataFrame, geo_id: str) -> pd.DataFrame:
    return source[source["geo_id"].eq(geo_id) & source["canonical_metric_key"].eq("median_sale_price")].sort_values("date").reset_index(drop=True)


def _expected_smoothed_price(source: pd.DataFrame, geo_id: str, date: pd.Timestamp) -> float:
    raw_price = _raw_price_series(source, geo_id)
    sample_index = int(raw_price.index[raw_price["date"].eq(date)][0])
    return float(raw_price.loc[sample_index - LEVEL_WINDOW + 1 : sample_index, "value"].mean())


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


def _expected_derived_level(source: pd.DataFrame, geo_id: str, date: pd.Timestamp, metric_key: str) -> float:
    price = _expected_smoothed_price(source, geo_id, date)
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


def _assert_lineage_complete(derived_metrics: pd.DataFrame, lineage: pd.DataFrame) -> None:
    expected_observations = (
        derived_metrics[
            derived_metrics["canonical_metric_key"].isin(EXPECTED_COMPONENTS)
        ][["geo_id", "date", "canonical_metric_key"]]
        .rename(columns={"canonical_metric_key": "derived_metric_key"})
        .drop_duplicates()
        .sort_values(["geo_id", "date", "derived_metric_key"])
        .reset_index(drop=True)
    )
    relevant = lineage[lineage["derived_metric_key"].isin(EXPECTED_COMPONENTS)].copy()
    if relevant.empty:
        raise AssertionError("No derived lineage rows found for linked metrics")
    actual_observations = (
        relevant[["geo_id", "date", "derived_metric_key"]]
        .drop_duplicates()
        .sort_values(["geo_id", "date", "derived_metric_key"])
        .reset_index(drop=True)
    )
    comparison = expected_observations.merge(
        actual_observations,
        on=["geo_id", "date", "derived_metric_key"],
        how="outer",
        indicator=True,
        validate="one_to_one",
    )
    if not comparison["_merge"].eq("both").all():
        raise AssertionError(
            "Derived observation keys do not match lineage observation keys:\n"
            + comparison[~comparison["_merge"].eq("both")].head(30).to_string(index=False)
        )
    counts = relevant.groupby(["geo_id", "date", "derived_metric_key", "component_metric_key"]).size().reset_index(name="count")
    if not counts["count"].eq(1).all():
        raise AssertionError("Derived lineage components are not unique per observation")
    membership = relevant.groupby(["geo_id", "date", "derived_metric_key"])["component_metric_key"].agg(lambda values: set(values)).reset_index()
    for row in membership.itertuples(index=False):
        expected = EXPECTED_COMPONENTS[row.derived_metric_key]
        if row.component_metric_key != expected:
            raise AssertionError(
                f"Lineage components for {row.geo_id}/{row.date}/{row.derived_metric_key} "
                f"expected {sorted(expected)}, found {sorted(row.component_metric_key)}"
            )


def _assert_source_preservation(source: pd.DataFrame, lineage: pd.DataFrame, geo_id: str, date: pd.Timestamp) -> dict[str, Any]:
    expected_price = _expected_smoothed_price(source, geo_id, date)
    expected_income = _expected_income(source, geo_id, date)
    expected_mortgage = _expected_mortgage(source, date)

    for derived_key in ("price_to_income", "payment_burden"):
        price_lineage = _component(lineage, geo_id, date, derived_key, "median_sale_price")
        _assert_close(float(price_lineage["component_value"]), expected_price, f"{geo_id}/{derived_key}/lineage_price")
        if price_lineage["component_source_date"] != date or price_lineage["component_source_geo_id"] != geo_id or bool(price_lineage["was_carried_forward"]):
            raise AssertionError(f"{geo_id}/{derived_key}: substituted price lineage metadata mismatch")

        income_lineage = _component(lineage, geo_id, date, derived_key, "median_household_income")
        _assert_close(float(income_lineage["component_value"]), float(expected_income["value"]), f"{geo_id}/{derived_key}/lineage_income")
        expected_income_carried = bool(expected_income["date"] < date)
        if income_lineage["component_source_date"] != expected_income["date"]:
            raise AssertionError(f"{geo_id}/{derived_key}: income source date mismatch")
        if income_lineage["component_source_geo_id"] != expected_income["geo_id"]:
            raise AssertionError(f"{geo_id}/{derived_key}: income source geography mismatch")
        if bool(income_lineage["was_carried_forward"]) != expected_income_carried:
            raise AssertionError(f"{geo_id}/{derived_key}: income carry-forward flag mismatch")

    mortgage_lineage = _component(lineage, geo_id, date, "payment_burden", "mortgage_30y")
    _assert_close(float(mortgage_lineage["component_value"]), float(expected_mortgage["value"]), f"{geo_id}/payment_burden/lineage_mortgage")
    if mortgage_lineage["component_source_date"] != expected_mortgage["date"]:
        raise AssertionError(f"{geo_id}: mortgage source date mismatch")
    if mortgage_lineage["component_source_geo_id"] != expected_mortgage["geo_id"]:
        raise AssertionError(f"{geo_id}: mortgage source geography mismatch")
    if bool(mortgage_lineage["was_carried_forward"]):
        raise AssertionError(f"{geo_id}: mortgage should be same-month, not carried forward")

    return {
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


def _assert_derived_feature_contract(source: pd.DataFrame, features: pd.DataFrame, geo_id: str, metric_key: str) -> list[dict[str, Any]]:
    raw_price = _raw_price_series(source, geo_id)
    samples = [
        ("level", 0, LEVEL_WINDOW - 1),
        ("short", SHORT_LAG_PERIODS, LEVEL_WINDOW - 1 + SHORT_LAG_PERIODS),
        ("long", LONG_LAG_PERIODS, LEVEL_WINDOW - 1 + LONG_LAG_PERIODS),
    ]
    rows: list[dict[str, Any]] = []
    for component, lag, index in samples:
        date = pd.Timestamp(raw_price.loc[index, "date"])
        expected_level = _expected_derived_level(source, geo_id, date, metric_key)
        row = _feature_row(features, geo_id, date, metric_key, component)
        if component == "level":
            _assert_close(float(row["raw_feature_value"]), expected_level, f"{geo_id}/{metric_key}/level")
            if not pd.isna(row["reference_value"]):
                raise AssertionError(f"{geo_id}/{metric_key}/level should not have a reference value")
            expected_reference = np.nan
            expected_feature = expected_level
        else:
            reference_date = pd.Timestamp(raw_price.loc[index - lag, "date"])
            expected_reference = _expected_derived_level(source, geo_id, reference_date, metric_key)
            expected_feature = expected_level / expected_reference - 1.0
            _assert_close(float(row["source_level_value"]), expected_level, f"{geo_id}/{metric_key}/{component}/source_level")
            _assert_close(float(row["reference_value"]), expected_reference, f"{geo_id}/{metric_key}/{component}/reference")
            _assert_close(float(row["raw_feature_value"]), expected_feature, f"{geo_id}/{metric_key}/{component}")
        rows.append({
            "geo_id": geo_id,
            "metric": metric_key,
            "feature_component": component,
            "date": date,
            "expected_level": expected_level,
            "expected_reference": expected_reference,
            "expected_feature": expected_feature,
        })
    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    source = _build_fixture()
    print(f"[phase1_linked_recalculation] candidate={CANDIDATE_ID} legacy_identifier={LEGACY_CANDIDATE_ID}")
    first = build_linked_price_family_features(source, experiment_id=CANDIDATE_ID)
    second = build_linked_price_family_features(source, experiment_id=CANDIDATE_ID)
    pd.testing.assert_frame_equal(first.derived_metrics, second.derived_metrics, check_exact=True)
    pd.testing.assert_frame_equal(first.derived_lineage, second.derived_lineage, check_exact=True)

    derived = first.derived_metrics
    lineage = first.derived_lineage
    features = first.feature_history
    if derived.duplicated(["geo_id", "date", "canonical_metric_key"]).any():
        raise AssertionError("Duplicate derived metric keys found")
    if lineage.duplicated(["geo_id", "date", "derived_metric_key", "component_metric_key"]).any():
        raise AssertionError("Duplicate lineage keys found")
    if not np.isfinite(derived["value"]).all():
        raise AssertionError("Derived values contain non-finite outputs")
    if (lineage["component_age_days"] < 0).any():
        raise AssertionError("Lineage contains negative component ages")
    _assert_lineage_complete(derived, lineage)

    sample_rows: list[dict[str, Any]] = []
    feature_formula_rows: list[dict[str, Any]] = []
    for geo_id in ("geo_alpha", "geo_beta"):
        date = pd.Timestamp("2021-06-30")
        preserved = _assert_source_preservation(source, lineage, geo_id, date)
        pti = derived[derived["geo_id"].eq(geo_id) & derived["date"].eq(date) & derived["canonical_metric_key"].eq("price_to_income")]["value"].iloc[0]
        burden = derived[derived["geo_id"].eq(geo_id) & derived["date"].eq(date) & derived["canonical_metric_key"].eq("payment_burden")]["value"].iloc[0]
        expected_pti = _expected_derived_level(source, geo_id, date, "price_to_income")
        expected_burden = _expected_derived_level(source, geo_id, date, "payment_burden")
        _assert_close(float(pti), expected_pti, f"{geo_id}/price_to_income")
        _assert_close(float(burden), expected_burden, f"{geo_id}/payment_burden")
        preserved.update({"price_to_income": float(pti), "payment_burden": float(burden)})
        sample_rows.append(preserved)
        for metric_key in ("price_to_income", "payment_burden"):
            feature_formula_rows.extend(_assert_derived_feature_contract(source, features, geo_id, metric_key))

    try:
        build_linked_price_family_features(pd.concat([source, source.head(1)], ignore_index=True), experiment_id=CANDIDATE_ID)
    except ValueError as exc:
        duplicate_error = str(exc).splitlines()[0]
    else:
        raise AssertionError("Duplicate source keys were not rejected")

    coverage = derived.groupby(["canonical_metric_key"], as_index=False).agg(rows=("value", "size"), first_date=("date", "min"), last_date=("date", "max"))
    lineage_summary = lineage.groupby(["derived_metric_key", "component_metric_key"], as_index=False).agg(rows=("component_value", "size"), carried_forward_rows=("was_carried_forward", "sum"), max_age_days=("component_age_days", "max"))
    _write_csv(coverage, OUTPUT_DIR / "derived_recalculation_coverage.csv")
    _write_csv(lineage_summary, OUTPUT_DIR / "derived_lineage_component_summary.csv")
    _write_csv(pd.DataFrame(sample_rows), OUTPUT_DIR / "row_level_reproducibility.csv")
    _write_csv(pd.DataFrame(feature_formula_rows), OUTPUT_DIR / "derived_feature_formula_samples.csv")
    summary = {"candidate_id": CANDIDATE_ID, "legacy_identifier": LEGACY_CANDIDATE_ID, "source_rows": len(source), "derived_rows": len(derived), "lineage_rows": len(lineage), "duplicate_protection": duplicate_error, "deterministic_rerun": True, "substituted_price_used": True, "income_and_mortgage_preserved_from_source_fixture": True, "lineage_component_membership_complete": True, "derived_observation_lineage_coverage_complete": True, "derived_same_state_features_validated": True}
    (OUTPUT_DIR / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(coverage.to_string(index=False))
    print(lineage_summary.to_string(index=False))
    print(f"[phase1_linked_recalculation] artifacts={OUTPUT_DIR}")
    print("[phase1_linked_recalculation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
