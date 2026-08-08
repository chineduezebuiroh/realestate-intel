"""Phase 4A diagnostic for Affordability derivation/smoothing order.

This module is deliberately experimental.  It reuses the canonical derived
metric builder and does not alter any production registry.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

from regime.derived_metrics import DERIVED_METRIC_COMPONENTS, build_derived_metrics_with_lineage
from regime.linked_price_family import build_structural_level
from regime.affordability_derivation import build_promoted_affordability_chronology


POLICY_A: Final = "AFF-DERIVATION-A"
POLICY_B: Final = "AFF-DERIVATION-B"
TARGET_METRICS: Final = ("price_to_income", "payment_burden")
FEATURE_WEIGHTS: Final = {"level": 0.50, "short": 0.20, "long": 0.30}
LEVEL_WINDOW: Final = 12
SHORT_LAG: Final = 3
LONG_LAG: Final = 12
FORMULA_TOLERANCE: Final = 1e-12


@dataclass(frozen=True)
class AffordabilityDerivationEvidence:
    tables: dict[str, pd.DataFrame]


def policy_registry() -> pd.DataFrame:
    rows = [
        (POLICY_A, "MA12(price) -> canonical derive -> lag3/lag12", True),
        (POLICY_B, "canonical derive(raw inputs) -> MA12 -> lag3/lag12", False),
    ]
    return pd.DataFrame([
        {
            "policy": policy, "derivation_order": order,
            "price_smoothed_before_derivation": price_smoothed,
            "income_smoothed_before_derivation": False,
            "mortgage_smoothed_before_derivation": False,
            "level_window": LEVEL_WINDOW, "short_lag": SHORT_LAG,
            "long_lag": LONG_LAG, "level_weight": FEATURE_WEIGHTS["level"],
            "short_weight": FEATURE_WEIGHTS["short"],
            "long_weight": FEATURE_WEIGHTS["long"],
            "recommendation_state": "none", "promotion_state": "none",
            "human_decision": "pending",
        }
        for policy, order, price_smoothed in rows
    ])


def _validate_source(source: pd.DataFrame) -> pd.DataFrame:
    required = {"geo_id", "date", "canonical_metric_key", "value"}
    missing = required - set(source.columns)
    if missing:
        raise ValueError(f"Source observations are missing columns: {sorted(missing)}")
    work = source.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    relevant = work[work.canonical_metric_key.isin({
        "median_sale_price", "median_household_income", "mortgage_30y"
    })]
    if relevant["date"].isna().any() or not np.isfinite(relevant["value"]).all():
        raise ValueError("Required source inputs contain invalid dates or non-finite values")
    dup = work.duplicated(["geo_id", "date", "canonical_metric_key"], keep=False)
    if dup.any():
        raise ValueError("Source observations contain duplicate geo/date/metric keys")
    # Monthly price histories define the local diagnostic chronology.
    # Normalize datetime units before equality checks so identical calendar
    # dates stored at different pandas resolutions (for example us vs ns)
    # do not create false gap failures.
    for geo_id, group in relevant[
        relevant.canonical_metric_key.eq("median_sale_price")
    ].groupby("geo_id"):
        dates = pd.DatetimeIndex(
            group["date"].sort_values()
        ).astype("datetime64[ns]")

        if not dates.is_monotonic_increasing:
            raise ValueError(
                f"Non-monotonic price chronology for {geo_id}"
            )

        expected = pd.DatetimeIndex(
            pd.date_range(
                dates.min(),
                dates.max(),
                freq="M",
            )
        ).astype("datetime64[ns]")

        if (
            len(dates) != len(expected)
            or not np.array_equal(
                dates.asi8,
                expected.asi8,
            )
        ):
            missing_dates = expected.difference(dates)
            unexpected_dates = dates.difference(expected)

            raise ValueError(
                "Unexpected interior monthly price gap; "
                f"geo_id={geo_id}, "
                f"missing_dates={list(missing_dates)}, "
                f"unexpected_dates={list(unexpected_dates)}"
            )
    return work.sort_values(["geo_id", "canonical_metric_key", "date"]).reset_index(drop=True)


def _policy_source(source: pd.DataFrame, policy: str) -> pd.DataFrame:
    if policy == POLICY_B:
        return source.copy()
    price = source[source.canonical_metric_key.eq("median_sale_price")]
    level = build_structural_level(price, level_window=LEVEL_WINDOW)
    replacement = level[["geo_id", "date", "structural_level_value"]]
    out = source.merge(replacement, on=["geo_id", "date"], how="left", validate="many_to_one")
    mask = out.canonical_metric_key.eq("median_sale_price")
    out.loc[mask, "value"] = out.loc[mask, "structural_level_value"]
    return out.drop(columns="structural_level_value")


def _derive(source: pd.DataFrame, policy: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    derived, lineage = build_derived_metrics_with_lineage(_policy_source(source, policy))
    derived = derived[derived.canonical_metric_key.isin(TARGET_METRICS)].copy()
    lineage = lineage[lineage.derived_metric_key.isin(TARGET_METRICS)].copy()
    derived["policy"] = policy
    if policy == POLICY_A:
        derived["pre_feature_derived_value"] = derived["value"]
        derived["structural_level"] = derived["value"]
    else:
        promoted, smoothed, promoted_lineage = build_promoted_affordability_chronology(source)
        derived = promoted.copy()
        derived["policy"] = policy
        derived["pre_feature_derived_value"] = derived["value"]
        derived["structural_level"] = smoothed["structural_level"].to_numpy()
        lineage = promoted_lineage
    return derived, lineage


def _input_audit(lineage: pd.DataFrame, policy: str) -> pd.DataFrame:
    out = lineage.rename(columns={
        "derived_metric_key": "metric", "component_metric_key": "input_name",
        "component_value": "input_value", "component_source_date": "input_source_date",
        "component_source_geo_id": "input_source_geo", "was_carried_forward": "forward_filled_flag",
    }).copy()
    out["input_frequency"] = np.where(out.input_name.eq("median_household_income"), "annual_forward_filled", "monthly")
    out["smoothing_applied_before_derivation"] = out.input_name.eq("median_sale_price") & (policy == POLICY_A)
    out["policy"] = policy
    columns = ["metric", "date", "geo_id", "input_name", "input_value", "input_source_date",
               "input_source_geo", "input_frequency", "forward_filled_flag",
               "smoothing_applied_before_derivation", "policy"]
    return out[columns]


def _features(raw: pd.DataFrame) -> pd.DataFrame:
    work = raw.sort_values(["policy", "canonical_metric_key", "geo_id", "date"]).copy()
    group = work.groupby(["policy", "canonical_metric_key", "geo_id"], sort=False)
    work["short_feature"] = work.structural_level / group.structural_level.shift(SHORT_LAG) - 1
    work["long_feature"] = work.structural_level / group.structural_level.shift(LONG_LAG) - 1
    return work.rename(columns={"canonical_metric_key": "metric"})[
        ["policy", "metric", "geo_id", "date", "structural_level", "short_feature", "long_feature"]
    ]


def _formula_audit(raw: pd.DataFrame, audits: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for policy in (POLICY_A, POLICY_B):
        p = audits[audits.policy.eq(policy)]
        reconstructed_source = p.rename(columns={"metric": "derived_metric", "input_name": "canonical_metric_key", "input_value": "value"})
        reconstructed_source = reconstructed_source[["geo_id", "date", "canonical_metric_key", "value"]].drop_duplicates()
        rebuilt, _ = build_derived_metrics_with_lineage(reconstructed_source)
        expected = raw[raw.policy.eq(policy)][["geo_id", "date", "canonical_metric_key", "pre_feature_derived_value"]]
        check = expected.merge(rebuilt, on=["geo_id", "date", "canonical_metric_key"], how="left", validate="one_to_one")
        check["absolute_error"] = (check.pre_feature_derived_value - check.value).abs()
        check["within_tolerance"] = check.absolute_error.le(FORMULA_TOLERANCE)
        check["policy"] = policy
        rows.append(check.rename(columns={"canonical_metric_key": "metric", "value": "reconstructed_value"}))
    result = pd.concat(rows, ignore_index=True)
    if result.reconstructed_value.isna().any() or not result.within_tolerance.all():
        raise AssertionError("Canonical formula reconstruction disagreement")
    return result


def _stability(features: pd.DataFrame) -> pd.DataFrame:
    work = features.sort_values(
        ["policy", "metric", "geo_id", "date"]
    ).copy()

    group = work.groupby(
        ["policy", "metric", "geo_id"],
        sort=False,
    )
    work["mom"] = group.structural_level.pct_change(
        fill_method=None
    )

    rows = []

    for (policy, metric), frame in work.groupby(
        ["policy", "metric"],
        sort=True,
    ):
        movement = frame["mom"].dropna().abs()

        # Count sign flips independently within each geography.
        # Never allow the last observation of one geography to be compared
        # with the first observation of the next geography.
        sign_flip_count = 0

        for _, geo_frame in frame.groupby(
            "geo_id",
            sort=False,
        ):
            signed = (
                geo_frame["mom"]
                .dropna()
                .loc[lambda s: ~np.isclose(s, 0.0)]
            )

            if len(signed) < 2:
                continue

            directions = np.sign(signed.to_numpy())

            sign_flip_count += int(
                np.sum(
                    directions[1:]
                    != directions[:-1]
                )
            )

        rows.append(
            {
                "policy": policy,
                "metric": metric,
                "median_abs_mom":
                    movement.median(),
                "p90_abs_mom":
                    movement.quantile(.90),
                "p99_abs_mom":
                    movement.quantile(.99),
                "max_abs_jump":
                    movement.max(),
                "sign_flips":
                    sign_flip_count,
            }
        )

    return pd.DataFrame(rows)


TURN_PERSISTENCE = 3


def _turning_point_tables(
    features: pd.DataFrame,
    *,
    policy_universe: tuple[str, ...] | None = None,
    metric_universe: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build persistent structural-level turning-point evidence.

    A turn occurs when monthly structural-level movement reverses direction
    and the new direction persists for TURN_PERSISTENCE consecutive monthly
    movements.

    The turning-point date is the prior month's structural-level observation:
    the local peak before a negative run or the local trough before a positive
    run.

    No prominence threshold is introduced in Phase 4A; persistence is the
    only qualification rule.
    """
    policy_universe = policy_universe or (POLICY_A, POLICY_B)
    metric_universe = metric_universe or TARGET_METRICS

    rows = []

    work = features.sort_values(
        ["policy", "metric", "geo_id", "date"]
    ).copy()

    for (policy, metric, geo_id), frame in work.groupby(
        ["policy", "metric", "geo_id"],
        sort=True,
    ):
        frame = (
            frame[
                [
                    "date",
                    "structural_level",
                ]
            ]
            .dropna()
            .sort_values("date")
            .reset_index(drop=True)
        )

        if len(frame) < TURN_PERSISTENCE + 2:
            continue

        frame["movement"] = (
            frame["structural_level"]
            .pct_change(fill_method=None)
        )

        directions = np.sign(
            frame["movement"].to_numpy(
                dtype=float
            )
        )

        # Treat exact zero movements as directionless rather than as a
        # separate turning-point direction.
        directions[
            np.isclose(
                directions,
                0.0,
                equal_nan=False,
            )
        ] = 0.0

        last_nonzero_direction = None

        for i in range(1, len(frame)):
            current_direction = directions[i]

            if (
                not np.isfinite(current_direction)
                or current_direction == 0
            ):
                continue

            if last_nonzero_direction is None:
                last_nonzero_direction = current_direction
                continue

            if current_direction == last_nonzero_direction:
                continue

            persistence_end = (
                i + TURN_PERSISTENCE
            )

            if persistence_end > len(frame):
                break

            persistence_directions = directions[
                i:persistence_end
            ]

            persistent = bool(
                len(persistence_directions)
                == TURN_PERSISTENCE
                and np.all(
                    persistence_directions
                    == current_direction
                )
            )

            if not persistent:
                continue

            turn_index = i - 1
            turn_date = frame.loc[
                turn_index,
                "date",
            ]
            turn_level = frame.loc[
                turn_index,
                "structural_level",
            ]

            confirmation_index = (
                persistence_end - 1
            )
            confirmation_date = frame.loc[
                confirmation_index,
                "date",
            ]
            confirmation_level = frame.loc[
                confirmation_index,
                "structural_level",
            ]

            turn_type = (
                "peak"
                if (
                    last_nonzero_direction > 0
                    and current_direction < 0
                )
                else "trough"
            )

            post_turn_change = (
                confirmation_level
                / turn_level
                - 1.0
            )

            rows.append(
                {
                    "policy": policy,
                    "metric": metric,
                    "geo_id": geo_id,
                    "turning_point_date":
                        turn_date,
                    "turning_point_type":
                        turn_type,
                    "structural_level_at_turn":
                        turn_level,
                    "prior_direction":
                        int(last_nonzero_direction),
                    "new_direction":
                        int(current_direction),
                    "persistence_months":
                        TURN_PERSISTENCE,
                    "confirmation_date":
                        confirmation_date,
                    "confirmation_level":
                        confirmation_level,
                    "post_turn_change_through_confirmation":
                        post_turn_change,
                }
            )

            # Once a persistent reversal is accepted, the new direction
            # becomes the governing direction.
            last_nonzero_direction = (
                current_direction
            )

    detail = pd.DataFrame(rows)

    if detail.empty:
        detail = pd.DataFrame(
            columns=[
                "policy",
                "metric",
                "geo_id",
                "turning_point_date",
                "turning_point_type",
                "structural_level_at_turn",
                "prior_direction",
                "new_direction",
                "persistence_months",
                "confirmation_date",
                "confirmation_level",
                "post_turn_change_through_confirmation",
            ]
        )

        summary = pd.MultiIndex.from_product(
            [policy_universe, metric_universe], names=["policy", "metric"]
        ).to_frame(index=False)
        for column in ("turning_points", "peak_count", "trough_count",
                       "geographies_with_turns", "latest_36m_turning_points"):
            summary[column] = 0
        summary["median_turn_spacing_months"] = np.nan

        return detail, summary

    detail["turning_point_date"] = pd.to_datetime(
        detail["turning_point_date"]
    )
    detail["confirmation_date"] = pd.to_datetime(
        detail["confirmation_date"]
    )

    spacing_rows = []

    for (policy, metric, geo_id), frame in detail.groupby(
        ["policy", "metric", "geo_id"],
        sort=True,
    ):
        dates = (
            frame["turning_point_date"]
            .sort_values()
            .reset_index(drop=True)
        )

        if len(dates) < 2:
            continue

        spacing = (
            dates.diff()
            .dropna()
            .dt.days
            / 30.4375
        )

        for value in spacing:
            spacing_rows.append(
                {
                    "policy": policy,
                    "metric": metric,
                    "geo_id": geo_id,
                    "spacing_months": value,
                }
            )

    spacing = pd.DataFrame(spacing_rows)

    max_dates = (
        features.groupby(
            ["policy", "metric"],
            as_index=False,
        )["date"]
        .max()
        .rename(
            columns={
                "date": "max_feature_date",
            }
        )
    )

    detail_with_max = detail.merge(
        max_dates,
        on=["policy", "metric"],
        how="left",
        validate="many_to_one",
    )

    detail_with_max[
        "latest_36m_flag"
    ] = (
        detail_with_max[
            "turning_point_date"
        ]
        >= (
            detail_with_max[
                "max_feature_date"
            ]
            - pd.DateOffset(months=35)
        )
    )

    summary_rows = []

    for (policy, metric), frame in detail_with_max.groupby(
        ["policy", "metric"],
        sort=True,
    ):
        spacing_sub = (
            spacing.loc[
                spacing["policy"].eq(policy)
                & spacing["metric"].eq(metric),
                "spacing_months",
            ]
            if not spacing.empty
            else pd.Series(dtype=float)
        )

        summary_rows.append(
            {
                "policy": policy,
                "metric": metric,
                "turning_points":
                    len(frame),
                "peak_count":
                    int(
                        frame[
                            "turning_point_type"
                        ].eq("peak").sum()
                    ),
                "trough_count":
                    int(
                        frame[
                            "turning_point_type"
                        ].eq("trough").sum()
                    ),
                "geographies_with_turns":
                    frame["geo_id"].nunique(),
                "median_turn_spacing_months":
                    (
                        spacing_sub.median()
                        if not spacing_sub.empty
                        else np.nan
                    ),
                "latest_36m_turning_points":
                    int(
                        frame[
                            "latest_36m_flag"
                        ].sum()
                    ),
            }
        )

    summary = pd.DataFrame(summary_rows)

    # Ensure the full policy × metric grid exists even if one pair
    # legitimately produces zero persistent turns.
    full_grid = pd.MultiIndex.from_product(
        [
            policy_universe,
            metric_universe,
        ],
        names=[
            "policy",
            "metric",
        ],
    ).to_frame(index=False)

    summary = full_grid.merge(
        summary,
        on=["policy", "metric"],
        how="left",
        validate="one_to_one",
    )

    for column in [
        "turning_points",
        "peak_count",
        "trough_count",
        "geographies_with_turns",
        "latest_36m_turning_points",
    ]:
        summary[column] = (
            summary[column]
            .fillna(0)
            .astype(int)
        )

    return (
        detail.sort_values(
            [
                "policy",
                "metric",
                "geo_id",
                "turning_point_date",
            ]
        ).reset_index(drop=True),
        summary.sort_values(
            ["policy", "metric"]
        ).reset_index(drop=True),
    )


def _divergence_tables(
    features: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Persist exact A/B level divergence chronology and summary."""

    wide = (
        features.pivot(
            index=[
                "metric",
                "geo_id",
                "date",
            ],
            columns="policy",
            values="structural_level",
        )
        .reset_index()
    )

    required = {
        POLICY_A,
        POLICY_B,
    }

    if not required.issubset(
        set(wide.columns)
    ):
        raise AssertionError(
            "A/B structural-level chronology is incomplete"
        )

    detail = wide.dropna(
        subset=[
            POLICY_A,
            POLICY_B,
        ]
    ).copy()

    detail[
        "level_difference_b_minus_a"
    ] = (
        detail[POLICY_B]
        - detail[POLICY_A]
    )

    detail[
        "absolute_level_difference"
    ] = (
        detail[
            "level_difference_b_minus_a"
        ].abs()
    )

    detail[
        "relative_level_difference_b_vs_a"
    ] = np.where(
        ~np.isclose(
            detail[POLICY_A],
            0.0,
        ),
        detail[
            "level_difference_b_minus_a"
        ]
        / detail[POLICY_A],
        np.nan,
    )

    summary_rows = []

    for metric, frame in detail.groupby(
        "metric",
        sort=True,
    ):
        largest_index = (
            frame[
                "absolute_level_difference"
            ].idxmax()
        )
        largest = frame.loc[largest_index]

        summary_rows.append(
            {
                "metric": metric,
                "observation_count":
                    len(frame),
                "median_absolute_difference":
                    frame[
                        "absolute_level_difference"
                    ].median(),
                "p90_absolute_difference":
                    frame[
                        "absolute_level_difference"
                    ].quantile(.90),
                "p99_absolute_difference":
                    frame[
                        "absolute_level_difference"
                    ].quantile(.99),
                "maximum_absolute_difference":
                    frame[
                        "absolute_level_difference"
                    ].max(),
                "largest_divergence_geo_id":
                    largest["geo_id"],
                "largest_divergence_date":
                    largest["date"],
                "policy_a_level_at_largest_divergence":
                    largest[POLICY_A],
                "policy_b_level_at_largest_divergence":
                    largest[POLICY_B],
                "signed_difference_at_largest_divergence":
                    largest[
                        "level_difference_b_minus_a"
                    ],
            }
        )

    summary = pd.DataFrame(summary_rows)

    return (
        detail.sort_values(
            ["metric", "geo_id", "date"]
        ).reset_index(drop=True),
        summary.sort_values(
            "metric"
        ).reset_index(drop=True),
    )


def build_affordability_derivation_evidence(source: pd.DataFrame) -> AffordabilityDerivationEvidence:
    source = _validate_source(source)
    raw_parts, audit_parts = [], []
    for policy in (POLICY_A, POLICY_B):
        derived, lineage = _derive(source, policy)
        raw_parts.append(derived)
        audit_parts.append(_input_audit(lineage, policy))
    raw = pd.concat(raw_parts, ignore_index=True)
    audit = pd.concat(audit_parts, ignore_index=True)
    expected_components = {key: set(DERIVED_METRIC_COMPONENTS[key]) for key in TARGET_METRICS}
    membership = audit.groupby(["policy", "metric", "geo_id", "date"]).input_name.agg(set)
    if any(inputs != expected_components[key[1]] for key, inputs in membership.items()):
        raise AssertionError("Input lineage audit is incomplete")
    features = _features(raw)
    formula = _formula_audit(raw, audit)
    stability = _stability(features)

    turning_points, turning_summary = (
        _turning_point_tables(features)
    )

    divergence_detail, divergence = (
        _divergence_tables(features)
    )
    decision = policy_registry().rename(columns={"policy": "Policy", "derivation_order": "Derivation order"})[["Policy", "Derivation order"]]
    required_decision_columns = [
        "Price-to-income median abs MoM", "Price-to-income P90", "Price-to-income P99",
        "Price-to-income max jump", "Price-to-income sign flips", "Price-to-income turning points",
        "Payment-burden median abs MoM", "Payment-burden P90", "Payment-burden P99",
        "Payment-burden max jump", "Payment-burden sign flips", "Payment-burden turning points",
        "Affordability dimension median abs MoM", "Affordability dimension P90",
        "Affordability dimension P99", "Affordability dimension turning points",
        "Latest-36m Affordability turns", "Largest level divergence between policies",
        "Median absolute level divergence", "Demand-axis changed months", "Changed county-month regimes",
    ]
    for column in required_decision_columns:
        decision[column] = np.nan
    for index, row in decision.iterrows():
        for metric, prefix in (("price_to_income", "Price-to-income"), ("payment_burden", "Payment-burden")):
            stats = stability[(stability.policy.eq(row.Policy)) & (stability.metric.eq(metric))].iloc[0]
            decision.loc[index, f"{prefix} median abs MoM"] = stats.median_abs_mom
            decision.loc[index, f"{prefix} P90"] = stats.p90_abs_mom
            decision.loc[index, f"{prefix} P99"] = stats.p99_abs_mom
            decision.loc[index, f"{prefix} max jump"] = stats.max_abs_jump
            decision.loc[index, f"{prefix} sign flips"] = stats.sign_flips

            turn_stats = turning_summary.loc[
                turning_summary["policy"].eq(row.Policy)
                & turning_summary["metric"].eq(metric)
            ]

            if len(turn_stats) != 1:
                raise AssertionError(
                    "Expected exactly one turning-point summary row; "
                    f"policy={row.Policy}, metric={metric}, "
                    f"rows={len(turn_stats)}"
                )

            decision.loc[
                index,
                f"{prefix} turning points",
            ] = turn_stats.iloc[0]["turning_points"]

        decision.loc[index, "Largest level divergence between policies"] = divergence.maximum_absolute_difference.max()
        decision.loc[index, "Median absolute level divergence"] = divergence.median_absolute_difference.median()
    decision["Decision"] = "pending"
    empty = pd.DataFrame()
    tables = {
        "affordability_derivation_policy_registry": policy_registry(),
        "affordability_derivation_input_audit": audit,
        "affordability_derivation_formula_audit": formula,
        "affordability_derivation_raw_chronology": raw,
        "affordability_derivation_feature_chronology": features,
        "affordability_derivation_normalized_feature_scores": empty,
        "affordability_derivation_metric_scores": empty,
        "affordability_derivation_metric_stability": stability,
        "affordability_derivation_metric_turning_points": turning_points,
        "affordability_derivation_metric_turning_point_summary": turning_summary,
        "affordability_derivation_level_divergence": divergence_detail,
        "affordability_derivation_level_divergence_summary": divergence,
        "affordability_derivation_dimension_chronology": empty,
        "affordability_derivation_dimension_stability": empty,
        "affordability_derivation_dimension_turning_point_summary": empty,
        "affordability_derivation_extreme_jumps": empty,
        "affordability_derivation_recent_chronology": features.groupby(["policy", "metric", "geo_id"]).tail(36),
        "affordability_derivation_demand_axis_context": empty,
        "affordability_derivation_regime_change_summary": empty,
        "affordability_derivation_parity_audit": divergence,
        "affordability_derivation_decision_matrix": decision,
        "affordability_derivation_human_decision_status": pd.DataFrame([{
            "recommendation_state": "none", "promotion_state": "none", "human_decision": "pending"
        }]),
        "affordability_derivation_runtime_summary": pd.DataFrame([{
            "policies": 2, "metrics": 2, "formula_reconstruction_passed": True,
            "authoritative_evidence_required": True
        }]),
    }
    return AffordabilityDerivationEvidence(tables)
