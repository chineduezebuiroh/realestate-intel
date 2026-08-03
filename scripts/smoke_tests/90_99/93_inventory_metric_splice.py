"""Smoke Test 93: final target-metric and aligned-metric causal splice."""
from __future__ import annotations

import runpy
from pathlib import Path

import pandas as pd

from regime.experiments.in_memory_challenger import _assemble_causal_splice
from regime.review.calibration.inventory_campaign import _parity_comparison


METRIC_KEYS = ["geo_id", "date", "canonical_metric_key"]


def _metrics() -> pd.DataFrame:
    rows = [
        ("fixture__county", pd.Timestamp("2024-01-31"), "active_inventory", 0.2),
        ("fixture__county", pd.Timestamp("2024-01-31"), "permit_activity", 0.4),
    ]
    return pd.DataFrame(rows, columns=[*METRIC_KEYS, "metric_score"])


def main() -> int:
    incumbent = _metrics()
    recomputed = incumbent.copy(deep=True)
    recomputed.loc[recomputed.canonical_metric_key.eq("active_inventory"), "metric_score"] = 0.8
    # Authoritative-style sentinel: a current-code sibling differs, but the
    # causal splice must discard it and preserve the persisted incumbent row.
    recomputed.loc[recomputed.canonical_metric_key.eq("permit_activity"), "metric_score"] = -0.9
    mixed = _assemble_causal_splice(
        candidate=recomputed, incumbent=incumbent,
        identity_column="canonical_metric_key", target_identity="active_inventory",
        keys=METRIC_KEYS, layer="metric universe",
    )
    assert mixed.query("canonical_metric_key == 'active_inventory'").iloc[0].metric_score == 0.8
    assert mixed.query("canonical_metric_key == 'permit_activity'").iloc[0].metric_score == 0.4

    early_target = incumbent.iloc[[0]].assign(date=pd.Timestamp("2023-12-31"))
    warm_incumbent = pd.concat([early_target, incumbent], ignore_index=True)
    warm_mixed = _assemble_causal_splice(
        candidate=recomputed, incumbent=warm_incumbent,
        identity_column="canonical_metric_key", target_identity="active_inventory",
        keys=METRIC_KEYS, layer="metric universe",
    )
    assert not warm_mixed.query(
        "canonical_metric_key == 'active_inventory'"
    ).date.eq(pd.Timestamp("2023-12-31")).any()

    aligned = incumbent.rename(columns={"date": "evaluation_date"}).assign(
        metric_date=lambda frame: frame.evaluation_date,
        metric_age_days=0,
    )[["geo_id", "evaluation_date", "metric_date", "canonical_metric_key", "metric_score", "metric_age_days"]]
    try:
        _assemble_causal_splice(
            candidate=aligned.drop(columns="metric_age_days"), incumbent=aligned,
            identity_column="canonical_metric_key", target_identity="active_inventory",
            keys=["geo_id", "evaluation_date", "canonical_metric_key"],
            layer="aligned-metric universe",
        )
    except ValueError as exc:
        assert "schema mismatch" in str(exc)
    else:
        raise AssertionError("Missing aligned lineage field did not fail closed")

    required = set(incumbent.columns)
    sibling = incumbent.query("canonical_metric_key != 'active_inventory'")
    for mutation in (
        sibling.iloc[0:0],
        pd.concat([sibling, sibling.assign(geo_id="extra__county")], ignore_index=True),
        sibling.assign(metric_score=0.401),
    ):
        result = _parity_comparison(
            candidate_id="sentinel", layer="metric", object_key="non_target",
            baseline=sibling, challenger=mutation, keys=METRIC_KEYS,
            required_columns=required,
        )
        assert not result["parity_pass"]

    try:
        _assemble_causal_splice(
            candidate=pd.concat([recomputed, recomputed.iloc[[0]]], ignore_index=True),
            incumbent=incumbent, identity_column="canonical_metric_key",
            target_identity="active_inventory", keys=METRIC_KEYS,
            layer="metric universe",
        )
    except ValueError as exc:
        assert "duplicate governed keys" in str(exc)
    else:
        raise AssertionError("Duplicate metric key did not fail closed")

    # Smoke 90 owns the production-scorer fixture, warmup, geography, aligned
    # lineage/schema, dimensions, axes, coordinates, regimes, and no-promotion
    # assertions. Reuse it rather than creating a divergent pipeline fixture.
    smoke = Path(__file__).with_name("90_inventory_challenger_completeness.py")
    namespace = runpy.run_path(str(smoke), run_name="inventory_metric_splice_contract")
    assert namespace["main"]() == 0
    print("SMOKE TEST 93 — INVENTORY METRIC SPLICE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
